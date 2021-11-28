package server

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/fengleng/flightmq/common"
	"github.com/fengleng/flightmq/log"
	"github.com/pingcap/errors"
	"sync"
	"sync/atomic"
	"time"
)

type Topic struct {
	name      string
	mode      int
	msgTTR    int
	msgRetry  int
	isAutoAck bool
	pushNum   int64
	popNum    int64
	deadNum   int64
	startTime time.Time
	//ctx        *Context
	closed     bool
	wg         common.WaitGroupWrapper
	dispatcher *Dispatcher
	exitChan   chan struct{}
	queues     map[string]*queue
	deadQueues map[string]*queue
	waitAckMux sync.Mutex
	queueMux   sync.Mutex
	sync.Mutex

	logger log.Logger
}

// 检索延迟消息
func (t *Topic) retrievalBucketExpireMsg() error {
	if t.closed {
		err := errors.New(fmt.Sprintf("topic.%s has exit.", t.name))
		t.logger.Error("%v", err)
		return err
	}

	var num int
	var err error
	err = t.dispatcher.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(t.name))
		if bucket.Stats().KeyN == 0 {
			return nil
		}

		now := time.Now().Unix()
		c := bucket.Cursor()
		for key, data := c.First(); key != nil; key, data = c.Next() {
			delayTime, _ := parseBucketKey(key)
			if now < int64(delayTime) {
				break
			}

			var dg DelayMsg
			if err := json.Unmarshal(data, &dg); err != nil {
				t.LogError(fmt.Errorf("decode delay message failed, %s", err))
				goto deleteBucketElem
			}
			if dg.Msg.Id == 0 {
				t.LogError(fmt.Errorf("invalid delay message."))
				goto deleteBucketElem
			}

			for _, bindKey := range dg.BindKeys {
				queue := t.getQueueByBindKey(bindKey)
				if queue == nil {
					t.LogError(fmt.Sprintf("bindkey:%s is not associated with queue", bindKey))
					continue
				}
				if err := queue.write(Encode(dg.Msg)); err != nil {
					t.LogError(err)
					continue
				}
				atomic.AddInt64(&t.pushNum, 1)
				num++
			}

		deleteBucketElem:
			if err := bucket.Delete(key); err != nil {
				t.LogError(err)
			}
		}

		return nil
	})

	if err != nil {
		return err
	}
	if num == 0 {
		return ErrMessageNotExist
	}

	return nil
}

// 检测超时消息
func (t *Topic) retrievalQueueExipreMsg() error {
	if t.closed {
		err := fmt.Errorf("topic.%s has exit.", t.name)
		t.LogWarn(err)
		return err
	}

	num := 0
	for _, queue := range t.queues {
		for {
			data, err := queue.scan()
			if err != nil {
				if err != ErrMessageNotExist && err != ErrMessageNotExpire {
					t.LogError(err)
				}
				break
			}

			msg := Decode(data)
			if msg.Id == 0 {
				msg = nil
				break
			}

			if err := queue.removeWait(msg.Id); err != nil {
				t.LogError(err)
				break
			}

			msg.Retry = msg.Retry + 1 // incr retry number
			if msg.Retry > uint16(t.msgRetry) {
				t.LogDebug(fmt.Sprintf("msg.Id %v has been added to dead queue.", msg.Id))
				if err := t.pushMsgToDeadQueue(msg, queue.bindKey); err != nil {
					t.LogError(err)
					break
				} else {
					continue
				}
			}

			// message is expired, and will be consumed again
			if err := queue.write(Encode(msg)); err != nil {
				t.LogError(err)
				break
			} else {
				t.LogDebug(fmt.Sprintf("msg.Id %v has expired and will be consumed again.", msg.Id))
				atomic.AddInt64(&t.pushNum, 1)
				num++
			}
		}
	}

	if num > 0 {
		return nil
	} else {
		return ErrMessageNotExist
	}
}

// 解析bucket.key
func parseBucketKey(key []byte) (uint64, uint64) {
	return binary.BigEndian.Uint64(key[:8]), binary.BigEndian.Uint64(key[8:])
}
