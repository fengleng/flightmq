package server

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/fengleng/flightmq/common"
	"github.com/fengleng/flightmq/config"
	"github.com/fengleng/flightmq/log"
	"github.com/pingcap/errors"
	"io/ioutil"
	"os"
	"regexp"
	"sync"
	"sync/atomic"
	"time"
)

type Topic struct {
	cfg       *config.Config
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
	closed       bool
	wg           common.WaitGroupWrapper
	dispatcher   *Dispatcher
	exitChan     chan struct{}
	queues       map[string]*queue
	deadQueues   map[string]*queue
	waitAckMux   sync.Mutex
	queueMux     sync.Mutex
	deadQueueMux sync.Mutex
	sync.Mutex

	db     *bolt.DB
	logger log.Logger
}

type TopicMeta struct {
	Mode       int         `json:"mode"`
	PopNum     int64       `json:"pop_num"`
	PushNum    int64       `json:"push_num"`
	DeadNum    int64       `json:"dead_num"`
	IsAutoAck  bool        `json:"is_auto_ack"`
	Queues     []QueueMeta `json:"queues"`
	DeadQueues []QueueMeta `json:"dead_queues"`
}

type QueueMeta struct {
	Num         int64  `json:"queue_num"`
	Name        string `json:"queue_name"`
	BindKey     string `json:"bind_key"`
	WriteOffset int64  `json:"write_offset"`
	ReadOffset  int64  `json:"read_offset"`
	ScanOffset  int64  `json:"scan_offset"`
}

type topicConfigure struct {
	isAutoAck int
	msgTTR    int
	msgRetry  int
	mode      int
}

//// 检索延迟消息
//func (t *Topic) retrievalBucketExpireMsg() error {
//	if t.closed {
//		err := errors.New(fmt.Sprintf("topic.%s has exit.", t.name))
//		t.logger.Error("%v", err)
//		return err
//	}
//
//	var num int
//	var err error
//	err = t.dispatcher.db.Update(func(tx *bolt.Tx) error {
//		bucket := tx.Bucket([]byte(t.name))
//		if bucket.Stats().KeyN == 0 {
//			return nil
//		}
//
//		now := time.Now().Unix()
//		c := bucket.Cursor()
//		for key, data := c.First(); key != nil; key, data = c.Next() {
//			delayTime, _ := parseBucketKey(key)
//			if now < int64(delayTime) {
//				break
//			}
//
//			var dg DelayMsg
//			if err := json.Unmarshal(data, &dg); err != nil {
//				t.LogError(fmt.Errorf("decode delay message failed, %s", err))
//				goto deleteBucketElem
//			}
//			if dg.Msg.Id == 0 {
//				t.LogError(fmt.Errorf("invalid delay message."))
//				goto deleteBucketElem
//			}
//
//			for _, bindKey := range dg.BindKeys {
//				queue := t.getQueueByBindKey(bindKey)
//				if queue == nil {
//					t.LogError(fmt.Sprintf("bindkey:%s is not associated with queue", bindKey))
//					continue
//				}
//				if err := queue.write(Encode(dg.Msg)); err != nil {
//					t.LogError(err)
//					continue
//				}
//				atomic.AddInt64(&t.pushNum, 1)
//				num++
//			}
//
//		deleteBucketElem:
//			if err := bucket.Delete(key); err != nil {
//				t.LogError(err)
//			}
//		}
//
//		return nil
//	})
//
//	if err != nil {
//		return err
//	}
//	if num == 0 {
//		return ErrMessageNotExist
//	}
//
//	return nil
//}
//
//// 检测超时消息
//func (t *Topic) retrievalQueueExipreMsg() error {
//	if t.closed {
//		err := fmt.Errorf("topic.%s has exit.", t.name)
//		t.LogWarn(err)
//		return err
//	}
//
//	num := 0
//	for _, queue := range t.queues {
//		for {
//			data, err := queue.scan()
//			if err != nil {
//				if err != ErrMessageNotExist && err != ErrMessageNotExpire {
//					t.LogError(err)
//				}
//				break
//			}
//
//			msg := Decode(data)
//			if msg.Id == 0 {
//				msg = nil
//				break
//			}
//
//			if err := queue.removeWait(msg.Id); err != nil {
//				t.LogError(err)
//				break
//			}
//
//			msg.Retry = msg.Retry + 1 // incr retry number
//			if msg.Retry > uint16(t.msgRetry) {
//				t.LogDebug(fmt.Sprintf("msg.Id %v has been added to dead queue.", msg.Id))
//				if err := t.pushMsgToDeadQueue(msg, queue.bindKey); err != nil {
//					t.LogError(err)
//					break
//				} else {
//					continue
//				}
//			}
//
//			// message is expired, and will be consumed again
//			if err := queue.write(Encode(msg)); err != nil {
//				t.LogError(err)
//				break
//			} else {
//				t.LogDebug(fmt.Sprintf("msg.Id %v has expired and will be consumed again.", msg.Id))
//				atomic.AddInt64(&t.pushNum, 1)
//				num++
//			}
//		}
//	}
//
//	if num > 0 {
//		return nil
//	} else {
//		return ErrMessageNotExist
//	}
//}

func NewTopic(name string, cfg *config.Config) *Topic {
	t := &Topic{
		//ctx:        ctx,
		cfg:       cfg,
		name:      name,
		msgTTR:    cfg.MsgTTR,
		msgRetry:  cfg.MsgMaxRetry,
		mode:      ROUTE_KEY_MATCH_FUZZY,
		isAutoAck: true,
		exitChan:  make(chan struct{}),
		//dispatcher: ctx.Dispatcher,
		startTime:  time.Now(),
		queues:     make(map[string]*queue),
		deadQueues: make(map[string]*queue),
	}
	t.logger = log.NewFileLogger(log.CfgOptionService("topic"))

	t.init()
	return t
}

// 初始化
func (t *Topic) init() {

	t.logger.Info(fmt.Sprintf("loading topic.%s metadata.", t.name))

	fd, err := os.OpenFile(fmt.Sprintf("%s/%s.meta", t.cfg.DataSavePath, t.name), os.O_RDONLY, 0600)
	if err != nil {
		if !os.IsNotExist(err) {
			t.logger.Error(fmt.Sprintf("load %s.meta failed, %v", t.name, err))
		}
		return
	}
	defer func() {
		_ = fd.Close()
	}()

	data, err := ioutil.ReadAll(fd)
	if err != nil {
		t.logger.Error(fmt.Sprintf("load %s.meta failed, %v", t.name, err))
		return
	}
	meta := &TopicMeta{}
	if err := json.Unmarshal(data, meta); err != nil {
		t.logger.Error(fmt.Sprintf("load %s.meta failed, %v", t.name, err))
		return
	}

	// retore topic meta data
	t.mode = meta.Mode
	t.popNum = meta.PopNum
	t.pushNum = meta.PushNum
	t.deadNum = meta.DeadNum
	t.isAutoAck = meta.IsAutoAck

	t.queueMux.Lock()
	defer t.queueMux.Unlock()

	// restore queue meta data
	for _, q := range meta.Queues {
		// skip empty queue
		//if q.Num == 0 {
		//	continue
		//}

		queue := NewQueue(q.Name, q.BindKey, t)
		queue.woffset = q.WriteOffset
		queue.roffset = q.ReadOffset
		queue.soffset = q.ScanOffset
		queue.num = q.Num
		queue.name = q.Name
		t.queues[q.BindKey] = queue

		t.logger.Info(fmt.Sprintf("restore queue %s", queue.name))
	}

	// restore dead queue meta data
	for _, q := range meta.DeadQueues {
		// skip empty queue
		//if q.Num == 0 {
		//	continue
		//}

		queue := NewQueue(q.Name, q.BindKey, t)
		queue.woffset = q.WriteOffset
		queue.roffset = q.ReadOffset
		queue.soffset = q.ScanOffset
		queue.num = q.Num
		queue.name = q.Name
		t.deadQueues[q.BindKey] = queue

		t.logger.Info(fmt.Sprintf("restore queue %s", queue.name))
	}
}

func (t *Topic) Serialize() error {
	t.Lock()
	defer t.Unlock()
	t.logger.Info(fmt.Sprintf("writing topic.%s metadata.", t.name))
	fd, err := os.OpenFile(fmt.Sprintf("%s/%s.meta", t.cfg.DataSavePath, t.name), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		t.logger.Error(fmt.Sprintf("write %s.meta failed, %v", t.name, err))
		return err
	}
	defer func() {
		_ = fd.Close()
	}()

	var queues []QueueMeta
	for k, q := range t.queues {
		queues = append(queues, QueueMeta{
			Num:         q.num,
			Name:        q.name,
			BindKey:     k,
			WriteOffset: q.woffset,
			ReadOffset:  q.roffset,
			ScanOffset:  q.soffset,
		})
	}

	var deadQueues []QueueMeta
	for k, q := range t.deadQueues {
		deadQueues = append(deadQueues, QueueMeta{
			Num:         q.num,
			Name:        q.name,
			BindKey:     k,
			WriteOffset: q.woffset,
			ReadOffset:  q.roffset,
			ScanOffset:  q.soffset,
		})
	}

	meta := TopicMeta{
		PopNum:     t.popNum,
		PushNum:    t.pushNum,
		DeadNum:    t.deadNum,
		Mode:       t.mode,
		IsAutoAck:  t.isAutoAck,
		Queues:     queues,
		DeadQueues: deadQueues,
	}
	data, err := json.Marshal(meta)
	if err != nil {
		t.logger.Error(fmt.Sprintf("write %s.meta failed, %v", t.name, err))
		return err
	}

	_, err = fd.Write(data)
	if err != nil {
		t.logger.Error(fmt.Sprintf("write %s.meta failed, %v", t.name, err))
		return err
	}
	return nil
}

// 退出topic
func (t *Topic) exit() {
	defer t.logger.Info(fmt.Sprintf("topic.%s has exit.", t.name))

	t.closed = true
	close(t.exitChan)
	t.wg.Wait()

	err := t.Serialize()
	if err != nil {
		t.logger.Error("%v", err)
	}
	t.queueMux.Lock()
	defer t.queueMux.Unlock()

	//var queues []QueueMeta
	for _, q := range t.queues {
		q.exit()
	}

	for _, q := range t.deadQueues {
		q.exit()
	}
}

// 消息消费
func (t *Topic) pop(bindKey string) (*Msg, error) {
	queue := t.getQueueByBindKey(bindKey)
	if queue == nil {
		return nil, fmt.Errorf("bindKey:%s can't match queue", bindKey)
	}

	data, err := queue.read(t.isAutoAck)
	if err != nil {
		return nil, err
	}

	msg := Decode(data.data)
	if msg.Id == 0 {
		msg = nil
		return nil, errors.New("message decode failed.")
	}

	atomic.AddInt64(&t.popNum, 1)
	return msg, nil
}

// 死信队列消费
func (t *Topic) dead(bindKey string) (*Msg, error) {
	queue := t.getDeadQueueByBindKey(bindKey, false)
	if queue == nil {
		return nil, fmt.Errorf("bindkey:%s is not associated with queue", bindKey)
	}

	data, err := queue.read(t.isAutoAck)
	if err != nil {
		return nil, err
	}

	msg := Decode(data.data)
	if msg.Id == 0 {
		msg = nil
		return nil, errors.New("message decode failed.")
	}

	atomic.AddInt64(&t.deadNum, -1)
	return msg, nil
}

// 消息确认
func (t *Topic) ack(msgId uint64, bindKey string) error {
	queue := t.getQueueByBindKey(bindKey)
	if queue == nil {
		return fmt.Errorf("bindkey:%s is not associated with queue", bindKey)
	}

	return queue.ack(msgId)
}

// 设置topic信息
func (t *Topic) set(configure *topicConfigure) error {
	t.Lock()
	defer t.Unlock()

	// auto-ack
	if configure.isAutoAck == 1 {
		t.isAutoAck = true
	} else {
		t.isAutoAck = false
	}

	// route mode
	if configure.mode == ROUTE_KEY_MATCH_FULL {
		t.mode = ROUTE_KEY_MATCH_FULL
	} else if configure.mode == ROUTE_KEY_MATCH_FUZZY {
		t.mode = ROUTE_KEY_MATCH_FUZZY
	}

	// ttr
	if configure.msgTTR > 0 && configure.msgTTR < t.msgTTR {
		t.msgTTR = configure.msgTTR
	}

	// retry
	if configure.msgRetry > 0 && configure.msgRetry < t.msgRetry {
		t.msgRetry = configure.msgRetry
	}
	err := t.Serialize()
	if err != nil {
		t.logger.Error("%v", err)
	}
	return err
}

// 声明队列，绑定key必须是唯一值
// 队列名称为<topic_name>_<bind_key>
func (t *Topic) declareQueue(bindKey string) error {
	queue := t.getQueueByBindKey(bindKey)
	if queue != nil {
		return errors.Errorf("bindKey %s has exist.", bindKey)
	}

	t.queueMux.Lock()
	defer t.queueMux.Unlock()

	queueName := fmt.Sprintf("%s_%s", t.name, bindKey)
	t.queues[bindKey] = NewQueue(queueName, bindKey, t)
	return t.Serialize()
}

// 根据路由键获取队列，支持全匹配和模糊匹配两种方式
func (t *Topic) getQueuesByRouteKey(routeKey string) []*queue {
	t.queueMux.Lock()
	defer t.queueMux.Unlock()

	// use default key when routeKey is empty
	if len(routeKey) == 0 {
		if q, ok := t.queues[DEFAULT_KEY]; ok {
			return []*queue{q}
		} else {
			queueName := t.generateQueueName(DEFAULT_KEY)
			t.queues[DEFAULT_KEY] = NewQueue(queueName, DEFAULT_KEY, t)
			err := t.Serialize()
			if err != nil {
				t.logger.Error("%v", err)
			}
			return []*queue{t.queues[DEFAULT_KEY]}
		}
	}

	var queues []*queue
	for k, v := range t.queues {
		if t.mode == ROUTE_KEY_MATCH_FULL {
			if k == routeKey {
				queues = append(queues, v)
			}
		} else {
			if ok, _ := regexp.MatchString(routeKey, k); ok {
				queues = append(queues, v)
			}
		}
	}

	return queues
}

// 根据绑定键获取队列
func (t *Topic) getQueueByBindKey(bindKey string) *queue {
	t.queueMux.Lock()
	defer t.queueMux.Unlock()

	if len(bindKey) == 0 {
		bindKey = DEFAULT_KEY
	}

	if q, ok := t.queues[bindKey]; ok {
		return q
	} else {
		return nil
	}
}

// 根据绑定键获取死信队列
func (t *Topic) getDeadQueueByBindKey(bindKey string, createNotExist bool) *queue {
	t.queueMux.Lock()
	defer t.queueMux.Unlock()

	if len(bindKey) == 0 {
		bindKey = DEFAULT_KEY
	}
	if q, ok := t.deadQueues[bindKey]; ok {
		return q
	}

	if createNotExist {
		queueName := t.generateQueueName(fmt.Sprintf("%s_%s", bindKey, DEAD_QUEUE_FLAG))
		t.deadQueueMux.Lock()
		defer t.deadQueueMux.Unlock()
		t.deadQueues[bindKey] = NewQueue(queueName, bindKey, t)
		err := t.Serialize()
		if err != nil {
			t.logger.Error("%v", err)
		}
		return t.deadQueues[bindKey]
	}

	return nil
}

func (t *Topic) generateQueueName(bindKey string) string {
	return fmt.Sprintf("%s_%s", t.name, bindKey)
}

// 消息推送
func (t *Topic) push(msg *Msg, routeKey string) error {
	queues := t.getQueuesByRouteKey(routeKey)
	if len(queues) == 0 {
		return fmt.Errorf("routeKey:%s is not match with queue, the mode is %d", routeKey, t.mode)
	}

	if msg.Delay > 0 {
		bindKeys := make([]string, len(queues))
		for i, q := range queues {
			bindKeys[i] = q.bindKey
		}

		msg.Expire = uint64(msg.Delay) + uint64(time.Now().Unix())
		return t.pushDelayMsg(&DelayMsg{msg, bindKeys})
	}

	for _, q := range queues {
		if err := q.write(Encode(msg)); err != nil {
			return err
		}
		atomic.AddInt64(&t.pushNum, 1)
	}

	return nil
}

// 延迟消息保存到bucket
func (t *Topic) pushDelayMsg(dg *DelayMsg) error {
	return t.dispatcher.pushDelayMsg(t.name, dg)
}

// bucket.key : delay + msgId
func creatBucketKey(msgId uint64, expire uint64) []byte {
	var buf = make([]byte, 16)
	binary.BigEndian.PutUint64(buf[:8], expire)
	binary.BigEndian.PutUint64(buf[8:], msgId)
	return buf
}

// 解析bucket.key
func parseBucketKey(key []byte) (uint64, uint64) {
	return binary.BigEndian.Uint64(key[:8]), binary.BigEndian.Uint64(key[8:])
}

// 检测超时消息
func (t *Topic) retrievalQueueExpireMsg() error {
	if t.closed {
		err := errors.Errorf("topic.%s has exit.", t.name)
		t.logger.Error("%v", err)
		return err
	}

	num := 0
	for _, queue := range t.queues {
		for {
			data, err := queue.scan()
			if err != nil {
				if err != ErrMessageNotExist && err != ErrMessageNotExpire {
					t.logger.Error("%v", err)
				}
				break
			}

			msg := Decode(data)
			if msg.Id == 0 {
				msg = nil
				break
			}

			if err := queue.removeWait(msg.Id); err != nil {
				t.logger.Error("%v", err)
				break
			}

			msg.Retry = msg.Retry + 1 // incr retry number
			if msg.Retry > uint16(t.msgRetry) {
				t.logger.Debug(fmt.Sprintf("msg.Id %v has been added to dead queue.", msg.Id))
				if err := t.pushMsgToDeadQueue(msg, queue.bindKey); err != nil {
					t.logger.Error("%v", err)
					break
				} else {
					continue
				}
			}

			// message is expired, and will be consumed again
			if err := queue.write(Encode(msg)); err != nil {
				t.logger.Error("%v", err)
				break
			} else {
				t.logger.Debug(fmt.Sprintf("msg.Id %v has expired and will be consumed again.", msg.Id))
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

// 添加消息到死信队列
func (t *Topic) pushMsgToDeadQueue(msg *Msg, bindKey string) error {
	queue := t.getDeadQueueByBindKey(bindKey, true)
	if err := queue.write(Encode(msg)); err != nil {
		return err
	}

	atomic.AddInt64(&t.deadNum, 1)
	return nil
}
