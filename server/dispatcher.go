package server

import (
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/fengleng/flightmq/common"
	"github.com/fengleng/flightmq/config"
	"github.com/fengleng/flightmq/log"
	"github.com/pingcap/errors"
	"sync"
	"sync/atomic"
	"time"
)

type Dispatcher struct {
	//ctx        *Context
	db *bolt.DB
	wg common.WaitGroupWrapper
	//scanWg        common.WaitGroupWrapper
	scanQueueExpireMsgWg common.WaitGroupWrapper
	closed               bool
	poolSize             int
	snowflake            *common.Snowflake
	exitChan             chan struct{}

	// topic
	topics   map[string]*Topic
	topicMux sync.RWMutex

	channels map[string]*Channel

	// channel
	//channels          map[string]*Channel
	channelMux sync.RWMutex

	srv *Server

	addTopic chan *Topic

	logger log.Logger
}

func NewDispatcher(cfg *config.Config) *Dispatcher {
	sn, err := common.NewSnowflake(int64(cfg.NodeId))
	if err != nil {
		panic(err)
	}

	dbFile := fmt.Sprintf("%s/flightMq.db", cfg.DataSavePath)
	db, err := bolt.Open(dbFile, 0600, nil)
	if err != nil {
		panic(err)
	}

	dispatcher := &Dispatcher{
		db: db,
		//ctx:        ctx,
		snowflake: sn,
		topics:    make(map[string]*Topic),
		//channels:   make(map[string]*Channel),
		exitChan: make(chan struct{}),
	}
	dispatcher.logger = log.NewFileLogger(log.CfgOptionService("dispatcher"))

	//ctx.Dispatcher = dispatcher
	return dispatcher
}

func (d *Dispatcher) Run() {
	defer d.logger.Info("dispatcher exit")
	d.wg.Wrap(d.scanDelayMsg)
	d.wg.Wrap(d.scanQueueExpireMsg)

	select {
	case <-d.srv.exitChan:
		d.exit()
	}
}

func (d *Dispatcher) scanQueueExpireMsg() {
	d.logger.Info("begin scan queue expire msg")
	ticker := time.NewTicker(10 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			for _, t := range d.topics {
				d.scanQueueExpireMsgWg.Wrap(func() {
					err := t.retrievalQueueExpireMsg()
					if err != nil {
						t.logger.Error("%v", err)
					}
				})

			}
			d.scanQueueExpireMsgWg.Wait()
		case <-d.exitChan:
			return
		}
	}
}

func (d *Dispatcher) scanDelayMsg() {
	d.logger.Info("begin scan delay msg")
	ticker := time.NewTicker(10 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			err := d.db.Update(func(tx *bolt.Tx) error {
				err := tx.ForEach(func(name []byte, b *bolt.Bucket) error {
					if b.Stats().KeyN == 0 {
						return nil
					}
					now := time.Now().Unix()
					cursor := b.Cursor()

					for key, data := cursor.First(); key != nil; key, data = cursor.Next() {
						delayTime, _ := parseBucketKey(key)
						if now < int64(delayTime) {
							continue
						}
						var dg DelayMsg
						var t *Topic
						var err error
						if err := json.Unmarshal(data, &dg); err != nil {
							d.logger.Error("decode delay message failed, %s topic:%v", err, string(name))
							goto deleteBucketElem
						}
						if dg.Msg.Id == 0 {
							d.logger.Error("invalid delay message. topic:%v", string(name))
							goto deleteBucketElem
						}
						t, err = d.GetExistTopic(string(name))
						if err != nil {
							d.logger.Error("%v", err)
							break
						}
						for _, bindKey := range dg.BindKeys {
							queue := t.getQueueByBindKey(bindKey)
							if queue == nil {
								t.logger.Error(fmt.Sprintf("bindkey:%s is not associated with queue", bindKey))
								continue
							}
							if err := queue.write(Encode(dg.Msg)); err != nil {
								t.logger.Error("%v", err)
								continue
							}
							atomic.AddInt64(&t.pushNum, 1)
							//num++
						}
					deleteBucketElem:
						if err := b.Delete(key); err != nil {
							d.logger.Error("%v", err)
						}
					}

					return nil
				})
				if err != nil {
					d.logger.Error("%v", err)
					return err
				}
				return nil
			})
			if err != nil {
				d.logger.Error("%v", err)
			}
		case <-d.exitChan:
			return
		}
	}
}

// exit dispatcher
func (d *Dispatcher) exit() {
	d.closed = true
	_ = d.db.Close()
	close(d.exitChan)

	for _, t := range d.topics {
		t.exit()
	}

	d.wg.Wait()
}

// GetTopics get all topics
func (d *Dispatcher) GetTopics() []*Topic {
	d.topicMux.RLock()
	defer d.topicMux.RUnlock()

	var topics []*Topic
	for _, t := range d.topics {
		topics = append(topics, t)
	}
	return topics
}

// GetTopic get topic
// create topic if it is not exist
func (d *Dispatcher) GetTopic(name string) *Topic {
	d.topicMux.RLock()
	if t, ok := d.topics[name]; ok {
		d.topicMux.RUnlock()
		return t
	} else {
		d.topicMux.RUnlock()
	}

	d.topicMux.Lock()
	t := NewTopic(name, d.srv.cfg)
	t.dispatcher = d
	d.topics[name] = t
	d.topicMux.Unlock()
	return t
}

// GetExistTopic get topic
// returns error when it is not exist
func (d *Dispatcher) GetExistTopic(name string) (*Topic, error) {
	d.topicMux.RLock()
	defer d.topicMux.RUnlock()

	if t, ok := d.topics[name]; ok {
		return t, nil
	} else {
		return nil, errors.New("topic is not exist")
	}
}

// RemoveTopic remove topic by topic.name
func (d *Dispatcher) RemoveTopic(name string) {
	d.topicMux.Lock()
	if t, ok := d.topics[name]; ok {
		delete(d.topics, t.name)
	}

	d.topicMux.Unlock()
}

// GetChannel get channel
// create channel if is not exist
func (d *Dispatcher) GetChannel(key string) *Channel {
	d.channelMux.RLock()
	if c, ok := d.channels[key]; ok {
		d.channelMux.RUnlock()
		return c
	} else {
		d.channelMux.RUnlock()
	}

	d.channelMux.Lock()
	//c := NewChannel(key, d.ctx)
	c := NewChannel(key)
	d.channels[key] = c
	d.channelMux.Unlock()
	return c
}

// RemoveChannel remove channel by channel.key
func (d *Dispatcher) RemoveChannel(key string) {
	d.channelMux.Lock()
	if c, ok := d.channels[key]; ok {
		delete(d.channels, c.key)
	}

	d.channelMux.Unlock()
}

// 消息推送
// 每一条消息都需要dispatcher统一分配msg.Id
func (d *Dispatcher) push(name string, routeKey string, data []byte, delay int) (uint64, error) {
	msgId := d.snowflake.Generate()
	msg := &Msg{}
	msg.Id = msgId
	msg.Delay = uint32(delay)
	msg.Body = data

	topic := d.GetTopic(name)
	err := topic.push(msg, routeKey)

	return msgId, err
}

// consume message
func (d *Dispatcher) pop(name, bindKey string) (*Msg, error) {
	topic := d.GetTopic(name)
	return topic.pop(bindKey)
}

// consume dead message
func (d *Dispatcher) dead(name, bindKey string) (*Msg, error) {
	topic := d.GetTopic(name)
	return topic.dead(bindKey)
}

// ack message
func (d *Dispatcher) ack(name string, msgId uint64, bindKey string) error {
	topic := d.GetTopic(name)
	return topic.ack(msgId, bindKey)
}

// Set config
func (d *Dispatcher) Set(name string, configure *topicConfigure) error {
	topic := d.GetTopic(name)
	return topic.set(configure)
}

// declare queue
func (d *Dispatcher) declareQueue(queueName, bindKey string) error {
	topic := d.GetTopic(queueName)
	return topic.declareQueue(bindKey)
}

// subscribe channel
func (d *Dispatcher) subscribe(channelName string, conn *TcpConn) error {
	channel := d.GetChannel(channelName)
	return channel.addConn(conn)
}

func (d *Dispatcher) publish(channelName string, msg []byte) error {
	channel := d.GetChannel(channelName)
	return channel.publish(msg)
}

func (d *Dispatcher) pushDelayMsg(topicName string, dg *DelayMsg) error {
	err := d.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(topicName))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}

		//bucket := tx.Bucket([]byte(topicName))
		key := creatBucketKey(dg.Msg.Id, dg.Msg.Expire)
		d.logger.Info(fmt.Sprintf("%v-%v-%v write in bucket", dg.Msg.Id, string(dg.Msg.Body), key))

		value, err := json.Marshal(dg)
		if err != nil {
			return err
		}
		if err := bucket.Put(key, value); err != nil {
			return err
		}

		return nil
	})
	return err
}
