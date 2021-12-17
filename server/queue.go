package server

import (
	"encoding/binary"
	"fmt"
	"github.com/fengleng/flightmq/common"
	"github.com/fengleng/flightmq/config"
	"github.com/fengleng/flightmq/log"
	"github.com/fengleng/flightmq/mq_errors"
	"github.com/pingcap/errors"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type queue struct {
	woffset  int64  // 写偏移量，用于记录当前队列已生成消息到哪个位置
	roffset  int64  // 读偏移量，用于记录当前队列被消费到哪个位置
	soffset  int64  // 扫描偏移量，用于处理过期消息
	filesize int64  // 当前队列文件大小
	num      int64  // 队列中消息数量，包括未消费或待确认两种状态的消息
	name     string // 队列名称，由topic.name和queue.bindKey两部分组成
	data     []byte // 内存映射文件
	topic    *Topic // 所属topic
	file     *os.File
	//ctx      *Context
	bindKey  string           //绑定键，topic.name_queue.bingKey是队列的唯一标识
	waitAck  map[uint64]int64 // 等待确认消息，消息ID和消息位置的映射关系
	readChan chan *readQueueData
	closed   bool
	wg       common.WaitGroupWrapper

	exitChan          chan struct{}
	notifyReadMsgChan chan bool
	sync.RWMutex

	cfg    *config.Config
	logger log.Logger
}

type readQueueData struct {
	data []byte
	pos  int64
}

func NewQueue(name, bindKey string, topic *Topic) *queue {
	queue := &queue{
		name: name,
		//ctx:      ctx,
		topic:    topic,
		bindKey:  bindKey,
		waitAck:  make(map[uint64]int64),
		readChan: make(chan *readQueueData),
		exitChan: make(chan struct{}),

		notifyReadMsgChan: make(chan bool),
	}

	path := fmt.Sprintf("%s/%s.queue", topic.cfg.DataSavePath, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		topic.logger.Error("open %s.queue failed, %v\n", queue.name, err)
	} else {
		queue.file = f
	}

	stat, err := f.Stat()
	if err != nil {
		topic.logger.Error("get %s.queue stat failed, %v\n", queue.name, err)
	}

	initMMapSize := int(stat.Size())

	// 初始文件为一个页大小
	if initMMapSize == 0 {
		if _, err := f.WriteAt([]byte{'0'}, int64(os.Getpagesize())-1); err != nil {
			topic.logger.Error("extend %v.queue failed, %v\n", queue.name, err)
		}

		if runtime.GOOS != "windows" {
			if err := syscall.Fdatasync(int(f.Fd())); err != nil {
				topic.logger.Error("sync %v.queue failed, %v\n", queue.name, err)
			}
		} else {
			if err := f.Sync(); err != nil {
				topic.logger.Error("sync %v.queue failed, %v\n", queue.name, err)
			}
		}

		initMMapSize = os.Getpagesize()
	}

	if err = queue.mmap(initMMapSize); err != nil {
		topic.logger.Error("%v", err)
	}
	//if data, err := mMap(f.Fd(), initMMapSize); err != nil {
	//	return queue
	//} else {
	//	queue.data = data
	//}

	//if err := queue.mmap(initMmapSize); err != nil {
	//	log.Fatalln(err)
	//}

	//queue.wg.Wrap(queue.loopRead)
	return queue
}

func (q *queue) loopRead() {
	for {
		queueData, err := q.read(q.topic.isAutoAck)
		if err != nil {
			q.logger.Error("%v", err)
		}

		select {
		case <-q.exitChan:
			return
		case q.readChan <- queueData:
		}
	}
}

func (q *queue) exit() {
	q.closed = true
	close(q.exitChan)
	close(q.notifyReadMsgChan)
	q.wg.Wait()
}

// 队列扫描未确认消息
func (q *queue) scan() ([]byte, error) {
	if q.closed {
		return nil, mq_errors.ErrQueueClosed
	}

	q.Lock()
	// q.LogDebug(fmt.Sprintf("scan.offset:%v read.offset:%v write.offset:%v", q.soffset, q.roffset, q.woffset))

	if q.soffset > REWRITE_SIZE {
		if err := q.rewrite(); err != nil {
			q.logger.Error("%v", err)
		}
	}
	if q.soffset == q.roffset {
		q.Unlock()
		return nil, ErrMessageNotExist
	}

	// 消息结构 flag(1-byte) + status(2-bytes) + msg_len(4-bytes) + msg(n-bytes)
	if flag := q.data[q.soffset]; flag != 'v' {
		q.Unlock()
		return nil, errors.New("unknown msg flag")
	}

	status := binary.BigEndian.Uint16(q.data[q.soffset+1 : q.soffset+3])
	msgLen := int64(binary.BigEndian.Uint32(q.data[q.soffset+3 : q.soffset+7]))

	if status == MSG_STATUS_READ {
		q.Unlock()
		return nil, ErrMessageNotExpire
	}

	// scan next message when the current message is finish
	if status == MSG_STATUS_FIN {
		q.soffset += MSG_FIX_LENGTH + msgLen
		atomic.AddInt64(&q.num, -1)
		q.Unlock()
		return q.scan()
	}

	expireTime := binary.BigEndian.Uint64(q.data[q.soffset+7 : q.soffset+15])
	q.logger.Info(fmt.Sprintf("msg.expire:%v now:%v", expireTime, time.Now().Unix()))

	// has not expire message
	if expireTime > uint64(time.Now().Unix()) {
		q.Unlock()
		return nil, ErrMessageNotExpire
	}

	// message will be consumed if it is expired
	binary.BigEndian.PutUint16(q.data[q.soffset+1:q.soffset+3], uint16(MSG_STATUS_EXPIRE))
	msg := make([]byte, msgLen)
	copy(msg, q.data[q.soffset+7:q.soffset+7+msgLen])
	q.soffset += MSG_FIX_LENGTH + msgLen
	atomic.AddInt64(&q.num, -1)

	q.Unlock()
	return msg, nil
}

// 重写文件大小
func (q *queue) rewrite() error {
	q.logger.Info(fmt.Sprintf("begin rewrite %v.queue, filesize:%v.", q.name, q.filesize))
	defer func() {
		q.logger.Info(fmt.Sprintf("after rewrite %v.queue, filesize:%v.", q.name, q.filesize))
	}()

	tempPath := fmt.Sprintf("%s/%s.temp.queue", q.cfg.DataSavePath, q.name)
	f, err := os.OpenFile(tempPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}

	pageSize := os.Getpagesize()
	size := q.filesize - q.soffset
	q.logger.Info(fmt.Sprintf("fileszie:%v,queue.soffset:%v,size:%v", q.filesize, q.soffset, size))

	// 确保mmap大小是页面大小的倍数
	sz := int(size)
	if (sz % pageSize) != 0 {
		remainSize := int64(sz - sz/pageSize*pageSize)
		if (q.filesize-q.woffset)-remainSize-int64(pageSize) > 0 {
			// 文件剩余空间满足页的大小的倍数,继续缩小文件大小
			size = size - remainSize
		} else {
			// 不满足,需要增加文件大小
			size = int64(sz/pageSize+1) * int64(pageSize)
		}
	}

	// 扩展文件
	_, err = f.WriteAt([]byte{'0'}, size-1)
	if err != nil {
		return err
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}

	// 将旧文件剩余数据迁移到新文件上
	copy(data, q.data[q.soffset:])

	q.file.Close()
	if err := q.unmap(); err != nil {
		return err
	}

	q.logger.Info(fmt.Sprintf("before rewrite, scan-offset:%v, read-offset:%v, write-offset:%v", q.soffset, q.roffset, q.woffset))
	q.data = data
	q.file = f
	q.roffset -= q.soffset
	q.woffset -= q.soffset
	q.filesize = size
	q.logger.Info(fmt.Sprintf("after rewrite, scan-offset:%v, read-offset:%v, write-offset:%v", q.soffset, q.roffset, q.woffset))
	// data = nil

	for k, v := range q.waitAck {
		q.waitAck[k] = v - q.soffset
	}

	q.soffset = 0
	path := fmt.Sprintf("%s/%s.queue", q.cfg.DataSavePath, q.name)
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}

	return nil
}

// 消息设置为已确认消费
func (q *queue) ack(msgId uint64) error {
	if q.closed {
		return mq_errors.ErrQueueClosed
	}

	q.Lock()
	defer q.Unlock()

	offset, ok := q.waitAck[msgId]
	if !ok {
		return errors.Errorf("msgId:%v is not exist.", msgId)
	}

	if offset > int64(len(q.data))-1 {
		return errors.Errorf("ack.offset greater than queue.length.")
	}
	if q.data[offset] != 'v' {
		return errors.Errorf("ack.offset error.")
	}

	binary.BigEndian.PutUint16(q.data[offset+1:offset+3], MSG_STATUS_FIN)
	delete(q.waitAck, msgId)
	return nil
}

// 移除消息等待状态
func (q *queue) removeWait(msgId uint64) error {
	q.Lock()
	defer q.Unlock()

	_, ok := q.waitAck[msgId]
	if !ok {
		return errors.Errorf("msgId:%v is not exist.", msgId)
	}

	delete(q.waitAck, msgId)
	return nil
}

// 映射文件
func (q *queue) mmap(size int) error {
	stat, err := q.file.Stat()
	if err != nil {
		return errors.Errorf("mmap %v.queue failed, %v.\n", q.name, err)
	}
	if stat.Size() == 0 {
		return errors.Errorf("mmap %v.queue failed, file is empty.\n", q.name)
	}

	// 解除上一次映射,如果有的话
	if len(q.data) > 0 {
		if err := q.unmap(); nil != err {
			return err
		}
	}

	if data, err := mMap(q.file.Fd(), size); err != nil {
		return err
	} else {
		q.data = data
		q.filesize = stat.Size()
	}
	return nil
}

// 解除映射
func (q *queue) unmap() error {
	return munMap(q.data)
}

// 队列读取消息
func (q *queue) read(isAutoAck bool) (*readQueueData, error) {
	if q.closed {
		return nil, mq_errors.ErrQueueClosed
	}

	q.Lock()
	defer q.Unlock()

	msgOffset := q.roffset
	if q.roffset == q.woffset {
		q.Unlock()
		// 等待消息写入
		hasMsg := <-q.notifyReadMsgChan
		q.Lock()
		if !hasMsg {
			return nil, nil
		}
	}

	// 消息结构 flag(1-byte) + status(2-bytes) + msg_len(4-bytes) + msg(n-bytes)
	// msg又包括expire(8-bytes) + id(8-bytes) + retry(2-bytes) + body(n-bytes)
	if flag := q.data[q.roffset]; flag != 'v' {
		//fmt.Println("xxxxx", string(flag), flag)
		return nil, errors.New("unknown msg flag")
	}

	msgLen := int64(binary.BigEndian.Uint32(q.data[q.roffset+3 : q.roffset+7]))
	msg := make([]byte, msgLen)
	copy(msg, q.data[q.roffset+7:q.roffset+7+msgLen])

	if isAutoAck {
		binary.BigEndian.PutUint16(q.data[q.roffset+1:q.roffset+3], uint16(MSG_STATUS_FIN))
		atomic.AddInt64(&q.num, -1)
	} else {
		binary.BigEndian.PutUint16(q.data[q.roffset+1:q.roffset+3], uint16(MSG_STATUS_WAIT))
		binary.BigEndian.PutUint64(q.data[q.roffset+7:q.roffset+15], uint64(time.Now().Unix())+uint64(q.topic.msgTTR))
		msgId := binary.BigEndian.Uint64(q.data[q.roffset+15 : q.roffset+23])
		q.waitAck[msgId] = msgOffset
	}

	q.roffset += MSG_FIX_LENGTH + msgLen
	return &readQueueData{msg, msgOffset}, nil
}

func (q *queue) updateMsgStatus(msgId uint64, offset int64, status int) {
	q.Lock()
	defer q.Unlock()

	queueMsgId := binary.BigEndian.Uint64(q.data[offset+15 : offset+23])
	if msgId != queueMsgId {
		q.logger.Error(fmt.Sprintf("invaild msgId, msgId %d, but queue.msg.id is %d", msgId, queueMsgId))
		return
	}

	if flag := q.data[offset]; flag != 'v' {
		q.logger.Error(fmt.Sprintf("invaild offset, offset : %d", offset))
		return
	}

	switch status {
	case MSG_STATUS_WAIT:
		q.waitAck[msgId] = offset
	case MSG_STATUS_FIN:
	default:
		q.logger.Error(fmt.Sprintf("invaild status %d", status))
		return
	}

	binary.BigEndian.PutUint16(q.data[offset+1:offset+3], uint16(status))
}

// 新写入信息的长度不能超过文件大小,超过则新建文件
func (q *queue) write(msg []byte) error {
	if q.closed {
		return mq_errors.ErrQueueClosed
	}

	q.Lock()
	defer q.Unlock()

	msgLen := int64(len(msg))
	if q.woffset+MSG_FIX_LENGTH+msgLen > q.filesize {
		// 文件大小不够,需要扩展文件
		if err := q.grow(); err != nil {
			return err
		}
	}

	// package = flag(1-byte) + status(2-bytes) + msgLen(4-bytes) + msg(n-bytes)
	copy(q.data[q.woffset:q.woffset+1], []byte{'v'})
	binary.BigEndian.PutUint16(q.data[q.woffset+1:q.woffset+3], uint16(MSG_STATUS_DEFAULT))
	binary.BigEndian.PutUint32(q.data[q.woffset+3:q.woffset+7], uint32(msgLen))
	copy(q.data[q.woffset+7:q.woffset+7+msgLen], msg)

	q.woffset += MSG_FIX_LENGTH + msgLen
	atomic.AddInt64(&q.num, 1)

	// 通知消费者已有消息，如果没有消费者等待notifyReadMsgChan
	// default分支用于保证不会阻塞生产者生产消息
	select {
	case q.notifyReadMsgChan <- true:
	default:
	}

	return nil
}

// 扩展文件大小,每次为GROW_SIZE
func (q *queue) grow() error {
	fz := q.filesize + GROW_SIZE
	if err := q.mmap(int(fz)); err != nil {
		return err
	}

	if runtime.GOOS != "windows" {
		if err := q.file.Truncate(fz); err != nil {
			return errors.New(fmt.Sprintf("file resize error: %s\n", err))
		}
	}
	if err := q.file.Sync(); err != nil {
		return errors.New(fmt.Sprintf("file sync error: %s\n", err))
	}

	q.logger.Info(fmt.Sprintf("grow %v.queue size to %v, and old is %v, default to %v", q.name, fz, q.filesize, GROW_SIZE))
	//q.filesize = fz
	return nil
}

// 映射文件
//func (q *queue) mmap(size int) error {
//	stat, err := q.file.Stat()
//	if err != nil {
//		return fmt.Errorf("mmap %v.queue failed, %v.\n", q.name, err)
//	}
//	if stat.Size() == 0 {
//		return fmt.Errorf("mmap %v.queue failed, file is empty.\n", q.name)
//	}
//
//	// 解除上一次映射,如果有的话
//	if len(q.data) > 0 {
//		if err := q.unmap(); nil != err {
//			return err
//		}
//	}
//
//	if data,err := mMap(q.file.Fd(), size); err != nil {
//		return err
//	}else {
//		q.data
//	}
//
//	q.filesize = stat.Size()
//	return nil
//}
