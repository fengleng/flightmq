package server

import (
	"github.com/fengleng/flightmq/common"
	"os"
	"sync"
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
}

type readQueueData struct {
	data []byte
	pos  int64
}

func (q *queue) exit() {
	q.closed = true
	close(q.exitChan)
	close(q.notifyReadMsgChan)
	q.wg.Wait()
}
