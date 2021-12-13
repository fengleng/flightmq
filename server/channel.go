package server

import (
	"fmt"
	"github.com/fengleng/flightmq/common"
	"github.com/fengleng/flightmq/log"
	"github.com/pingcap/errors"
	"sync"
	"time"
)

type Channel struct {
	key         string
	conns       map[*TcpConn]bool
	exitChan    chan struct{}
	pushMsgChan chan []byte
	wg          common.WaitGroupWrapper
	sync.RWMutex

	logger log.Logger
}

func NewChannel(key string) *Channel {
	ch := &Channel{
		key: key,
		//ctx:         ctx,
		exitChan:    make(chan struct{}),
		conns:       make(map[*TcpConn]bool),
		pushMsgChan: make(chan []byte),
		logger: log.NewFileLogger(),
	}
	ch.wg.Wrap(ch.distribute)
	return ch
}

// exit channel
func (c *Channel) exit() {
	//c.ctx.Dispatcher.RemoveChannel(c.key)
	close(c.exitChan)
	c.wg.Wait()
}

// add connection to channel
func (c *Channel) addConn(tcpConn *TcpConn) error {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.conns[tcpConn]; ok {
		return errors.Errorf("client %s had connection.", tcpConn.conn.LocalAddr().String())
	}

	start := make(chan struct{})
	c.wg.Wrap(func() {
		start <- struct{}{}
		select {
		case <-tcpConn.exitChan:
			// delete connection on close
			//c.LogInfo(fmt.Sprintf("connection %s has exit.", tcpConn.conn.RemoteAddr()))
			delete(c.conns, tcpConn)

			// exit channel when the number of connections is zero
			if len(c.conns) == 0 {
				c.exit()
			}
			return
		case <-c.exitChan:
			return
		}
	})

	// wait for goroutine to start
	<-start
	c.conns[tcpConn] = true

	return nil
}

// publish message to all connections
func (c *Channel) publish(msg []byte) error {
	c.RLock()
	defer c.RUnlock()

	if len(c.conns) == 0 {
		return errors.Errorf("no subscribers")
	}

	select {
	case <-time.After(10 * time.Second):
		return errors.Errorf("timeout.")
	case c.pushMsgChan <- msg:
		return nil
	}
}

func (c *Channel) distribute() {
	for {
		select {
		case <-c.exitChan:
			c.logger.Error(fmt.Sprintf("channel %s has exit distribute.", c.key))
			return
		case msg := <-c.pushMsgChan:
			c.RLock()
			for tcpConn, _ := range c.conns {
				_ = tcpConn.Send(RESP_CHANNEL, msg)
			}
			c.RUnlock()
		}
	}
}
