package server

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/fengleng/flightmq/common"
	"github.com/fengleng/flightmq/log"
	"io"
	"net"
	"sync"
	"time"
)

const (
	RESP_MESSAGE = 101
	RESP_ERROR   = 102
	RESP_RESULT  = 103
	RESP_CHANNEL = 104
	RESP_PING    = 105
)

type TcpConn struct {
	conn     net.Conn
	serv     *TcpServ
	wg       common.WaitGroupWrapper
	exitChan chan struct{}
	once     sync.Once
	writer   *bufio.Writer
	reader   *bufio.Reader

	logger log.Logger
}

func (c *TcpConn) Send(respType int16, respData []byte) error {
	var buf = make([]byte, 2+4+len(respData))
	binary.BigEndian.PutUint16(buf[:2], uint16(respType))
	binary.BigEndian.PutUint32(buf[2:6], uint32(len(respData)))
	copy(buf[6:], respData)
	_, err := c.conn.Write(buf)
	return err
}

// <cmd_name> <param_1> ... <param_n>\n
func (c *TcpConn) Handle() {
	if err := c.conn.SetDeadline(time.Time{}); err != nil {
		c.logger.Error(fmt.Sprintf("set deadlie failed, %s", err))
	}

	var buf bytes.Buffer // todo 待优化
	for {
		var err error

		// todo 如果是服务器自己退出，这里应该会报错的
		line, isPrefix, err := c.reader.ReadLine()
		if err != nil {
			if err != io.EOF {
				c.logger.Error(fmt.Sprintf("connection error, %s", err))
			}
			break
		}
		if len(line) == 0 {
			c.logger.Error("cmd is empty")
			break
		}
		buf.Write(line)
		if isPrefix { // conn.buffer is full,but we don't get '\n',continue to read
			continue
		}

		params := bytes.Split(buf.Bytes(), []byte(" "))
		buf.Reset() // reset buf after reading
		if len(params) < 2 {
			c.logger.Error("params muset be greater than 2")
			break
		}

		cmd := params[0]
		params = params[1:]

		switch {
		//case bytes.Equal(cmd, []byte("pub")):
		//	err = c.PUB(params)
		//case bytes.Equal(cmd, []byte("pop")):
		//	err = c.POP(params)
		//case bytes.Equal(cmd, []byte("ack")):
		//	err = c.ACK(params)
		//case bytes.Equal(cmd, []byte("mpub")):
		//	err = c.MPUB(params)
		//case bytes.Equal(cmd, []byte("dead")):
		//	err = c.DEAD(params)
		//case bytes.Equal(cmd, []byte("set")):
		//	err = c.SET(params)
		//case bytes.Equal(cmd, []byte("queue")):
		//	err = c.DECLAREQUEUE(params)
		//case bytes.Equal(cmd, []byte("subscribe")):
		//	err = c.SUBSCRIBE(params)
		//case bytes.Equal(cmd, []byte("publish")):
		//	err = c.PUBLISH(params)
		//case bytes.Equal(cmd, []byte("ping")):
		//	c.PING()
		default:
			err = NewClientErr(ErrUnkownCmd, fmt.Sprintf("unkown cmd: %s", cmd))
		}

		if err != nil {
			// response error to client
			if err := c.Send(RESP_ERROR, []byte(err.Error())); err != nil {
				break
			}
			// fatal error must be closed automatically
			if _, ok := err.(*FatalClientErr); ok {
				break
			} else {
				continue
			}
		}
	}

	// force close conn
	_ = c.conn.Close()
	close(c.exitChan) // notify channel to remove connection
}

func (c *TcpConn) exit() {
	c.conn.Close()
}
