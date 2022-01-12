package server

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/fengleng/flightmq/common"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

const (
	RespResult = 101
	RespError  = 102
	//RespPing    = 103
)

type TcpConn struct {
	conn         net.Conn
	serv         *TcpServ
	wg           common.WaitGroupWrapper
	exitChan     chan struct{}
	connExitChan chan struct{}
	once         sync.Once
	writer       *bufio.Writer
	reader       *bufio.Reader

	//logger log.Logger
}

func (c *TcpConn) Send(respType int16, respData []byte) error {
	var buf = make([]byte, 2+4+len(respData))
	binary.BigEndian.PutUint16(buf[:2], uint16(respType))
	binary.BigEndian.PutUint32(buf[2:6], uint32(len(respData)))
	copy(buf[6:], respData)
	_, err := c.conn.Write(buf)
	return err
}

// Handle <cmd_name> <param_1> ... <param_n>\n
func (c *TcpConn) Handle() {
	if err := c.conn.SetDeadline(time.Time{}); err != nil {
		c.serv.logger.Error(fmt.Sprintf("set deadlie failed, %s", err))
	}

	var buf bytes.Buffer // todo 待优化
	for {
		var err error

		// todo 如果是服务器自己退出，这里应该会报错的
		line, isPrefix, err := c.reader.ReadLine()
		if err != nil {
			if err != io.EOF {
				c.serv.logger.Error(fmt.Sprintf("connection error, %s", err))
			}
			break
		}
		if len(line) == 0 {
			c.serv.logger.Error("cmd is empty")
			break
		}
		buf.Write(line)
		if isPrefix { // conn.buffer is full,but we don't get '\n',continue to read
			continue
		}

		params := bytes.Split(buf.Bytes(), []byte(" "))
		buf.Reset() // reset buf after reading
		cmd := params[0]
		params = params[1:]

		switch {
		case bytes.Equal(cmd, []byte("pub")):
			err = c.PUB(params)
		case bytes.Equal(cmd, []byte("pop")):
			err = c.POP(params)
		case bytes.Equal(cmd, []byte("ack")):
			err = c.ACK(params)
		case bytes.Equal(cmd, []byte("mpub")):
			err = c.MPUB(params)
		case bytes.Equal(cmd, []byte("dead")):
			err = c.DEAD(params)
		case bytes.Equal(cmd, []byte("set")):
			err = c.SET(params)
		case bytes.Equal(cmd, []byte("queue")):
			err = c.DECLAREQUEUE(params)
		case bytes.Equal(cmd, []byte("subscribe")):
			err = c.SUBSCRIBE(params)
		case bytes.Equal(cmd, []byte("publish")):
			err = c.PUBLISH(params)
		case bytes.Equal(cmd, []byte("ping")):
			err = c.PING()
		default:
			err = NewClientErr(ErrUnkownCmd, fmt.Sprintf("unkown cmd: %s", cmd))
		}

		if err != nil {
			// response error to client
			if err := c.RespErr(err); err != nil {
				break
			}
			if _, ok := err.(*FatalClientErr); ok {
				break
			} else {
				continue
			}
		}
		select {
		case <-c.exitChan:
			break
		default:

		}
	}

	// force close conn
	_ = c.conn.Close()
	close(c.connExitChan) // notify channel to remove connection
}

// PUB <topic_name> <route_key> <delay-time>\n
// [ 4-byte size in bytes ][ N-byte binary data ]
func (c *TcpConn) PUB(params [][]byte) error {
	if len(params) != 3 {
		return NewFatalClientErr(ErrParams, "3 parameters required")
	}

	topic := string(params[0])
	routeKey := string(params[1])
	delay, _ := strconv.Atoi(string(params[2]))
	if delay > MSG_MAX_DELAY {
		return NewClientErr(ErrDelay, fmt.Sprintf("delay can't exceeding the maximum %d", MSG_MAX_DELAY))
	}

	bodyLenBuf := make([]byte, 4)
	_, err := io.ReadFull(c.reader, bodyLenBuf)
	if err != nil {
		return NewFatalClientErr(ErrReadConn, err.Error())
	}

	bodyLen := int(binary.BigEndian.Uint32(bodyLenBuf))
	body := make([]byte, bodyLen)
	_, err = io.ReadFull(c.reader, body)
	if err != nil {
		return NewFatalClientErr(ErrReadConn, err.Error())
	}

	cb := make([]byte, len(body))
	copy(cb, body)

	msgId, err := c.serv.dispatcher.push(topic, routeKey, cb, delay)
	if err != nil {
		return NewFatalClientErr(ErrReadConn, err.Error())
	}
	if err := c.RespRes([]byte(strconv.FormatUint(msgId, 10))); err != nil {
		return NewFatalClientErr(ErrResp, err.Error())
	}
	return nil
}

// MPUB <topic_name> <num>\n
// <msg.len> <[]byte({"delay":1,"body":"xxx","topic":"xxx","routeKey":"xxx"})>
// <msg.len> <[]byte({"delay":1,"body":"xxx","topic":"xxx","routeKey":"xxx"})>
func (c *TcpConn) MPUB(params [][]byte) error {
	var err error

	if len(params) != 2 {
		return NewFatalClientErr(ErrParams, "2 parameters required")
	}

	topic := string(params[0])
	num, _ := strconv.Atoi(string(params[1]))
	if num <= 0 || num > c.serv.cfg.MsgMaxPushNum {
		return NewFatalClientErr(ErrPushNum, fmt.Sprintf("number of push must be between 1 and %v", c.serv.cfg.MsgMaxPushNum))
	}

	msgIds := make([]uint64, num)
	for i := 0; i < num; i++ {
		msgLenBuf := make([]byte, 4)
		_, err = io.ReadFull(c.reader, msgLenBuf)
		if err != nil {
			return NewFatalClientErr(ErrReadConn, err.Error())
		}

		msgLen := int(binary.BigEndian.Uint32(msgLenBuf))
		msg := make([]byte, msgLen)
		_, err = io.ReadFull(c.reader, msg)
		if err != nil {
			return NewFatalClientErr(ErrReadConn, err.Error())
		}

		var recvMsg RecvMsgData
		if err := json.Unmarshal(msg, &recvMsg); err != nil {
			//c.RespErr(fmt.Errorf("decode msg failed, %s", err))
			return err
		}

		msgId, err := c.serv.dispatcher.push(topic, recvMsg.RouteKey, []byte(recvMsg.Body), recvMsg.Delay)
		if err != nil {
			return NewClientErr(ErrPush, err.Error())
		}

		msgIds[i] = msgId
	}

	nBytes, err := json.Marshal(msgIds)
	if err != nil {
		return NewClientErr(ErrJson, err.Error())
	}

	if err := c.RespRes(nBytes); err != nil {
		return NewFatalClientErr(ErrResp, err.Error())
	}

	return nil
}

// POP 消费消息
// pop <topic_name> <bind_key>\n
func (c *TcpConn) POP(params [][]byte) error {
	if len(params) != 2 {
		return NewFatalClientErr(ErrParams, "2 parameters required.")
	}

	topic := string(params[0])
	if len(topic) == 0 {
		return NewFatalClientErr(ErrParams, "topic name required.")
	}

	bindKey := string(params[1])
	t := c.serv.dispatcher.GetTopic(topic)

	msg, err := t.pop(bindKey)
	if err != nil {
		return NewClientErr(ErrPopMsg, fmt.Sprintf("bindKey:%s can't pop data", bindKey))
	}
	if err := c.RespMsg(msg); err != nil {
		q := t.getQueueByBindKey(bindKey)
		if q == nil {
			return NewClientErr(ErrPopMsg, fmt.Sprintf("bindKey:%s can't match queue", bindKey))
		}
		if !t.isAutoAck {
			_ = q.ack(msg.Id)
		}

		// client disconnected unexpectedly
		// add to the queue again to ensure message is not lose
		c.serv.logger.Error(fmt.Sprintf("client disconnected unexpectedly, the message is written to queue again. message.id %d", msg.Id))
		_ = q.write(Encode(msg))
		return NewFatalClientErr(ErrReadConn, err.Error())
	}
	return nil
}

// ACK 确认消息
// ack <message_id> <topic> <bind_key>\n
func (c *TcpConn) ACK(params [][]byte) error {
	if len(params) != 3 {
		return NewFatalClientErr(ErrParams, "3 parameters required")
	}

	msgId, _ := strconv.ParseInt(string(params[0]), 10, 64)
	topic := string(params[1])
	bindKey := string(params[2])

	if err := c.serv.dispatcher.ack(topic, uint64(msgId), bindKey); err != nil {
		return NewClientErr(ErrAckMsg, err.Error())
	}

	if err := c.RespOk(); err != nil {
		return NewFatalClientErr(ErrResp, err.Error())
	}

	return nil
}

// DEAD 死信队列消费
// dead <topic_name> <bind_key>\n
func (c *TcpConn) DEAD(params [][]byte) error {
	if len(params) != 2 {
		return NewFatalClientErr(ErrParams, "2 parameters required")
	}

	topic := string(params[0])
	bindKey := string(params[1])
	msg, err := c.serv.dispatcher.dead(topic, bindKey)
	if err != nil {
		return NewClientErr(ErrDead, err.Error())
	}

	if err := c.RespMsg(msg); err != nil {
		return NewFatalClientErr(ErrResp, err.Error())
	}

	return nil
}

// SET topic isAutoAck mode msgTTR msgRetry
func (c *TcpConn) SET(params [][]byte) error {
	if len(params) != 5 {
		return NewFatalClientErr(ErrParams, "params equal 5")
	}
	topic := string(params[0])
	if len(topic) == 0 {
		return NewFatalClientErr(ErrTopicEmpty, "topic is empty")
	}

	configure := &topicConfigure{}
	configure.isAutoAck, _ = strconv.Atoi(string(params[1]))
	configure.mode, _ = strconv.Atoi(string(params[2]))
	configure.msgTTR, _ = strconv.Atoi(string(params[3]))
	configure.msgRetry, _ = strconv.Atoi(string(params[4]))

	err := c.serv.dispatcher.Set(topic, configure)
	if err != nil {
		return NewClientErr(ErrSet, err.Error())
	}
	if err := c.RespOk(); err != nil {
		return NewFatalClientErr(ErrResp, err.Error())
	}
	return nil
}

// DECLAREQUEUE declare queue
// queue <topic_name> <bind_key>\n
func (c *TcpConn) DECLAREQUEUE(params [][]byte) error {
	if len(params) != 2 {
		return NewFatalClientErr(ErrParams, "2 parameters required")
	}

	topic := string(params[0])
	if len(topic) == 0 {
		return NewFatalClientErr(ErrTopicEmpty, "topic name required")
	}
	bindKey := string(params[1])
	if len(bindKey) == 0 {
		return NewFatalClientErr(ErrBindKeyEmpty, "bind key required")
	}

	if err := c.serv.dispatcher.declareQueue(topic, bindKey); err != nil {
		return NewClientErr(ErrDeclare, err.Error())
	}
	if err := c.RespOk(); err != nil {
		return NewFatalClientErr(ErrResp, err.Error())
	}

	return nil
}

// SUBSCRIBE subscribe channel
// subscribe <channel_name> \n
func (c *TcpConn) SUBSCRIBE(params [][]byte) error {
	if len(params) != 1 {
		return NewFatalClientErr(ErrParams, "1 parameters required")
	}

	channelName := string(params[0])
	if len(channelName) == 0 {
		return NewFatalClientErr(ErrChannelEmpty, "channel name is empty")
	}

	if err := c.serv.dispatcher.subscribe(channelName, c); err != nil {
		return NewClientErr(ErrSubscribe, err.Error())
	}

	if err := c.RespOk(); err != nil {
		return NewFatalClientErr(ErrResp, err.Error())
	}

	return nil
}

// PUBLISH message to channel
// publish <channel_name>\n
// <message_len> <message>
func (c *TcpConn) PUBLISH(params [][]byte) error {
	if len(params) != 1 {
		return NewFatalClientErr(ErrParams, "1 parameters required")
	}

	channelName := string(params[0])
	if len(channelName) == 0 {
		return NewFatalClientErr(ErrChannelEmpty, "channel name required")
	}

	bodylenBuf := make([]byte, 4)
	_, err := io.ReadFull(c.reader, bodylenBuf)
	if err != nil {
		return NewFatalClientErr(ErrReadConn, err.Error())
	}

	bodyLen := int(binary.BigEndian.Uint32(bodylenBuf))
	body := make([]byte, bodyLen)
	_, err = io.ReadFull(c.reader, body)
	if err != nil {
		return NewFatalClientErr(ErrReadConn, err.Error())
	}

	if err := c.serv.dispatcher.publish(channelName, body); err != nil {
		return NewClientErr(ErrPublish, err.Error())
	}

	if err := c.RespOk(); err != nil {
		return NewFatalClientErr(ErrResp, err.Error())
	}

	return nil
}

func (c *TcpConn) PING() error {
	return c.RespPing()
}

func (c *TcpConn) exit() {
	_ = c.conn.Close()
}

func (c *TcpConn) RespMsg(msg *Msg) error {
	msgData := RespMsgData{}
	msgData.Body = string(msg.Body)
	msgData.Retry = msg.Retry
	msgData.Id = strconv.FormatUint(msg.Id, 10)

	data, err := json.Marshal(msgData)
	if err != nil {
		c.serv.logger.Error("%v", err)
		return err
	}

	err = c.Send(RespResult, data)
	if err != nil {
		c.serv.logger.Error("%v", err)
		return err
	}
	return nil
}

func (c *TcpConn) RespErr(err error) error {
	err2 := c.Send(RespError, []byte(err.Error()))
	if err2 != nil {
		c.serv.logger.Error("%v", err)
		return err2
	}
	return nil
}

func (c *TcpConn) RespRes(res []byte) error {
	err := c.Send(RespResult, res)
	if err != nil {
		c.serv.logger.Error("%v", err)
		return err
	}
	return nil
}

func (c *TcpConn) RespOk() error {
	err := c.Send(RespResult, []byte("ok"))
	if err != nil {
		c.serv.logger.Error("%v", err)
		return err
	}
	return nil
}

func (c *TcpConn) RespPing() error {
	err := c.Send(RespResult, []byte("ping"))
	if err != nil {
		c.serv.logger.Error("%v", err)
		return err
	}
	return nil
}
