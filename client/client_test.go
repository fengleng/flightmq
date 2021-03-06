package client

import (
	"encoding/json"
	"strconv"
	"testing"
)

func TestClient_Set(t *testing.T) {
	client := NewClient("127.0.0.1:9503", 1)
	//res,err := client.Set("test2", 1,1,30,5)
	//
	////recv, bytes := client.Receive()
	//t.Log(string(res))
	////t.Log(string(bytes))
	//t.Log(err)

	bytes, err := client.Ping()
	t.Log(string(bytes))
	//t.Log(string(bytes))
	t.Log(err)
}

func TestClient_Declare(t *testing.T) {
	client := NewClient("127.0.0.1:9503", 1)

	bytes, err := client.Declare("test1", "queue1")
	t.Log(err)
	t.Log(string(bytes))
}

func TestClient_Push(t *testing.T) {
	c := NewClient("127.0.0.1:9503", 1)
	bytes, err := c.Push(MsgPkg{Topic: "test1", Delay: 0, RouteKey: "queue1", Body: "TestClientPopAndAck"})
	t.Log(err)
	t.Log(string(bytes))
}

func TestClientMMPub(t *testing.T) {
	total := 100
	var msgList []MMsgPkg
	for i := 0; i < total; i++ {
		msgList = append(msgList, MMsgPkg{"golang_" + strconv.Itoa(i*i), 0})
	}
	c := NewClient("127.0.0.1:9503", 1)
	msgIds, err := c.Mpush("test1", msgList, "queue1")
	t.Log(err)
	t.Log(msgIds)
}

func TestClientSet(t *testing.T) {
	c := NewClient("127.0.0.1:9503", 1)
	bytes, err := c.Set("test1", 0, 2, 2, 10)
	t.Log(err)
	t.Log(string(bytes))
}

func TestClientForPop(t *testing.T) {
	for true {
		c := NewClient("127.0.0.1:9503", 1)
		bytes, err := c.Pop("test1", "queue1")
		t.Log(err)
		t.Log(string(bytes))
	}
}

func TestClientPopAndAck(t *testing.T) {
	c := NewClient("127.0.0.1:9503", 1)
	bytes, err := c.Pop("test1", "queue1")
	t.Log(err)

	var m RespMsgData
	err = json.Unmarshal(bytes, &m)
	t.Log(err)
	t.Log(string(bytes))
	err = c.Ack("test1", m.Id, "queue1")
	t.Log(err)
}

func TestClientDead(t *testing.T) {
	for true {
		c := NewClient("127.0.0.1:9503", 1)
		bytes, err := c.Dead("test1", "queue1")
		t.Log(err)
		t.Log(string(bytes))
	}
}

func TestClientPushDefault(t *testing.T) {
	c := NewClient("127.0.0.1:9503", 1)
	bytes, err := c.Push(MsgPkg{Topic: "test5", Delay: 0, RouteKey: "queue1", Body: "hello20"})
	t.Log(err)
	t.Log(string(bytes))
}

func TestClientPopDefault(t *testing.T) {
	c := NewClient("127.0.0.1:9503", 1)
	bytes, err := c.Pop("test4", "")
	t.Log(err)
	t.Log(string(bytes))
}
