package client

import "testing"

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
	bytes, err := c.Push(MsgPkg{Topic: "test1", Delay: 5, RouteKey: "queue1", Body: "hello"})
	t.Log(err)
	t.Log(string(bytes))
}

func TestClient_Pop(t *testing.T) {
	c := NewClient("127.0.0.1:9503", 1)
	bytes, err := c.Pop("test1", "queue1")
	t.Log(err)
	t.Log(string(bytes))
}
