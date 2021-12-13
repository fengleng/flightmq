package server

import (
	"os"
	"testing"
)

func TestMMap(t *testing.T) {
	f, err := os.OpenFile("my.txt", os.O_CREATE|os.O_RDWR, 0666)
	if err!=nil {
		t.Fatal(err)
	}
	stat, err := f.Stat()
	var size int64 = 0
	if stat.Size()==0 {
		size = int64(os.Getpagesize())
		n, err := f.WriteAt([]byte{0}, size-1)
		if err!=nil {
			t.Fatal(err)
		}
		t.Log(n)
	}
	data, err := mMap(f.Fd(), int(size))
	for i, b := range []byte("hello,world!") {
		data[i]=b
	}
}
