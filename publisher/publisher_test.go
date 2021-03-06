package publisher

import (
	"log"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/xuther/go-message-router/common"
)

func TestPublish(t *testing.T) {

	publisher, err := NewPublisher("60000", 100, 10)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	go publisher.Listen()

	var dialer *websocket.Dialer
	conn, _, err := dialer.Dial("ws://localhost:60000/subscribe", nil)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	publisher.Write(common.Message{MessageBody: []byte("this is a test")})

	var toRead common.Message

	log.Printf("%v", &conn)

	err = conn.ReadJSON(&toRead)

	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	log.Printf("%s", toRead.MessageBody)
	if string(toRead.MessageBody) != "this is a test" {
		t.FailNow()
	}
}

/*
func BenchmarkPublisher(b *testing.B) {
	time.Sleep(time.Second)

	log.Printf("Running on %v", b.N)
	publisher, err := NewPublisher("60000", 1000, 10)
	if err != nil {
		b.Error(err)
	}

	go publisher.Listen()

	radder, err := net.ResolveTCPAddr("tcp", "localhost:60000")
	if err != nil {
		b.Error(err)
	}

	conn, err := net.DialTCP("tcp", nil, radder)
	if err != nil {
		b.Error(err)
	}

	conn1, err := net.DialTCP("tcp", nil, radder)
	if err != nil {
		b.Error(err)
	}

	defer conn1.Close()
	defer conn.Close()
	time.Sleep(time.Second)

	toRead := make([]byte, 30)
	go func() {
		conn.Read(toRead)
	}()
	toRead1 := make([]byte, 30)
	go func() {
		conn1.Read(toRead1)
	}()

	header := [24]byte{}
	copy(header[:], "TestHeader")
	message := common.Message{header, []byte("test")}

	starttime := time.Now()
	for n := 0; n < 100000; n++ {
		time.Sleep(time.Nanosecond * 1000)
		publisher.Write(message)
	}
	endTime := time.Now()
	time.Sleep(time.Second)

	d := endTime.Sub(starttime)
	perSec := 100000 / float64(d/time.Second)

	log.Printf("Messages per second: %v", perSec)

}

*/
