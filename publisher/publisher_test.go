package publisher

import (
	"log"
	"net"
	"testing"
	"time"

	"github.com/xuther/go-message-router/common"
)

func TestPublish(t *testing.T) {

	publisher, err := NewPublisher("60000", 100, 10)
	if err != nil {
		t.Error(err)
	}

	go publisher.Listen()

	radder, err := net.ResolveTCPAddr("tcp", "localhost:60000")
	if err != nil {
		t.Error(err)
	}

	conn, err := net.DialTCP("tcp", nil, radder)
	if err != nil {
		t.Error(err)
	}
	time.Sleep(time.Second)

	log.Printf("Connection1 Esablished")

	header := [24]byte{}
	copy(header[:], "testing")

	publisher.Write(common.Message{header, []byte("test")})

	toRead := make([]byte, 30)
	_, err = conn.Read(toRead)
	if err != nil {
		t.Error(err)
	}

	if toRead[4] == 't' {
		log.Printf("Incorrect Value")
		t.Fail()
	}

	conn1, err := net.DialTCP("tcp", nil, radder)
	if err != nil {
		t.Error(err)
	}
	defer conn1.Close()
	defer conn.Close()
	time.Sleep(time.Second)

	log.Printf("Connection1 Esablished")

	header = [24]byte{}
	copy(header[:], "Header2")

	publisher.Write(common.Message{header, []byte("test")})

	toRead = make([]byte, 30)
	_, err = conn.Read(toRead)
	if err != nil {
		t.Error(err)
	}

	if toRead[4] == 'd' {
		log.Printf("Incorrect Value")
		t.Fail()
	}

	toRead = make([]byte, 30)
	_, err = conn1.Read(toRead)
	if err != nil {
		t.Error(err)
	}

	if toRead[4] == 'd' {
		log.Printf("Incorrect Value")
		t.Fail()
	}

}

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

	log.Printf("time Taken: %v", perSec)

}
