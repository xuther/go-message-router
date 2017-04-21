package subscriber

import (
	"log"
	"testing"
	"time"

	"github.com/xuther/go-message-router/common"
	"github.com/xuther/go-message-router/publisher"
)

func TestSubscribe(t *testing.T) {
	publisher, err := publisher.NewPublisher("60000", 1000, 10)
	if err != nil {
		t.Error(err)
	}
	go publisher.Listen()

	sub, err := NewSubscriber(10)
	if err != nil {
		t.Error(err)
	}
	sub.Subscribe("localhost:60000", []string{"a"})
	header := [24]byte{}
	copy(header[:], "a")

	time.Sleep(1 * time.Second)
	log.Printf("Writing")

	publisher.Write(common.Message{header, []byte("test")})
	log.Printf("Reading")

	test := sub.Read()

	log.Printf("%v", test)
}
