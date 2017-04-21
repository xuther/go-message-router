package subscriber

import (
	"fmt"
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

	for i := 0; i < 100; i++ {
		message := fmt.Sprintf("test %v\n", i)

		publisher.Write(common.Message{header, []byte(message)})
	}

	log.Printf("Reading")

	for i := 0; i < 100; i++ {
		test := sub.Read()
		log.Printf("%v", test)

	}
}
