package subscriber

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
		message := fmt.Sprintf("%v", i)
		publisher.Write(common.Message{header, []byte(message)})
	}

	log.Printf("Reading")

	for i := 0; i < 100; i++ {
		sub.Read()
	}
}

func TestFilter(t *testing.T) {
	publisher, err := publisher.NewPublisher("60001", 1000, 10)
	if err != nil {
		t.Error(err)
	}
	go publisher.Listen()

	sub, err := NewSubscriber(200)
	if err != nil {
		t.Error(err)
	}
	err = sub.Subscribe("localhost:60001", []string{"a"})
	if err != nil {
		t.Error(err)
	}

	headera := [24]byte{}
	copy(headera[:], "a")

	headerb := [24]byte{}
	copy(headerb[:], "b")

	time.Sleep(1 * time.Second)

	log.Printf("Writing..")

	for i := 0; i < 2; i++ {
		message := fmt.Sprintf("%v", i)
		publisher.Write(common.Message{headera, []byte(message)})
		publisher.Write(common.Message{headerb, []byte(message)})
	}

	log.Printf("Reading..")

	test1 := sub.Read()
	if test1.MessageBody[0] != '0' || test1.MessageHeader != headera {
		t.Fail()
	}

	test2 := sub.Read()
	if test2.MessageBody[0] != '1' || test2.MessageHeader != headera {
		t.Fail()
	}
}

func TestMultipleSubscriber(t *testing.T) {
	//create publishers
	pub, err := publisher.NewPublisher("60002", 1000, 10)
	if err != nil {
		t.Error(err)
	}
	go pub.Listen()

	pub1, err := publisher.NewPublisher("60003", 1000, 10)
	if err != nil {
		t.Error(err)
	}
	go pub1.Listen()

	//create subscriber
	sub, err := NewSubscriber(200)
	if err != nil {
		t.Error(err)
	}

	//subscribe
	err = sub.Subscribe("localhost:60002", []string{"a"})
	if err != nil {
		t.Error(err)
	}
	err = sub.Subscribe("localhost:60003", []string{"a", "b"})
	if err != nil {
		t.Error(err)
	}
	headera := [24]byte{}
	copy(headera[:], "a")

	headerb := [24]byte{}
	copy(headerb[:], "b")

	time.Sleep(1 * time.Second)

	log.Printf("Writing..")
	for i := 0; i < 2; i++ {
		message := fmt.Sprintf("1-%v", i)
		pub.Write(common.Message{headera, []byte(message)})
		pub.Write(common.Message{headerb, []byte(message)})
	}
	for i := 0; i < 100; i++ {
		message := fmt.Sprintf("2-%v", i)
		pub1.Write(common.Message{headera, []byte(message)})
		pub1.Write(common.Message{headerb, []byte(message)})
	}

	counts := make(map[string]int)

	//read all the messages
	for i := 0; i < 202; i++ {
		message := sub.Read()
		if message.MessageBody[0] == '1' && message.MessageHeader != headera {
			t.Error("Filter failed")
			t.Fail()
		} else if message.MessageBody[0] == '1' && message.MessageHeader == headera {
			counts["1a"]++
		} else if message.MessageBody[0] == '2' && message.MessageHeader == headera {
			counts["2a"]++
		} else if message.MessageBody[0] == '2' && message.MessageHeader == headerb {
			counts["2b"]++
		}
	}

	assert.Equal(t, counts["1a"], 2)
	assert.Equal(t, counts["2a"], 100)
	assert.Equal(t, counts["2b"], 100)

}
