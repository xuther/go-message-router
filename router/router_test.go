package router

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/xuther/go-message-router/common"
	"github.com/xuther/go-message-router/publisher"
	"github.com/xuther/go-message-router/subscriber"
)

func TestRouter(t *testing.T) {

	routingGuide := make(map[string][]string)

	routingGuide["a"] = []string{"b", "c"}
	routingGuide["b"] = []string{"a"}
	routingGuide["c"] = []string{"d"}

	pub, err := publisher.NewPublisher("60000", 1000, 10)
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	go pub.Listen()
	time.Sleep(1 * time.Second) //give the os time to bind teh tcp port

	var wg sync.WaitGroup
	wg.Add(3)

	_, err = Start(routingGuide, wg, 1000, []string{"localhost:60000"}, "60005")
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	time.Sleep(1 * time.Second) //give the os time to bind teh tcp port

	sub, err := subscriber.NewSubscriber(3000)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	sub.Subscribe("localhost:60005", []string{"a", "b", "c", "d"})

	headera := [24]byte{}
	copy(headera[:], "a")

	headerb := [24]byte{}
	copy(headerb[:], "b")

	headerc := [24]byte{}
	copy(headerb[:], "c")

	pub.Write(common.Message{headera, []byte("a->b|a->c")})
	pub.Write(common.Message{headerb, []byte("b -> a")})
	pub.Write(common.Message{headerc, []byte("c -> d")})

	a := sub.Read()
	fmt.Printf("|")
	b := sub.Read()
	fmt.Printf("|")
	c := sub.Read()
	fmt.Printf("|")
	d := sub.Read()
	fmt.Printf("|\n")

	log.Printf("%s - %s", a.MessageHeader, a.MessageBody)
	log.Printf("%s - %s", b.MessageHeader, b.MessageBody)
	log.Printf("%s - %s", c.MessageHeader, c.MessageBody)
	log.Printf("%s - %s", d.MessageHeader, d.MessageBody)
}
