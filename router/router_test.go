package router

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

	r := Router{}

	err = r.Start(routingGuide, wg, 1000, []string{"localhost:60000"}, 0, 0, "60005")
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	sub, err := subscriber.NewSubscriber(3000)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	sub.Subscribe("localhost:60005", []string{"a", "b", "c", "d"})

	time.Sleep(1 * time.Second) //give the os time to bind teh tcp port

	headera := [24]byte{}
	copy(headera[:], "a")

	headerb := [24]byte{}
	copy(headerb[:], "b")

	headerc := [24]byte{}
	copy(headerc[:], "c")

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

//test remote subscription based on sending a subscribe packet
func TestSocketSubscription(t *testing.T) {

	//Start setup

	routingGuide := make(map[string][]string)

	routingGuide["a"] = []string{"b"}
	routingGuide["c"] = []string{"d", "b"}
	routingGuide["d"] = []string{"b"}

	pub, err := publisher.NewPublisher("60010", 1000, 10)
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	go pub.Listen()
	time.Sleep(1 * time.Second) //give the os time to bind the tcp port

	var wg1 sync.WaitGroup
	wg1.Add(3)

	r1 := Router{}

	err = r1.Start(routingGuide, wg1, 1000, []string{"localhost:60012"}, 10, time.Second*5, "60011")
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	var wg2 sync.WaitGroup
	wg2.Add(3)

	time.Sleep(1 * time.Second) //give the os time to bind the tcp port

	r2 := Router{}

	err = r2.Start(routingGuide, wg2, 1000, []string{"localhost:60011", "localhost:60010"}, 10, time.Second*1, "60012")
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	sub, err := subscriber.NewSubscriber(3000)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	sub.Subscribe("localhost:60011", []string{"a", "b", "c"})
	sub1, err := subscriber.NewSubscriber(3000)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	sub1.Subscribe("localhost:60012", []string{"a", "b", "c"})
	time.Sleep(1 * time.Second) //give the os time to bind the tcp port

	headera := [24]byte{}
	copy(headera[:], "a")

	headerb := [24]byte{}
	copy(headerb[:], "b")

	headerc := [24]byte{}
	copy(headerc[:], "c")

	headerd := [24]byte{}
	copy(headerd[:], "d")

	//End Setup
	pub.Write(common.Message{MessageHeader: headera, MessageBody: []byte("first")})

	//should be in Sub1 not in Sub2
	temp := sub1.Read()
	assert.Equal(t, temp.MessageHeader, headerb)
	assert.Equal(t, temp.MessageBody, []byte("first"))
	log.Printf("First message sent/received")

	time.Sleep(5 * time.Second) //give the os time to bind the tcp port

	pub.Write(common.Message{MessageHeader: headerc, MessageBody: []byte("second")})
	log.Printf("Second message sent/received")

	temp1 := sub.Read()
	temp2 := sub1.Read()

	assert.Equal(t, temp1.MessageHeader, headerb)
	assert.Equal(t, temp1.MessageBody, []byte("second"))

	assert.Equal(t, temp2.MessageHeader, headerb)
	assert.Equal(t, temp2.MessageBody, temp1.MessageBody)
}
