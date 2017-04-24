package router

import (
	"errors"
	"fmt"
	"log"
	"regexp"
	"sync"

	"github.com/xuther/go-message-router/common"
	"github.com/xuther/go-message-router/publisher"
	"github.com/xuther/go-message-router/subscriber"
)

func Publisher(messageChan <-chan common.Message, exit <-chan bool, port string, wg sync.WaitGroup) error {

	log.Printf("Publisher: Starting publisher")
	pub, err := publisher.NewPublisher(port, 1000, 10)
	if err != nil {
		wg.Done()
		return err
	}
	defer pub.Close()
	go pub.Listen()

	log.Printf("Publisher: Publisher ready.")
	go func() {
		for {
			select {
			case message := <-messageChan:
				pub.Write(message)
			case <-exit:
				wg.Done()
				return
			}
		}
	}()
	return nil
}

func Reciever(messageChan chan<- common.Message, exit <-chan bool, addressChan chan string, messageTypes []string, wg sync.WaitGroup) error {
	log.Printf("Reciever: staring reciever")
	sub, err := subscriber.NewSubscriber(3000)
	if err != nil {
		wg.Done()
		return err
	}

	go func() {
		for {
			subscriptions := []string{}
			select {
			case addr, ok := <-addressChan:
				if ok {
					alreadySubbed := false
					log.Printf("Reciever: Starting connection with %s", addr)
					for _, t := range subscriptions {
						if t == addr {
							alreadySubbed = true
							break
						}
					}
					if alreadySubbed {
						continue
					}

					err = sub.Subscribe(addr, messageTypes)
					if err != nil {
						log.Printf("Reciever: ERROR: %s", err.Error())
						continue
					}

					//note that we already have a subscription so we won't run one again
					subscriptions = append(subscriptions, addr)
				} else {
					log.Printf("Reciever: Error - subscription channel closed")
					return
				}
			}
		}
	}()

	log.Printf("Reciever: reciever ready")
	//Start listening
	go func() {
		for {

			//check for new subscription messages
			tempMessage := sub.Read()
			if tempMessage.MessageHeader == [24]byte{
				1, 1, 1, 1,
				1, 1, 1, 1,
				1, 1, 1, 1,
				1, 1, 1, 1,
				1, 1, 1, 1,
				1, 1, 1, 1,
			} {
				address := fmt.Sprintf("%s", tempMessage.MessageBody)
				addressChan <- address
				continue
			}
			messageChan <- tempMessage

			//check for exit command
			select {
			case <-exit:
				wg.Done()
				return
			default:
				continue
			}
		}
	}()

	return nil
}

//Start starts the router on teh specified port, returns an address channel that may be used to add more subscriptions
func Start(routingGuide map[string][]string, wg sync.WaitGroup, channelSize int, addresses []string, publisherPort string) (chan<- string, error) {
	//Build our channel
	inChan := make(chan common.Message, channelSize)
	outChan := make(chan common.Message, channelSize)
	exitChan := make(chan bool, 3)
	addressChan := make(chan string)

	toListen := []string{}

	//start our publisher
	//func Publisher(messageChan <-chan common.Message, exit <-chan bool, port int, wg sync.WaitGroup) error {
	err := Publisher(outChan, exitChan, publisherPort, wg)
	if err != nil {
		return addressChan, err
	}

	//start our router
	err = Router(inChan, outChan, exitChan, wg, routingGuide)
	if err != nil {
		return addressChan, err
	}

	//build our list of things to listen to.
	for k, _ := range routingGuide {
		toListen = append(toListen, k)
	}

	err = Reciever(inChan, exitChan, addressChan, toListen, wg)
	if err != nil {
		return addressChan, err
	}

	//subscribe to all the addresses
	for _, addr := range addresses {
		addressChan <- addr
	}
	return addressChan, nil
}

//The router takes messages in, relabels them according to the routing guide, and then outputs them.
//Note that the routing guide takes a first rule matched approach, so ensure that more specific rules are defined first
func Router(inChan <-chan common.Message, outChan chan<- common.Message, exit <-chan bool, wg sync.WaitGroup, routingGuide map[string][]string) error {
	log.Printf("Router: starting router")

	workingGuide := make(map[*regexp.Regexp][][24]byte)

	for k, v := range routingGuide {
		if len(k) > 24 {
			err := errors.New(fmt.Sprintf("Header %v is too long, max length is 24", k))
			wg.Done()
			return err
		}
		sinks := [][24]byte{}
		for _, val := range v {
			if len(val) > 24 {
				err := errors.New(fmt.Sprintf("Header %v is too long, max length is 24", val))
				wg.Done()
				return err
			}
			var cur [24]byte
			copy(cur[:], val)
			sinks = append(sinks, cur)
		}

		//compile the regex, and put all the strings into byte arrays
		regex, err := regexp.Compile(k)
		if err != nil {
			err := errors.New(fmt.Sprintf("%v is not a valid regex string", k))
			wg.Done()
			return err
		}

		workingGuide[regex] = sinks
	}

	go func() {

		for {
			select {
			case curEvent, ok := <-inChan:
				if ok {
					for k, v := range workingGuide {
						if k.Match(curEvent.MessageHeader[:]) {
							for i := range v {
								outChan <- common.Message{MessageHeader: v[i], MessageBody: curEvent.MessageBody}
							}
							break //break out of our for loop
						}
					}
				} else {
					log.Printf("Router: In Channel closed, exiting")
					wg.Done()
					return
				}
			}
		}
	}()
	return nil
}
