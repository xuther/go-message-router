package subscriber

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"reflect"

	"github.com/xuther/go-message-router/common"
)

type Subscriber interface {
	Subscribe(address string, filter []string) error
	Read() common.Message
	Close()
}

type susbcriber struct {
	aggregateQueueSize  int
	individualQueueSize int
	subscriptions       []*readSubscription
	susbcriptionSelect  []reflect.SelectCase
	queuedMessages      chan common.Message
	subscribeChan       chan *readSubscription
	unsubscrbeChan      chan *readSubscription
}

type readSubscription struct {
	address    string
	sub        *subscriber
	connection *net.TCPConn
	ReadQueue  chan common.Message
	filters    []string
}

func NewSubscriber(address string, aggregateQueueSize int, individualQueueSize int) (*Subscriber, error) {

	return &Subscriber{
		address:             address,
		aggregateQueueSize:  aggregateQueueSize,
		individualQueueSize: individualQueueSize,
		queuedMessage:       make(chan common.Message, aggregateQueueSize),
	}, nil
}

func (s *subscriber) Subscribe(address string, filters []string) error {

	addr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		err = errors.New(fmt.Sprintf("Error resolving the TCP addr %s: %s", p.port, err.Error()))
		log.Printf(err.Error())
		return err
	}

	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		log.Printf("Error establishing a connection with %s: %s", address, err.Error())
		return err
	}

	s.startAggregationManager()

	subscription := readSubscription{
		address:    address,
		sub:        s,
		connection: conn,
		ReadQueue:  make(chan common.Message, s.individualQueueSize),
	}

	s.subscribeChan <- subscription
}

func (s *subscriber) startAggregator() {
	go func() {
		for {
			chosen, value, ok := reflect.Select(s.subscriptionSelect)
			if !ok {
				s.subscriptionSelect[chosen] = s.subscriptionSelect[len(s.subscriptionSelect)-1]
				s.subscriptionSelect[len(s.subscriptionSelect)-1] = nil
				s.subscriptionSelect = s.subscriptionSelect[:len(s.subscriptionSelect)-1]
			}

			//New Subscription to listen to
			if chosen == 1 {

			}

		}
	}()
}

func (s *subscriber) startAggregationManager() {
	go func() {
		firstSubscription := <-s.subscribeChan
		subscription.StartListener()
		s.subscriptions = append(s.subscriptions, subscription)
		s.startAggregator()

		//Add the first two to our select statments, our subscribe and unsubscribe channel.
		s.subscriptionSelect = append(s.SubscriptionSelect,
			reflect.SelectCase{Dir: reflect.SelectRecv, Chan: subscribeChan},
			reflect.SelectCase{Dir: reflect.SelectRecv, Chan: unsubscribeChan})
		startAggregator()

	}()
}

func (rs *readSubscription) StartListener() {
	go func() {
		for {
			buf := []byte{}
			temp := [8192]byte{}

			num, err := rs.connection.Read(temp)
			if err != nil {
				if err == io.EOF {
					log.Printf("The connection for %s was closed", rs.connection.RemoteAddr())
					rs.sub.UnsubscribeChan <- rs
					return

				}

				log.Printf("There was a problem reading from the connection to %s", rs.address)
				continue
			}
			buf = append(buf, temp[:num])

			//finish reading the message
			for num%8192 == 0 && num != 0 {
				num, err := rs.connection.Read(temp)
				if err != nil {
					log.Printf("There was a problem reading from the connection to %s", rs.address)
					continue
				}
				buf = append(buf, temp[:num])
			}
			rs.ReadQueue <- common.Message{MessageHeader: buf[:24], MessageBody: buf[24:]}
		}
	}()
}
