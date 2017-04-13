package subscriber

import (
	"errors"
	"fmt"
	"log"
	"net"

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
	queuedMessages      chan common.Message
	aggregatorStarted   bool
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

	if !s.aggregatorStarted {

	}

	subscription := readSubscription{
		address:    address,
		sub:        s,
		connection: conn,
		ReadQueue:  make(chan common.Message, s.individualQueueSize),
	}
	subscription.StartListener()

	s.subscriptions = append(s.subscriptions, subscription)
}

func (s *subscriber) startAggregator() {
	go func() {
		for {

		}
	}()
}

func (rs *readSubscription) StartListener() {
	go func() {
		for {
			buf := []byte{}
			temp := [8192]byte{}

			num, err := rs.connection.Read(temp)
			if err != nil {
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
