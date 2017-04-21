package subscriber

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/xuther/go-message-router/common"
)

type Subscriber interface {
	Subscribe(address string, filter []string) error
	Read() common.Message
	Close()
}

type subscriber struct {
	aggregateQueueSize int
	subscriptions      []*readSubscription
	QueuedMessages     chan common.Message
	subscribeChan      chan *readSubscription
	unsubscribeChan    chan *readSubscription
}

type readSubscription struct {
	Address    string
	Sub        *subscriber
	Connection *net.TCPConn
}

func NewSubscriber(aggregateQueueSize int) (Subscriber, error) {

	toReturn := subscriber{
		aggregateQueueSize: aggregateQueueSize,
		QueuedMessages:     make(chan common.Message, aggregateQueueSize),
		subscribeChan:      make(chan *readSubscription, 10),
		unsubscribeChan:    make(chan *readSubscription, 10),
	}
	toReturn.startSubscriptionManager()

	return &toReturn, nil
}

func (s *subscriber) Close() {
}

func (s *subscriber) Read() common.Message {
	return <-s.QueuedMessages

}

func (s *subscriber) Subscribe(address string, filters []string) error {

	addr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		err = errors.New(fmt.Sprintf("Error resolving the TCP addr %s: %s", address, err.Error()))
		log.Printf(err.Error())
		return err
	}

	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		log.Printf("Error establishing a connection with %s: %s", address, err.Error())
		return err
	}

	log.Printf("Connection sent")

	subscription := readSubscription{
		Address:    address,
		Sub:        s,
		Connection: conn,
	}

	log.Printf("Subscription created, adding to manager")

	s.subscribeChan <- &subscription
	return nil
}

func (s *subscriber) startSubscriptionManager() {
	log.Printf("Starting subscription Manager.")
	go func() {
		for {
			select {
			case sub := <-s.subscribeChan:
				log.Printf("Subscribed.")
				s.subscriptions = append(s.subscriptions, sub)
				sub.StartListener()

			case unsub := <-s.unsubscribeChan:
				for k, i := range s.subscriptions {
					if i == unsub { //remove from our list of subscribers
						s.subscriptions[k] = s.subscriptions[len(s.subscriptions)-1]
						s.subscriptions = s.subscriptions[:len(s.subscriptions)-1]
					}
				}
			}
		}
	}()
}

func (rs *readSubscription) StartListener() {
	go func() {
		log.Printf("Starting listener")
		for {
			buf := []byte{}
			temp := make([]byte, 8192)

			num, err := rs.Connection.Read(temp)
			log.Printf("Message Recieved: %s", temp)
			if err != nil {
				if err == io.EOF {
					log.Printf("The connection for %s was closed", rs.Address)
					rs.Sub.unsubscribeChan <- rs
					return
				}

				log.Printf("There was a problem reading from the connection to %s", rs.Address)
				continue
			}
			buf = append(buf, temp[:num]...)

			//finish reading the message
			for num%8192 == 0 && num != 0 {
				num, err := rs.Connection.Read(temp)
				if err != nil {
					log.Printf("There was a problem reading from the connection to %s", rs.Address)
					continue
				}
				buf = append(buf, temp[:num]...)
			}
			header := [24]byte{}
			copy(header[:], buf[:24])
			rs.Sub.QueuedMessages <- common.Message{MessageHeader: header, MessageBody: buf[24:]}
		}
	}()
}
