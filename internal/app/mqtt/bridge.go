package mqtt

import (
	"context"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"sync"
	"time"
)

type BridgeOption func(bridge *Bridge) error

type Options struct {
	ClientId string
	Servers  []string
}

type Message struct {
	Id        uint16
	Timestamp time.Time
	Duplicate bool
	Retained  bool
	Payload   []byte
}

type Bridge struct {
	client *Client

	topics    map[string]chan *Message
	topicsMux *sync.RWMutex

	wg      sync.WaitGroup
	closeCh chan struct{}
}

func NewBridge(config Config) (*Bridge, error) {
	topics := make(map[string]chan *Message)
	topicsMux := &sync.RWMutex{}

	onConnect := func(client mqtt.Client) {
		topicsMux.RLock()
		defer topicsMux.RUnlock()

		for topic, channel := range topics {
			_ = subscribeToChannel(topic, client, channel)
		}
	}

	options := []ClientOption{
		WithConnectCallback(onConnect),
	}

	client, err := NewClient(config.ClientId, config.Brokers, options...)
	if nil != err {
		return nil, err
	}

	return &Bridge{
		client:    client,
		topics:    topics,
		topicsMux: topicsMux,

		wg:      sync.WaitGroup{},
		closeCh: make(chan struct{}),
	}, nil
}

func (b *Bridge) Add(topic string) (<-chan *Message, error) {
	b.topicsMux.Lock()
	defer b.topicsMux.Unlock()

	if value, ok := b.topics[topic]; !ok {
		channel := subscribeToChannel(topic, b.client.Unwrap(), make(chan *Message, 32))
		b.topics[topic] = channel
		return channel, nil
	} else {
		return value, nil
	}
}

func (b *Bridge) Start() error {
	return b.client.Connect(context.Background())
}

func (b *Bridge) Stop() error {
	close(b.closeCh)
	b.wg.Wait()

	b.client.Disconnect(5 * time.Second)

	b.topicsMux.Lock()
	defer b.topicsMux.Unlock()

	for _, channel := range b.topics {
		close(channel)
	}
	return nil
}

func subscribeToChannel(topic string, client mqtt.Client, channel chan *Message) chan *Message {
	client.Subscribe(topic, 1, func(client mqtt.Client, msg mqtt.Message) {
		channel <- &Message{
			Id:        msg.MessageID(),
			Timestamp: time.Now(),
			Duplicate: msg.Duplicate(),
			Retained:  msg.Retained(),
			Payload:   msg.Payload(),
		}
		msg.Ack()
	})
	return channel
}
