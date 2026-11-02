package device

import (
	"context"
	"fmt"
	"github.com/iobestar/sensus/internal"
	"github.com/iobestar/sensus/internal/app/mqtt"
	log "github.com/sirupsen/logrus"
	"reflect"
	"sync"
	"sync/atomic"
)

type baseSnzb struct {
	device Device

	bridge *mqtt.Bridge

	createMessageFunc func(msg *mqtt.Message) (Message, error)

	msgLast  atomic.Value
	msgStore MessageStore
	msgType  reflect.Type

	attachMux sync.RWMutex
	attachChs []chan interface{}
}

func (s *baseSnzb) Device() Device {
	return s.device
}

func (s *baseSnzb) Messages() (chan interface{}, error) {
	s.attachMux.Lock()
	defer s.attachMux.Unlock()
	attachCh := make(chan interface{}, 1)

	if lastMsg := s.msgLast.Load(); nil != lastMsg {
		attachCh <- s.msgLast.Load()
	}
	s.attachChs = append(s.attachChs, attachCh)

	go func() {
		for msg := range attachCh {
			attachCh <- msg
		}
	}()
	return attachCh, nil
}

func (s *baseSnzb) GetMessage() interface{} {
	return s.msgLast.Load()
}

func (s *baseSnzb) Run(_ context.Context, wg *sync.WaitGroup, _ *internal.Disposer) error {
	id := s.device.Id

	msgCh, err := s.bridge.Subscribe(fmt.Sprintf("zigbee2mqtt/%s", id))
	if nil != err {
		return err
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for m := range msgCh {
			if msg, err := s.createMessageFunc(m); nil != err {
				log.WithError(err).Errorf("unable to create message: %s", id)
			} else {
				s.msgLast.Store(msg)
				if err := s.msgStore.Store(id, msg); nil != err {
					log.WithError(err).Errorf("unable to store message: %s", id)
				}

				s.attachMux.RLock()
				for _, ch := range s.attachChs {
					select {
					case ch <- msg:
					default:
					}
				}
				s.attachMux.RUnlock()
			}
		}

		s.attachMux.Lock()
		defer s.attachMux.Unlock()
		for _, ch := range s.attachChs {
			close(ch)
		}

	}()
	return nil
}

type snzb01 struct {
	baseSnzb
}

type MessageSNZB01 struct {
	Timestamp   int64   `json:"timestamp"`
	Action      string  `json:"action"`
	Battery     float32 `json:"battery"`
	Voltage     int     `json:"voltage"`
	LinkQuality int     `json:"linkquality"`
}

func (m *MessageSNZB01) setTimestamp(value int64) {
	m.Timestamp = value
}

func (m *MessageSNZB01) GetTimestamp() int64 {
	return m.Timestamp
}

func newSNZB01(id, name string, msgStore MessageStore, bridge *mqtt.Bridge) (*snzb01, error) {
	return &snzb01{
		baseSnzb{
			device: Device{
				Id:       id,
				Name:     name,
				Model:    ModelSNZB01,
			},

			bridge: bridge,

			createMessageFunc: func(msg *mqtt.Message) (Message, error) {
				return createMessage(msg, reflect.TypeOf(MessageSNZB01{}))
			},

			msgLast:  atomic.Value{},
			msgStore: msgStore,
			msgType:  reflect.TypeOf(MessageSNZB01{}),

			attachMux: sync.RWMutex{},
			attachChs: make([]chan interface{}, 0),
		},
	}, nil
}

type snzb02 struct {
	baseSnzb
}

type MessageSNZB02 struct {
	Timestamp   int64   `json:"timestamp"`
	Temperature float32 `json:"temperature"`
	Humidity    float32 `json:"humidity"`
	Battery     float32 `json:"battery"`
	Voltage     int     `json:"voltage"`
	LinkQuality int     `json:"linkquality"`
}

func (m *MessageSNZB02) setTimestamp(value int64) {
	m.Timestamp = value
}

func (m *MessageSNZB02) GetTimestamp() int64 {
	return m.Timestamp
}

func newSNZB02(id, name string, msgStore MessageStore, bridge *mqtt.Bridge) (*snzb02, error) {
	return &snzb02{
		baseSnzb{
			device: Device{
				Id:       id,
				Name:     name,
				Model:    ModelSNZB02,
			},

			bridge: bridge,

			createMessageFunc: func(msg *mqtt.Message) (Message, error) {
				return createMessage(msg, reflect.TypeOf(MessageSNZB02{}))
			},

			msgLast:  atomic.Value{},
			msgStore: msgStore,
			msgType:  reflect.TypeOf(MessageSNZB02{}),

			attachMux: sync.RWMutex{},
			attachChs: make([]chan interface{}, 0),
		},
	}, nil
}

type snzb03 struct {
	baseSnzb
}

type MessageSNZB03 struct {
	Timestamp   int64   `json:"timestamp"`
	Occupancy   bool    `json:"occupancy"`
	Tamper      bool    `json:"tamper"`
	BatteryLow  bool    `json:"battery_low"`
	Battery     float32 `json:"battery"`
	Voltage     int     `json:"voltage"`
	LinkQuality int     `json:"linkquality"`
}

func (m *MessageSNZB03) setTimestamp(value int64) {
	m.Timestamp = value
}

func (m *MessageSNZB03) GetTimestamp() int64 {
	return m.Timestamp
}

func newSNZB03(id, name string, msgStore MessageStore, bridge *mqtt.Bridge) (*snzb03, error) {
	return &snzb03{
		baseSnzb{
			device: Device{
				Id:       id,
				Name:     name,
				Model:    ModelSNZB03,
			},

			bridge: bridge,

			createMessageFunc: func(msg *mqtt.Message) (Message, error) {
				return createMessage(msg, reflect.TypeOf(MessageSNZB03{}))
			},

			msgLast:  atomic.Value{},
			msgStore: msgStore,
			msgType:  reflect.TypeOf(MessageSNZB03{}),

			attachMux: sync.RWMutex{},
			attachChs: make([]chan interface{}, 0),
		},
	}, nil
}

type snzb04 struct {
	baseSnzb
}

type MessageSNZB04 struct {
	Timestamp   int64   `json:"timestamp"`
	Contact     bool    `json:"contact"`
	BatteryLow  bool    `json:"battery_low"`
	Battery     float32 `json:"battery"`
	Voltage     int     `json:"voltage"`
	LinkQuality int     `json:"linkquality"`
}

func (m *MessageSNZB04) setTimestamp(value int64) {
	m.Timestamp = value
}

func (m *MessageSNZB04) GetTimestamp() int64 {
	return m.Timestamp
}

func newSNZB04(id, name string, msgStore MessageStore, bridge *mqtt.Bridge) (*snzb04, error) {
	return &snzb04{
		baseSnzb{
			device: Device{
				Id:       id,
				Name:     name,
				Model:    ModelSNZB04,
			},

			bridge: bridge,

			createMessageFunc: func(msg *mqtt.Message) (Message, error) {
				return createMessage(msg, reflect.TypeOf(MessageSNZB04{}))
			},

			msgLast:  atomic.Value{},
			msgStore: msgStore,
			msgType:  reflect.TypeOf(MessageSNZB04{}),

			attachMux: sync.RWMutex{},
			attachChs: make([]chan interface{}, 0),
		},
	}, nil
}
