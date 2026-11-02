package device

import (
	"encoding/json"
	"fmt"
	"github.com/iobestar/sensus/internal/app/mqtt"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
	"reflect"
)

const messagesBucket = "messages"

type timestampSetter interface {
	setTimestamp(value int64)
}

type Message interface {
	GetTimestamp() int64
}

func createMessage(mqttMsg *mqtt.Message, msgType reflect.Type) (Message, error) {
	msg := reflect.New(msgType)
	if t, ok := (msg.Interface()).(timestampSetter); ok {
		t.setTimestamp(mqttMsg.Timestamp.UnixMilli())
	}

	if err := json.Unmarshal(mqttMsg.Payload, msg.Interface()); nil != err {
		return nil, err
	}

	if msg, ok := (msg.Interface()).(Message); ok {
		return msg, nil
	}
	return nil, errors.New(fmt.Sprintf("invalid message type: %s", msgType.String()))
}

type MessageStore interface {
	Store(string, Message) error
}

type messageStore struct {
	db *bolt.DB
}

func NewMessageStore(db *bolt.DB) (MessageStore, error) {
	err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte(messagesBucket)); nil != err {
			return err
		}
		return nil
	})
	if nil != err {
		return nil, err
	}
	return &messageStore{
		db: db,
	}, nil
}

func (s *messageStore) Store(deviceId string, msg Message) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		mb := tx.Bucket([]byte(messagesBucket))
		db, err := mb.CreateBucketIfNotExists([]byte(deviceId))
		if nil != err {
			return err
		}

		buf, err := json.Marshal(msg)
		if nil != err {
			return err
		}

		sid, _ := db.NextSequence()
		return db.Put([]byte(fmt.Sprintf("%d-%d", msg.GetTimestamp(), sid)), buf)
	})
}
