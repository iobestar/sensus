package mqtt

import (
	"context"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func Test_Subscribe(t *testing.T) {
	broker := fmt.Sprintf("tcp://%s:%d", "192.168.8.117", 1883)

	onConnect := func(client mqtt.Client) {
		fmt.Println("client connect")
	}

	onConnectionLost := func(client mqtt.Client, err error) {
		fmt.Println("client connection lost")
	}

	onReconnect := func(client mqtt.Client, options *mqtt.ClientOptions) {
		fmt.Println("client reconnect")
	}

	client, err := NewClient("sensus", []string{broker},
		WithConnectCallback(onConnect),
		WithConnectionLostCallback(onConnectionLost),
		WithReconnectCallback(onReconnect),
	)
	require.Nil(t, err, "unexpected error")

	err = client.Connect(context.Background())
	require.Nil(t, err, "unexpected error")

	err = client.Subscribe(context.Background(), "zigbee2mqtt/0x00124b0025485d3a", 1, func(client mqtt.Client, msg mqtt.Message) {
		fmt.Printf("topic=%s payload=%s\n", msg.Topic(), string(msg.Payload()))
		msg.Ack()
	})
	require.Nil(t, err, "unexpected error")

	<-time.After(5 * time.Minute)

	client.Disconnect(5 * time.Second)
	fmt.Printf("connected=%v connection=%v\n", client.client.IsConnected(), client.client.IsConnectionOpen())
	fmt.Println("DONE")
}

func Test_Lock(t *testing.T) {
	l := sync.Mutex{}
	l.Lock()
	l.Unlock()
	a := func() {
		l.Lock()
		defer l.Unlock()
		fmt.Println("A")
	}

	a()
}

func foobar(l sync.Mutex) {
	l.Lock()
	defer l.Unlock()
	fmt.Println("foobar")
}
