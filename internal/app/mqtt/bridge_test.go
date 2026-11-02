package mqtt

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func Test(t *testing.T) {
	config := Config{
		ClientId: "sensus_mqtt_client_A",
		Brokers:  []string{fmt.Sprintf("tcp://%s:%d", "192.168.8.117", 1883)},
	}

	wg := sync.WaitGroup{}

	bridge, err := NewBridge(config)
	require.Nil(t, err, "unexpected error")

	channel, err := bridge.Add("zigbee2mqtt/0x00124b002892a073")
	require.Nil(t, err, "unexpected error")

	wg.Add(1)
	go func() {
		defer wg.Done()
		for msg := range channel {
			fmt.Printf("%d: %s\n", msg.Id, string(msg.Payload))
		}
	}()

	err = bridge.Start()
	require.Nil(t, err, "unexpected error")

	<-time.After(5 * time.Minute)
	err = bridge.Stop()
	require.Nil(t, err, "unexpected error")

	wg.Wait()
}
