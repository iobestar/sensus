package mqtt

import (
	"context"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	log "github.com/sirupsen/logrus"
	"time"
)

func init() {
	//mqtt.DEBUG = log.StandardLogger()
}

type ClientOption func(options *mqtt.ClientOptions) error

func WithConnectCallback(callback func(mqtt.Client)) ClientOption {
	return func(options *mqtt.ClientOptions) error {
		options.SetOnConnectHandler(callback)
		return nil
	}
}

func WithConnectionLostCallback(callback func(mqtt.Client, error)) ClientOption {
	return func(options *mqtt.ClientOptions) error {
		options.SetConnectionLostHandler(callback)
		return nil
	}
}

func WithReconnectCallback(callback func(mqtt.Client, *mqtt.ClientOptions)) ClientOption {
	return func(options *mqtt.ClientOptions) error {
		options.SetReconnectingHandler(callback)
		return nil
	}
}

func WithConnectRetry(value bool) ClientOption {
	return func(options *mqtt.ClientOptions) error {
		options.SetConnectRetry(value)
		return nil
	}
}

func WithConnectRetryInterval(value time.Duration) ClientOption {
	return func(options *mqtt.ClientOptions) error {
		options.SetConnectRetryInterval(value)
		return nil
	}
}

func WithOrderMatters(value bool) ClientOption {
	return func(options *mqtt.ClientOptions) error {
		options.SetOrderMatters(value)
		return nil
	}
}

func WithKeepAlive(value time.Duration) ClientOption {
	return func(options *mqtt.ClientOptions) error {
		options.SetKeepAlive(value)
		return nil
	}
}

type Client struct {
	client mqtt.Client
}

func NewClient(clientId string, brokers []string, opts ...ClientOption) (*Client, error) {
	clientOpts := mqtt.NewClientOptions()

	clientOpts.SetClientID(clientId)

	for _, broker := range brokers {
		clientOpts.AddBroker(broker)
	}

	clientOpts.SetConnectionLostHandler(func(client mqtt.Client, err error) {
		log.WithError(err).Error("client connection lost")
	})

	for _, opt := range opts {
		if err := opt(clientOpts); nil != err {
			return nil, err
		}
	}

	return &Client{
		client: mqtt.NewClient(clientOpts),
	}, nil
}

func (c *Client) Connect(ctx context.Context) error {
	token := c.client.Connect()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-token.Done():
		if nil != token.Error() {
			return token.Error()
		}
	}
	return nil
}

func (c *Client) Disconnect(timeout time.Duration) {
	c.client.Disconnect(uint(timeout.Milliseconds()))
}

func (c *Client) Subscribe(ctx context.Context, topic string, qos byte, callback mqtt.MessageHandler) error {
	token := c.client.Subscribe(topic, qos, callback)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-token.Done():
		if nil != token.Error() {
			return token.Error()
		}
	}
	return nil
}

func (c *Client) Unsubscribe(ctx context.Context, topic string) error {
	token := c.client.Unsubscribe(topic)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-token.Done():
		if nil != token.Error() {
			return token.Error()
		}
	}
	return nil
}

func (c *Client) Publish(ctx context.Context, topic string, qos byte, retained bool, payload interface{}) error {
	token := c.client.Publish(topic, qos, retained, payload)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-token.Done():
		if nil != token.Error() {
			return token.Error()
		}
	}
	return nil
}

func (c *Client) Unwrap() mqtt.Client {
	return c.client
}
