package app

import "github.com/iobestar/sensus/internal/app/mqtt"

func init() {
	// add init here
}

type Config struct {
	Mqtt mqtt.Config
}

func NewConfig() Config {
	return Config{
		Mqtt: mqtt.NewConfig(),
	}
}
