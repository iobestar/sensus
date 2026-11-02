package mqtt

import (
	"github.com/google/uuid"
	"github.com/spf13/viper"
)

func init() {
	viper.SetDefault("app.mqtt.client_id", "sensus_"+uuid.New().String()[:8])
	viper.SetDefault("app.mqtt.brokers", []string{"tcp://localhost:1883"})
}

type Config struct {
	ClientId string
	Brokers  []string
}

func NewConfig() Config {
	return Config{
		ClientId: viper.GetString("app.mqtt.client_id"),
		Brokers:  viper.GetStringSlice("app.mqtt.brokers"),
	}
}
