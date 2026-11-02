package device

import (
	"embed"
	"encoding/json"
)

//go:embed devices.json
var devices embed.FS

type Protocol int

const (
	ProtocolUndefined Protocol = iota
	ProtocolZigbee
)

func (p Protocol) String() string {
	switch p {
	case ProtocolZigbee:
		return "zigbee"
	case ProtocolUndefined:
		return "undefined"
	}
	return "undefined"
}

type Model int

const (
	ModelUndefined Model = iota
	ModelSNZB01
	ModelSNZB02
	ModelSNZB03
	ModelSNZB04
)

func (t Model) String() string {
	switch t {
	case ModelSNZB01:
		return "snzb01"
	case ModelSNZB02:
		return "snzb02"
	case ModelSNZB03:
		return "snzb03"
	case ModelSNZB04:
		return "snzb04"
	case ModelUndefined:
		return "undefined"
	}
	return "undefined"
}

type Device struct {
	Id          string   `json:"id"`
	Name        string   `json:"name"`
	Model       Model    `json:"model"`
	Protocol    Protocol `json:"protocol"`
	Description string   `json:"description"`
}

type Sensor interface {
	Messages() (chan interface{}, error)
	GetMessage() interface{}
}

func Load() ([]Device, error) {
	data, err := devices.ReadFile("devices.json")
	if nil != err {
		return nil, err
	}

	var result []Device
	if err := json.Unmarshal(data, &result); nil != err {
		return nil, err
	}
	return result, nil
}
