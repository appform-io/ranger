package rangerdns

import (
	"fmt"
)

type RangerServiceNodeResponse struct {
	Status       string        `json:"status"`
	Message      string        `json:"message"`
	ServiceNodes []ServiceNode `json:"data"`
}

type ServiceNode struct {
	Host       string `json:"host"`
	Port       int32  `json:"port"`
	PortScheme string `json:"portScheme"`
}

type EndpointInfo struct {
	Endpoint string
	Host     string
	Port     int32
}

type RangerConfig struct {
	Endpoint                       string
	SkipDataFromReplicationSources bool
}

func (dc RangerConfig) Validate() error {
	if dc.Endpoint == "" {
		return fmt.Errorf("endpoint can't be empty")
	}
	return nil
}

func NewRangerConfig() RangerConfig {
	return RangerConfig{}
}
