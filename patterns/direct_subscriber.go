package main

import (
	"time"

	"github.com/SolaceDev/pubsubplus-go-client"
	"github.com/SolaceDev/pubsubplus-go-client/pkg/solace/config"
)


func main() {
	messagingService, err := pubsubplus.NewMessagingServiceBuilder().FromConfigurationProvider(config.ServicePropertyMap{
		config.TransportLayerPropertyHost: "tcp://localhost:55555",
		config.ServicePropertyVPNName:  "default",
		config.AuthenticationPropertySchemeBasicPassword: "default",
		config.AuthenticationPropertySchemeBasicUserName: "default",
	}).Build()

	if err != nil {
		panic(err)
	}

	if err := messagingService.Connect(); err != nil {
		panic(err)
	}

	time.Sleep(60* time.Minute)
}