package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"
)

// Message Handler
func MessageHandler(message message.InboundMessage) {
	payload, ok := message.GetPayloadAsString()
	if !ok {
		fmt.Println("Message is NOT String")
	} else {
		fmt.Printf("payload: %v\n", payload)
	}
}

func main() {

	// Define Topic Subscriptions
	TOPIC_PREFIX := "solace/samples/go"

	// Build A messaging service with a reconnection strategy of 20 retries over an interval of 3 seconds
	// Note: The reconnections strategy could also be configured using the broker properties object
	messagingService, err := messaging.NewMessagingServiceBuilder().FromConfigurationProvider(config.ServicePropertyMap{
		config.TransportLayerPropertyHost:                "tcp://localhost:55554",
		config.ServicePropertyVPNName:                    "default",
		config.AuthenticationPropertySchemeBasicPassword: "default",
		config.AuthenticationPropertySchemeBasicUserName: "default",
	}).Build()

	if err != nil {
		panic(err)
	}

	// Connect to the messaging serice
	if err := messagingService.Connect(); err != nil {
		panic(err)
	}

	fmt.Println("Connected to the broker? ", messagingService.IsConnected())

	// Build a Direct message receivers with given topics
	directReciever, err := messagingService.CreateDirectMessageReceiverBuilder().
		WithSubscriptions(resource.TopicSubscriptionOf(TOPIC_PREFIX + "/direct/sub/>")).
		Build()

	if err != nil {
		panic(err)
	}

	// Start Direct Message Recieer
	if err := directReciever.Start(); err != nil {
		panic(err)
	}

	fmt.Println("Direct Reciever running? ", directReciever.IsRunning())

	// Register Message callback handler to the Message Receiver
	if regErr := directReciever.ReceiveAsync(MessageHandler); regErr != nil {
		panic(regErr)
	}

	fmt.Println("\nInterrupt (CTR+C) to handle graceful terminaltion of the subscriber")
	// Run forever until an interrupt signal is received
	go func() {
		for {
			time.Sleep(1)
		}
	}()

	// Handle interrupts

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	// Block until a signal is received.
	s := <-c
	_ = s

	directReciever.Terminate(1)
	fmt.Println("\nDirect Reciever Terminated? ", directReciever.IsTerminated())
	messagingService.Disconnect()
	fmt.Println("Mesagging Service Disconnected? ", !messagingService.IsConnected())

}
