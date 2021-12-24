package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"
)

// Message Handler
func MessageHandler(message message.InboundMessage) {
	fmt.Printf("Message Dump %s \n", message)
}

func ReconnectionHandler(e solace.ServiceEvent) {
	e.GetTimestamp()
	e.GetBrokerURI()
	err := e.GetCause()
	if err != nil {
		fmt.Println(err)
	}
}

func getEnv(key, def string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return def
}

func main() {

	// Define Topic Subscriptions
	TOPIC_PREFIX := "solace/samples/go"

	// Configuration parameters
	brokerConfig := config.ServicePropertyMap{
		config.TransportLayerPropertyHost:                getEnv("SOLACE_HOST", "tcp://localhost:55554"),
		config.ServicePropertyVPNName:                    getEnv("SOLACE_VPN", "default"),
		config.AuthenticationPropertySchemeBasicPassword: getEnv("SOLACE_PASSWORD", "default"),
		config.AuthenticationPropertySchemeBasicUserName: getEnv("SOLACE_USERNAME", "default"),
	}

	// Note: The reconnections strategy could also be configured using the broker properties object
	messagingService, err := messaging.NewMessagingServiceBuilder().FromConfigurationProvider(brokerConfig).Build()

	if err != nil {
		panic(err)
	}

	messagingService.AddReconnectionListener(ReconnectionHandler)

	// Connect to the messaging serice
	if err := messagingService.Connect(); err != nil {
		panic(err)
	}

	fmt.Println("Connected to the broker? ", messagingService.IsConnected())

	queueName := "sample-queue"
	durable_exclusive_queue = *resource.QueueDurableExclusive(queueName)

	// Build a Direct message receivers with given topics
	persistentReceiver, err := messagingService.CreatePersistentMessageReceiverBuilder(durable_exclusive_queue).Build()

	if err != nil {
		panic(err)
	}

	// Start Direct Message Receiver
	if err := persistentReceiver.Start(); err != nil {
		panic(err)
	}

	fmt.Println("Direct Receiver running? ", persistentReceiver.IsRunning())

	// Register Message callback handler to the Message Receiver
	if regErr := persistentReceiver.ReceiveAsync(MessageHandler); regErr != nil {
		panic(regErr)
	}

	fmt.Println("\n===Interrupt (CTR+C) to handle graceful terminaltion of the subscriber===\n")

	// Run forever until an interrupt signal is received
	// Handle interrupts

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	// Block until a signal is received.
	<-c

	// Terminate the Direct Receiver
	persistentReceiver.Terminate(1 * time.Second)
	fmt.Println("\nDirect Receiver Terminated? ", persistentReceiver.IsTerminated())
	// Disconnect the Message Service
	messagingService.Disconnect()
	fmt.Println("Messaging Service Disconnected? ", !messagingService.IsConnected())

}
