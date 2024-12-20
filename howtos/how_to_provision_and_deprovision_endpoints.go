package main

import (
	"fmt"
	"os"
	"time"

	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
)

func getEnv(key, def string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return def
}

// Code examples of how to use the Endpoint Provisioner to Provision
// queues on a Solace broker and Deprovision queues from a Solace broker.
//
// Possible Provision errors include:
// [x] Already Exists - when a queue with the same properties already exists on the broker
// [x] Endpoint Property Mismatch - when a queue with the same name but different provision properties already exists on the broker
//
// Possible Deprovision errors include:
// [x] Unknown Queue - when a queue with the provided queue name does not exists on the broker
func main() {
	// logging.SetLogLevel(logging.LogLevelInfo)

	// Configuration parameters
	brokerConfig := config.ServicePropertyMap{
		config.TransportLayerPropertyHost:                getEnv("SOLACE_HOST", "tcp://localhost:55555,tcp://localhost:55554"),
		config.ServicePropertyVPNName:                    getEnv("SOLACE_VPN", "default"),
		config.AuthenticationPropertySchemeBasicPassword: getEnv("SOLACE_PASSWORD", "default"),
		config.AuthenticationPropertySchemeBasicUserName: getEnv("SOLACE_USERNAME", "default"),
	}

	messagingService, err := messaging.NewMessagingServiceBuilder().
		FromConfigurationProvider(brokerConfig).
		WithProvisionTimeoutMs(10 * time.Millisecond). // set a provision timeout on the session
		Build()

	if err != nil {
		panic(err)
	}

	// Connect to the messaging serice
	if err := messagingService.Connect(); err != nil {
		panic(err)
	}

	fmt.Println("Connected to the broker? ", messagingService.IsConnected())

	/////////////////////////////////////////
	// PROVISIONING EXAMPLES
	/////////////////////////////////////////

	//// Start Provision with blocking Example
	queueName := "ProvisionedQueueName"
	var outcome solace.ProvisionOutcome = messagingService.EndpointProvisioner().
		WithDurability(true).                            // provision a durable queue (this is Default to True irresspective of whether this setter is called)
		WithMaxMessageRedelivery(10).                    //  number of times queue messages will be redelivered before moving to the DMQ
		WithDiscardNotification(true).                   // will notify senders about message discards
		WithTTLPolicy(true).                             // respect message TTL on queue
		WithQuotaMB(100).                                // set the queue message quota (in MB)
		WithMaxMessageSize(1000000).                     // set Max message size (in Bytes)
		WithExclusiveAccess(true).                       // provision an Exclusive queue
		WithPermission(config.EndpointPermissionDelete). // with the delete permission
		Provision(queueName, false)

	fmt.Println("\nEndpoint Provision on the broker [Status]: ", outcome.GetStatus())
	fmt.Println("Endpoint Provision on the broker [Error]: ", outcome.GetError(), "\n")
	//// End Provision with blocking Example

	// //// Start ProvisionAsync Example
	// queueName := "ProvisionedQueueName"
	// outcomeChannel := messagingService.EndpointProvisioner().
	// 	WithDurability(true).                            // provision a durable queue (this is Default to True irresspective of whether this setter is called)
	// 	WithMaxMessageRedelivery(10).                    //  number of times queue messages will be redelivered before moving to the DMQ
	// 	WithDiscardNotification(true).                   // will notify senders about message discards
	// 	WithTTLPolicy(true).                             // respect message TTL on queue
	// 	WithQuotaMB(100).                                // set the queue message quota (in MB)
	// 	WithMaxMessageSize(1000000).                     // set Max message size (in Bytes)
	// 	WithExclusiveAccess(true).                       // provision an Exclusive queue
	// 	WithPermission(config.EndpointPermissionDelete). // with the delete permission
	// 	ProvisionAsync(queueName, false)

	// outcome := <-outcomeChannel
	// fmt.Println("\nEndpoint Provision Aysnc on the broker [Status]: ", outcome.GetStatus())
	// fmt.Println("Endpoint Provision Aysnc on the broker [Error]: ", outcome.GetError(), "\n")
	// //// End ProvisionAsync Example

	// //// Start ProvisionAsync with callback Example
	// queueName := "ProvisionedQueueName"
	// provisionCallbackHandler := func(outcome solace.ProvisionOutcome) {
	// 	fmt.Println("\nEndpoint Provision Aysnc With Callback on the broker [Status]: ", outcome.GetStatus())
	// 	fmt.Println("Endpoint Provision Aysnc With Callback on the broker [Error]: ", outcome.GetError(), "\n")
	// }
	// messagingService.EndpointProvisioner().
	// 	WithDurability(true).                            // provision a durable queue (this is Default to True irresspective of whether this setter is called)
	// 	WithMaxMessageRedelivery(10).                    //  number of times queue messages will be redelivered before moving to the DMQ
	// 	WithDiscardNotification(true).                   // will notify senders about message discards
	// 	WithTTLPolicy(true).                             // respect message TTL on queue
	// 	WithQuotaMB(100).                                // set the queue message quota (in MB)
	// 	WithMaxMessageSize(1000000).                     // set Max message size (in Bytes)
	// 	WithExclusiveAccess(true).                       // provision an Exclusive queue
	// 	WithPermission(config.EndpointPermissionDelete). // with the delete permission
	// 	ProvisionAsyncWithCallback(queueName, false, provisionCallbackHandler)
	// //// End ProvisionAsync with callback Example

	/////////////////////////////////////////
	// DEPROVISIONING EXAMPLES
	/////////////////////////////////////////

	// //// Start Deprovision with blocking Example
	// queueName := "ProvisionedQueueName"
	// var deprovError error = messagingService.EndpointProvisioner().Deprovision(queueName, false)
	// fmt.Println("\nEndpoint Deprovision on the broker [Error]: ", deprovError)
	// //// End Deprovision with blocking

	// //// Start DeprovisionAsync
	// queueName := "ProvisionedQueueName"
	// errorChannel := messagingService.EndpointProvisioner().DeprovisionAsync(queueName, false)
	// deprovError := <-errorChannel
	// fmt.Println("\nEndpoint Deprovisioner Aysnc on the broker [Error]: ", deprovError)
	// //// End DeprovisionAsync Example

	// //// Start DeprovisionAsync with callback Example
	// queueName := "ProvisionedQueueName"
	// deprovisionCallbackHandler := func(deprovError error) {
	// 	fmt.Println("\nEndpoint Deprovisioner Aysnc With Callback on the broker [Error]: ", deprovError)
	// }
	// messagingService.EndpointProvisioner().DeprovisionAsyncWithCallback(queueName, false, deprovisionCallbackHandler)
	// //// End DeprovisionAsync with callback Example

	fmt.Println("\n===Interrupt (CTR+C) to handle graceful terminaltion of the messaiging service===")

	// Disconnect the Message Service
	messagingService.Disconnect()
	fmt.Println("Messaging Service Disconnected? ", !messagingService.IsConnected())
}
