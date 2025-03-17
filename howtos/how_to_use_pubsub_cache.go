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

const (
	ValidCachedMessageAge   int32 = 0
	ValidMaxCachedMessages  int32 = 0
	ValidCacheAccessTimeout int32 = 5000
)

func getEnv(key, def string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return def
}

// HowToGetCacheRequestConfiguration - example of how to retrieve the cache request
// based on the different supported strategies
func HowToGetCacheRequestConfiguration(cacheRequestStrategy resource.CachedMessageSubscriptionStrategy) resource.CachedMessageSubscriptionRequest {
	// specific the topic name
	topic := "MaxMsgs3/default/notcached"
	// specific the cache name
	cacheName := "CacheMessages"
	var cacheRequestConfig resource.CachedMessageSubscriptionRequest

	switch cacheRequestStrategy {
	// For cache request with strategy AsAvailable
	case resource.CacheRequestStrategyAsAvailable:
		cacheRequestConfig = resource.NewCachedMessageSubscriptionRequest(
			resource.CacheRequestStrategyAsAvailable,
			cacheName,
			resource.TopicSubscriptionOf(topic),
			ValidCacheAccessTimeout, // specific cache access timeout
			ValidMaxCachedMessages,  // specific max cached messages
			ValidCachedMessageAge,   // specific max cached message age
		)
		// For cache request with strategy CachedFirst
	case resource.CacheRequestStrategyCachedFirst:
		cacheRequestConfig = resource.NewCachedMessageSubscriptionRequest(
			resource.CacheRequestStrategyCachedFirst,
			cacheName,
			resource.TopicSubscriptionOf(topic),
			ValidCacheAccessTimeout, // specific cache access timeout
			ValidMaxCachedMessages,  // specific max cached messages
			ValidCachedMessageAge,   // specific max cached message age
		)
		// For cache request with strategy CachedOnly
	case resource.CacheRequestStrategyCachedOnly:
		cacheRequestConfig = resource.NewCachedMessageSubscriptionRequest(
			resource.CacheRequestStrategyCachedOnly,
			cacheName,
			resource.TopicSubscriptionOf(topic),
			ValidCacheAccessTimeout, // specific cache access timeout
			ValidMaxCachedMessages,  // specific max cached messages
			ValidCachedMessageAge,   // specific max cached message age
		)
		// For cache request with strategy LiveCancelsCached
	case resource.CacheRequestStrategyLiveCancelsCached:
		cacheRequestConfig = resource.NewCachedMessageSubscriptionRequest(
			resource.CacheRequestStrategyLiveCancelsCached,
			cacheName,
			resource.TopicSubscriptionOf(topic),
			ValidCacheAccessTimeout, // specific cache access timeout
			ValidMaxCachedMessages,  // specific max cached messages
			ValidCachedMessageAge,   // specific max cached message age
		)
	}

	// Dump the cache request
	fmt.Println(fmt.Sprintf("\nConstructed Cache Request Strategy: %s \n", cacheRequestConfig))
	return cacheRequestConfig
}

// Message Handler
func CacheResponseCallback(cacheResponse solace.CacheResponse) {
	// do something with the cache response
	// ...
	// ...

	// dump the cache response here
	// How to check the cache request ID of a message
	fmt.Printf("Received Cache Response; CacheRequestID %s\n", cacheResponse.GetCacheRequestID())

	// How to check the CacheRequestOutcome of a cache response
	fmt.Printf("Received Cache Response; CacheRequestOutcome %s\n", cacheResponse.GetCacheRequestOutcome())

	// How to check the error of a cache response
	fmt.Printf("Received Cache Response; Error %s\n", cacheResponse.GetError())
}

// HowToSendCacheRequestWithCallback - example of send the cache request and retrieve the cache response using a callback
func HowToSendCacheRequestWithCallback(directReceiver solace.DirectMessageReceiver, cacheRequestConfig resource.CachedMessageSubscriptionRequest) {
	// the cache request ID
	cacheRequestID := message.CacheRequestID(1)

	// call the method on the direct receiver to send the cache request and retrieve the cache response using a callback
	err := directReceiver.RequestCachedAsyncWithCallback(cacheRequestConfig, cacheRequestID, CacheResponseCallback)

	// we do not expect an error
	if err != nil {
		panic(err)
	}

	// ...
}

// HowToSendCacheRequestWithChannel - example of how to send the cache request and retrieve the cache response on a channel
func HowToSendCacheRequestWithChannel(directReceiver solace.DirectMessageReceiver, cacheRequestConfig resource.CachedMessageSubscriptionRequest) {
	// the cache request ID
	cacheRequestID := message.CacheRequestID(1)

	// call the method on the direct receiver to send the cache request and retrieve the cache response on a channel
	cacheResponseChannel, err := directReceiver.RequestCachedAsync(cacheRequestConfig, cacheRequestID)

	// we do not expect an error
	if err != nil {
		panic(err)
	}

	select {
	// this will hold the cache response that was received on the channel
	case cacheResponse := <-cacheResponseChannel:
		// do something with the cache response
		// ...
		// ...

		// dump the cache response here
		// How to check the cache request ID of a message
		fmt.Printf("Received Cache Response; CacheRequestID %s\n", cacheResponse.GetCacheRequestID())

		// How to check the CacheRequestOutcome of a cache response
		fmt.Printf("Received Cache Response; CacheRequestOutcome %s\n", cacheResponse.GetCacheRequestOutcome())

		// How to check the error of a cache response
		fmt.Printf("Received Cache Response; Error %s\n", cacheResponse.GetError())

	case <-time.After(1 * time.Second):
		fmt.Printf("timed out waiting for cache response to be recieved")
	}

	// ...
}

// Code examples of how to use the Go PubSub+ cache.
func main() {
	// logging.SetLogLevel(logging.LogLevelInfo)

	// Configuration parameters
	brokerConfig := config.ServicePropertyMap{
		config.TransportLayerPropertyHost:                 getEnv("SOLACE_HOST", "tcp://localhost:55555,tcp://localhost:55554"),
		config.ServicePropertyVPNName:                     getEnv("SOLACE_VPN", "default"),
		config.AuthenticationPropertySchemeBasicPassword:  getEnv("SOLACE_PASSWORD", "default"),
		config.AuthenticationPropertySchemeBasicUserName:  getEnv("SOLACE_USERNAME", "default"),
		config.TransportLayerPropertyReconnectionAttempts: 0,
	}

	messagingService, err := messaging.NewMessagingServiceBuilder().
		FromConfigurationProvider(brokerConfig).
		Build()

	if err != nil {
		panic(err)
	}

	// Connect to the messaging serice
	if err := messagingService.Connect(); err != nil {
		panic(err)
	}

	fmt.Println("Connected to the broker? ", messagingService.IsConnected())

	// Build a Direct message receivers with given topics
	directReceiver, err := messagingService.CreateDirectMessageReceiverBuilder().
		// we are using an abitary value for back pressure (you can configure this based on your use case)
		OnBackPressureDropOldest(100100).
		Build()

	if err != nil {
		panic(err)
	}

	// Start Direct Message Receiver
	if err := directReceiver.Start(); err != nil {
		panic(err)
	}

	fmt.Println("Direct Receiver running? ", directReceiver.IsRunning())

	/////////////////////////////////////////
	// Cache EXAMPLES
	/////////////////////////////////////////

	// For cache request with strategy AsAvailable
	var cachedMessageSubscriptionRequest resource.CachedMessageSubscriptionRequest = HowToGetCacheRequestConfiguration(resource.CacheRequestStrategyAsAvailable)

	// // For cache request with strategy CachedFirst
	// var cachedMessageSubscriptionRequest resource.CachedMessageSubscriptionRequest = HowToGetCacheRequestConfiguration(resource.CacheRequestStrategyCachedFirst)

	// // For cache request with strategy CachedOnly
	// var cachedMessageSubscriptionRequest resource.CachedMessageSubscriptionRequest = HowToGetCacheRequestConfiguration(resource.CacheRequestStrategyCachedOnly)

	// // For cache request with strategy LiveCancelsCached
	// var cachedMessageSubscriptionRequest resource.CachedMessageSubscriptionRequest = HowToGetCacheRequestConfiguration(resource.CacheRequestStrategyLiveCancelsCached)

	// Send the cache request and retrieve the cache response using a callback
	HowToSendCacheRequestWithCallback(directReceiver, cachedMessageSubscriptionRequest)

	// Send the cache request and retrieve the cache response using a callback
	HowToSendCacheRequestWithChannel(directReceiver, cachedMessageSubscriptionRequest)

	fmt.Println("\n===Interrupt (CTR+C) to handle graceful terminaltion of the messaiging service===")

	// cleanup after the main calling function has finished execution
	defer func() {
		// Terminate the Direct Receiver
		directReceiver.Terminate(1 * time.Second)
		fmt.Println("\nDirect Receiver Terminated? ", directReceiver.IsTerminated())
		// Disconnect the Message Service
		messagingService.Disconnect()
		fmt.Println("Messaging Service Disconnected? ", !messagingService.IsConnected())
	}()

	// Run forever until an interrupt signal is received
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	// Block until a interrupt signal is received.
	<-c
}
