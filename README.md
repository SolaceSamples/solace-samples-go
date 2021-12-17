# PubSub+ Go API

This repo hosts sample code to showcase how the Solace PubSub+ Go API could be used. You can find:

1. `/patterns` --> runnable code showcasing different message exchange patters with the PubSub+ Go API
1. `/howtos` --> code snippets showcasing how to use different features of the API. All howtos are named `how_to_*.go` with some sampler files under sub folders.

## Environment Setup

1. Install the latest supported version of Go from https://go.dev/doc/install. Currently, the samples are run and tested against [Go v1.17](https://go.dev/dl/)
1. Install the Solace PubSub+ Messaging API for Go into the root of this directory. This is done by either
    1. Downloading the API archive from the [Solace Community](https://solace.community/group/4-solace-early-access-golang-api)
    1. Clone the source code into the root of this repo
    1. [Not supported in EA] `go get solace.dev/go/messaging`

## Run Patterns
1. Copy the PubSub+ Go API into this directory
1. [Skip if existing] Initialize the directory with go modules `go mod init SolaceSamples.com/PubSub+Go`
1. [For local development and EA] Modify the go.mod file to replace `solace.dev/go/messaging` with the local version of `./pubsubplus-go-client` by adding the following line in your go.mod file   
    ```
    replace solace.dev/go/messaging vX.Y.Z => ./pubsubplus-go-client
    ```
    where `X.Y.Z` refer to the version of the API being used
1. Run the samples. There are two way to run the samples
    - `go run`: Navigate to the [patterns](./patterns) directory and execute `go run <name_of_sample>.go`
    - `go build`: Navigate to the [patterns](./patterns) directory and execute `go build <name_of_sample>.go -o <name_of_sample>`. This will produce an executable that can be run via `./<name_of_sample>`

## Howtos

This directory contains compilable non run-able code

## Supported Environments
Monitor the Solace Community for updates on supported environment. As of right now, for version 0.9.0, the following environments are supported:

- Macos 64 bit (no support for M1 chip yet)
- Linux with glibc (no support for alpine)
- Windows under wsl
- Any alpine support with glibc


## Resources

- Solace Developer Portal is at [solace.dev](https://solace.dev)
- Ask the [Solace Community](https://solace.community) for further discussions and questions.
- Official go documentation on
