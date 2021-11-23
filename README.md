# PubSub+ Go API

This repo hosts sample code to showcase how the Solace PubSub+ Go API could be used. You can find:

1. `/patterns` --> runnable code showcasing different message exchange patters with the PubSub+ Go API
1. `/howtos` --> code snippets showcasing how to use different features of the API. All howtos are named `how_to_*.go` with some sampler files under sub folders.

## Environment Setup

Install the latest supported version of Go from https://go.dev/doc/install. Currently, the samples are run and tested against [Go v1.17](https://go.dev/dl/)

Note: For development purposes, you can clone the source code of the Go API directly into this directory `git clone git@github.com:<NameOfRepo>.git`

## Get the PubSub+ Go API

Download the early access Solace PubSub+ Go API package from the [Solace Community](https://solace.community/)

## Run Patterns
1. Copy the PubSub+ Go API into this directory
1. [Skip if existing] Initialize the directory with go modules `go mod init SolaceSamples.com/PubSub+Go`
1. Modify the go.mod file to replace
1. Run the samples. There are two way to run the samples

- Run: Navigate to the [patterns](./patterns) directory and execute `go run <name_of_sample>.go`
- Build: Navigate to the [patterns](./patterns) directory and execute `go build <name_of_sample>.go -o <name_of_sample>`. This will produce an executable that can be run via `./<name_of_sample>`

## Howtos

This directory contains compilable non run-able code

## Notes:


## Resources

- Solace Developer Portal is at [solace.dev](https://solace.dev)
- Ask the [Solace Community](https://solace.community) for further discussions and questions.
- Official go documentation on
