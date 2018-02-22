package main

import (
	"os"

	"google.golang.org/api/option"
)

func Options() []option.ClientOption {
	opts := []option.ClientOption{}
	if os.Getenv("TRAVIS") != "" {
		authOpt := option.WithCredentialsFile("../../travis-testing.key")
		opts = append(opts, authOpt)
	}

	return opts
}

// TODO Add unit tests
