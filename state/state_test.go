package state

import (
	"log"
	"os"
)

// InitEnv reinitializes the environment state, for testing.
func InitEnv() {
	env.Project = os.Getenv("PROJECT")
}

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}
