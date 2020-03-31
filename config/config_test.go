package config_test

import (
	"flag"
	"log"
	"testing"

	"github.com/m-lab/etl-gardener/config"
	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/rtx"
)

var _ = func() error {
	log.Println("Setting config path")
	flag.Set("config_path", "testdata/config.yml")
	return nil
}()

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestBasic(t *testing.T) {
	flag.Parse()
	rtx.Must(flagx.ArgsFromEnv(flag.CommandLine), "Could not get args from env")

	config.ParseConfig()

}
