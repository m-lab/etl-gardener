// Package config defines the config for the binary.
// For Gardener, the intent is to provide the config.yml file through config map.
package config

// Modelled on https://dev.to/ilyakaznacheev/a-clean-way-to-pass-configs-in-a-go-application-1g64

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/kelseyhightower/envconfig"
	"gopkg.in/yaml.v2"
)

type StatusConfig struct {
	Port string `yaml:"port"`
}

type TrackerConfig struct {
	Timeout time.Duration `yaml:"timeout"`
}

type SourceConfig struct {
	Bucket      string    `yaml:"bucket"`
	ArchivePath string    `yaml:"archive_path"`
	Datatype    string    `yaml:"datatype"`
	Start       time.Time `yaml:"start"`
	Prefix      string    `yaml:"prefix"`
	Filter      string    `yaml:"filter"`
	Target      string    `yaml:"target"`
}

// Gardener is the full config for a Gardener instance.
type Gardener struct {
	Project string `yaml:"project"` // informational
	Commit  string `yaml:"commit"`
	Release string `yaml:"release"`

	Status StatusConfig `yaml:"status"`

	Tracker TrackerConfig `yaml:"tracker"`

	Sources []SourceConfig `yaml:"sources"`
}

var gardener Gardener

// ParseConfig loads the full Config, or Exits on failure.
func ParseConfig() {
	log.Println("config init")
	readFile(&gardener)
	readEnv(&gardener)

	log.Printf("%+v\n", gardener)
}

func processError(err error) {
	fmt.Println(err)
	os.Exit(2)
}

var configPath = flag.String("config_path", "config.yml", "Path to the config file.")

func readFile(cfg *Gardener) {
	log.Println("Config path:", *configPath)
	if *configPath == "" {
		return
	}
	f, err := os.Open(*configPath)
	if err != nil {
		processError(err)
	}
	defer f.Close()

	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(cfg)
	if err != nil {
		processError(err)
	}
}

func readEnv(cfg *Gardener) {
	err := envconfig.Process("", cfg)
	if err != nil {
		processError(err)
	}
}
