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

// TrackerConfig holds the config for the job tracker.
type TrackerConfig struct {
	Timeout time.Duration `yaml:"timeout"`
}

// MonitorConfig holds the config for the state machine monitor.
type MonitorConfig struct {
	PollingInterval time.Duration `yaml:"polling_interval"`
}

// SourceConfig holds the config that defines all data sources to be processed.
type SourceConfig struct {
	Bucket     string    `yaml:"bucket"`
	Experiment string    `yaml:"experiment"`
	Datatype   string    `yaml:"datatype"`
	Filter     string    `yaml:"filter"`
	Start      time.Time `yaml:"start"`
	Target     string    `yaml:"target"`
}

// Gardener is the full config for a Gardener instance.
type Gardener struct {
	Tracker TrackerConfig  `yaml:"tracker"`
	Monitor MonitorConfig  `yaml:"monitor"`
	Sources []SourceConfig `yaml:"sources"`
}

var gardener Gardener

// Sources returns the list of sources that should be processed.
func Sources() []SourceConfig {
	src := make([]SourceConfig, len(gardener.Sources))
	copy(src, gardener.Sources)
	return src
}

// ParseConfig loads the full Config, or Exits on failure.
func ParseConfig() {
	log.Println("config init")
	readFile(&gardener)
	readEnv(&gardener)

	log.Printf("%+v\n", gardener)
}

func processError(err error) {
	fmt.Println(err)
	// For now don't die...	os.Exit(2)
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
