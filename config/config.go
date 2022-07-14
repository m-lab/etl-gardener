// Package config defines the config for the binary.
// For Gardener, the intent is to provide the config.yml file through config map.
package config

// Modelled on https://dev.to/ilyakaznacheev/a-clean-way-to-pass-configs-in-a-go-application-1g64

import (
	"errors"
	"log"
	"os"
	"time"

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
	Bucket     string   `yaml:"bucket"`
	Experiment string   `yaml:"experiment"`
	Datatype   string   `yaml:"datatype"`
	Filter     string   `yaml:"filter"`
	Datasets   Datasets `yaml:"target_datasets"`
	DailyOnly  bool     `yaml:"daily_only"`
}

// Datasets contains the name of BigQuery datasets used for temporary, raw, or
// joined tables.
type Datasets struct {
	Tmp  string `yaml:"tmp"`
	Raw  string `yaml:"raw"`
	Join string `yaml:"join"`
}

// Gardener is the full config for a Gardener instance.
type Gardener struct {
	StartDate time.Time      `yaml:"start_date"`
	Tracker   TrackerConfig  `yaml:"tracker"`
	Monitor   MonitorConfig  `yaml:"monitor"`
	Sources   []SourceConfig `yaml:"sources"`
}

// StartDate returns the first date that should be processed.
func (g *Gardener) Start() time.Time {
	return g.StartDate.UTC().Truncate(24 * time.Hour)
}

// ParseConfig loads the full Config, or Exits on failure.
func ParseConfig(name string) (*Gardener, error) {
	log.Println("Config path:", name)
	g := &Gardener{}
	err := readFile(name, g)
	if err != nil {
		return nil, err
	}
	return g, nil
}

var ErrNoConfig = errors.New("no config file given")

func readFile(name string, cfg *Gardener) error {
	if name == "" {
		return ErrNoConfig
	}
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()

	decoder := yaml.NewDecoder(f)
	return decoder.Decode(cfg)
}
