package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Loaded struct {
	Config   Config
	Scenario Scenario
	Name     string
}

func Load(path, scenarioFlag string) (Loaded, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Loaded{}, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return Loaded{}, fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Loaded{}, err
	}
	name, scenario, err := cfg.SelectScenario(scenarioFlag)
	if err != nil {
		return Loaded{}, err
	}
	if err := scenario.Validate(); err != nil {
		return Loaded{}, fmt.Errorf("scenario %s: %w", name, err)
	}
	return Loaded{Config: cfg, Scenario: scenario, Name: name}, nil
}

func (c Config) SelectScenario(flagName string) (string, Scenario, error) {
	name := flagName
	if name == "" {
		name = c.Generation.ActiveScenario
	}
	if name == "" {
		return "", Scenario{}, fmt.Errorf("scenario name is empty")
	}
	scenario, ok := c.Generation.Scenarios[name]
	if !ok {
		return "", Scenario{}, fmt.Errorf("scenario %q is not defined", name)
	}
	return name, scenario, nil
}
