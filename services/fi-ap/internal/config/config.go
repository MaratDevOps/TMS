package config

import (
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Postgres        PostgresConfig        `yaml:"postgres"`
	Kafka           KafkaConfig           `yaml:"kafka"`
	Metrics         MetricsConfig         `yaml:"metrics"`
	Generation      GenerationConfig      `yaml:"generation"`
	OutboxPublisher OutboxPublisherConfig `yaml:"outboxPublisher"`
}

type PostgresConfig struct {
	DSN string `yaml:"dsn"`
}

type KafkaConfig struct {
	Brokers        []string `yaml:"brokers"`
	Topic          string   `yaml:"topic"`
	ProducerSystem string   `yaml:"producerSystem"`
}

type MetricsConfig struct {
	Listen string `yaml:"listen"`
}

type GenerationConfig struct {
	ActiveScenario string              `yaml:"activeScenario"`
	ReferenceData  ReferenceData       `yaml:"referenceData"`
	Scenarios      map[string]Scenario `yaml:"scenarios"`
}

type ReferenceData struct {
	CompanyCodes        []string `yaml:"companyCodes"`
	CounterpartyIDs     []string `yaml:"counterpartyIds"`
	Currencies          []string `yaml:"currencies"`
	PaymentMethods      []string `yaml:"paymentMethods"`
	Statuses            []string `yaml:"statuses"`
	OriginDocumentTypes []string `yaml:"originDocumentTypes"`
}

type Scenario struct {
	PositionCount           int         `yaml:"positionCount"`
	ParallelThreads         int         `yaml:"parallelThreads"`
	RandomSeed              int64       `yaml:"randomSeed"`
	PositionsPerDocument    int         `yaml:"positionsPerDocument"`
	BaseDocumentDate        time.Time   `yaml:"baseDocumentDate"`
	OriginDocumentShare     float64     `yaml:"originDocumentShare"`
	Amount                  AmountRange `yaml:"amount"`
	DueDays                 IntRange    `yaml:"dueDays"`
	PaymentBlockProbability float64     `yaml:"paymentBlockProbability"`
	PaymentPurposeTemplate  string      `yaml:"paymentPurposeTemplate"`
	StopOnError             bool        `yaml:"stopOnError"`
}

type AmountRange struct {
	Min DecimalString `yaml:"min"`
	Max DecimalString `yaml:"max"`
}

type DecimalString string

type IntRange struct {
	Min int `yaml:"min"`
	Max int `yaml:"max"`
}

type OutboxPublisherConfig struct {
	Enabled                 bool        `yaml:"enabled"`
	Workers                 int         `yaml:"workers"`
	BatchSize               int         `yaml:"batchSize"`
	PollInterval            Duration    `yaml:"pollInterval"`
	ClaimTransactionTimeout Duration    `yaml:"claimTransactionTimeout"`
	LockTimeout             Duration    `yaml:"lockTimeout"`
	KafkaSendTimeout        Duration    `yaml:"kafkaSendTimeout"`
	MaxAttempts             int         `yaml:"maxAttempts"`
	Retry                   RetryConfig `yaml:"retry"`
	ShutdownTimeout         Duration    `yaml:"shutdownTimeout"`
}

type RetryConfig struct {
	InitialInterval Duration `yaml:"initialInterval"`
	Multiplier      float64  `yaml:"multiplier"`
	MaxInterval     Duration `yaml:"maxInterval"`
	Jitter          float64  `yaml:"jitter"`
}

type Duration time.Duration

func (d Duration) Duration() time.Duration {
	return time.Duration(d)
}

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(parsed)
	return nil
}

func (d *DecimalString) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.ScalarNode {
		return fmt.Errorf("decimal: expected scalar")
	}
	*d = DecimalString(value.Value)
	return nil
}
