package config

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestLoadCommittedSmoke(t *testing.T) {
	path := committedConfigPath(t)
	loaded, err := Load(path, "smoke")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Name != "smoke" {
		t.Fatalf("name %q", loaded.Name)
	}
	if loaded.Scenario.PositionCount != 10 {
		t.Fatalf("positionCount %d", loaded.Scenario.PositionCount)
	}
	if loaded.Scenario.BaseDocumentDate.IsZero() {
		t.Fatal("baseDocumentDate is zero")
	}
	if loaded.Config.OutboxPublisher.PollInterval.Duration() != 500*time.Millisecond {
		t.Fatalf("pollInterval %s", loaded.Config.OutboxPublisher.PollInterval.Duration())
	}
}

func TestSelectScenarioFlagWins(t *testing.T) {
	path := committedConfigPath(t)
	loaded, err := Load(path, "parallel-load")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Name != "parallel-load" || loaded.Scenario.ParallelThreads != 8 {
		t.Fatalf("%s threads=%d", loaded.Name, loaded.Scenario.ParallelThreads)
	}
}

func TestUnknownScenario(t *testing.T) {
	path := committedConfigPath(t)
	if _, err := Load(path, "missing"); err == nil {
		t.Fatal("expected error")
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load("no-such-file.yaml", ""); err == nil {
		t.Fatal("expected error")
	}
}

func committedConfigPath(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("caller")
	}
	path := filepath.Join(filepath.Dir(file), "..", "..", "configs", "generation.yaml")
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	return path
}
