package sqlbatch

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSourceMappingRequiresLimit(t *testing.T) {
	c := &conn{config: Config{Mappings: map[string]Mapping{
		"public.items": {Namespace: "public.items", Query: "SELECT * FROM public.items"},
	}}}
	_, err := c.sourceMapping("public.items")
	if err == nil || !strings.Contains(err.Error(), "invalid or unspecified limit") {
		t.Fatalf("expected invalid limit error, got %v", err)
	}
}

func TestParseConfigAllowsSinkWithoutSourceMappings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sqlbatch.yml")
	contents := []byte("driver: pgx\nconnectionstring: postgres://example\nsink:\n  writebatchsize: 25\n  tables:\n    public.large_items:\n      writebatchsize: 5\n")
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	config, err := parseConfigFromFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.Sink.WriteBatchSize != 25 {
		t.Fatalf("unexpected sink config: %#v", config.Sink)
	}
	if got := config.Sink.writeBatchSize("public.large_items"); got != 5 {
		t.Fatalf("table batch size = %d, want 5", got)
	}
	if got := config.Sink.writeBatchSize("public.items"); got != 25 {
		t.Fatalf("global batch size = %d, want 25", got)
	}
}

func TestParseConfigRejectsSourceMappingWithoutLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sqlbatch.yml")
	contents := []byte("driver: pgx\nconnectionstring: postgres://example\nmappings:\n  - namespace: public.items\n    query: SELECT * FROM public.items\n")
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := parseConfigFromFile(path)
	if err == nil || !strings.Contains(err.Error(), "invalid or unspecified limit") {
		t.Fatalf("expected invalid limit error, got %v", err)
	}
}

func TestParseConfigRejectsInvalidSinkBatchSizes(t *testing.T) {
	tests := []string{
		"driver: pgx\nconnectionstring: postgres://example\nsink:\n  writebatchsize: -1\n",
		"driver: pgx\nconnectionstring: postgres://example\nsink:\n  tables:\n    public.items:\n      writebatchsize: 0\n",
	}
	for i, contents := range tests {
		path := filepath.Join(t.TempDir(), "sqlbatch.yml")
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := parseConfigFromFile(path); err == nil {
			t.Fatalf("case %d: expected invalid sink batch size error", i)
		}
	}
}
