package airbyte

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os/exec"
	"strings"

	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"go.mongodb.org/mongo-driver/bson"
	"golang.org/x/sync/errgroup"
)

func NewAirbyteExecutable(dockerImage string) *AirbyteExecutable {
	return &AirbyteExecutable{dockerImage}
}

type AirbyteExecutable struct {
	dockerImage string
}

func (e *AirbyteExecutable) createCommand(ctx context.Context, action string, config string, catalog string, state string) *exec.Cmd {
	args := []string{
		"run",
		"--rm", "-i",
		"--network", "host",
	}
	if config != "" {
		args = append(args, "-v", config+":/tmp/config.json")
	}
	if catalog != "" {
		args = append(args, "-v", catalog+":/tmp/catalog.json")
	}
	if state != "" {
		args = append(args, "-v", state+":/tmp/state.json")
	}
	args = append(args, e.dockerImage)
	args = append(args, action)
	if config != "" {
		args = append(args, "--config", "/tmp/config.json")
	}
	if catalog != "" {
		args = append(args, "--catalog", "/tmp/catalog.json")
	}
	if state != "" {
		args = append(args, "--state", "/tmp/state.json")
	}

	return exec.CommandContext(ctx, "docker", args...)
}

func (e *AirbyteExecutable) Spec(ctx context.Context) ([]byte, error) {
	cmd := e.createCommand(ctx, "spec", "", "", "")
	return cmd.Output()
}

func (e *AirbyteExecutable) Check(ctx context.Context, config string) (bool, error) {
	cmd := e.createCommand(ctx, "check", config, "", "")
	b, err := cmd.Output()
	if err != nil {
		return false, err
	}
	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		line := sc.Text()
		if strings.Contains(line, "CONNECTION_STATUS") && strings.Contains(line, "SUCCEEDED") {
			return true, nil
		}
	}
	return false, nil
}

func (e *AirbyteExecutable) Discover(ctx context.Context, config string) ([]byte, error) {
	cmd := e.createCommand(ctx, "discover", config, "", "")
	return cmd.Output()
}

func (e *AirbyteExecutable) Read(ctx context.Context, config string, catalog string, state string, ch chan<- []byte) error {
	defer close(ch)
	cmd := e.createCommand(ctx, "read", config, catalog, state)
	stdout, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		return err
	}

	sc := bufio.NewScanner(stdout)
	for sc.Scan() {
		ch <- bytes.Clone(sc.Bytes())
	}

	return cmd.Wait()
}

func (e *AirbyteExecutable) Write(ctx context.Context, config string, catalog string, chIn <-chan []byte, chOut chan<- []byte) error {
	eg, egCtx := errgroup.WithContext(ctx)
	cmd := e.createCommand(egCtx, "write", config, catalog, "")
	stdout, _ := cmd.StdoutPipe()
	stdin, _ := cmd.StdinPipe()
	if err := cmd.Start(); err != nil {
		close(chOut)
		return err
	}

	eg.Go(func() error {
		defer stdin.Close()
		for input := range chIn {
			_, err := stdin.Write(input)
			if err != nil {
				return err
			}
			_, err = stdin.Write([]byte("\n"))
			if err != nil {
				return err
			}
		}
		return nil
	})

	eg.Go(func() error {
		defer close(chOut)
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			chOut <- bytes.Clone(sc.Bytes())
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		_ = cmd.Wait()
		// Still return the first error
		return err
	}

	return cmd.Wait()
}

func NewAirbyteCatalog(data []byte) (*AirbyteCatalog, error) {
	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() {
		var j map[string]interface{}
		err := json.Unmarshal(sc.Bytes(), &j)
		if err != nil {
			return nil, err
		}
		if j["type"] == "CATALOG" {
			streams := j["catalog"].(map[string]interface{})["streams"].([]interface{})
			m := map[string]interface{}{}
			for _, s := range streams {
				name := s.(map[string]interface{})["name"].(string)
				m[name] = s
			}
			return &AirbyteCatalog{m}, nil
		}
	}

	return nil, nil
}

type AirbyteCatalog struct {
	streams map[string]interface{}
}

func (c *AirbyteCatalog) CatalogNames() []string {
	var names []string
	for name := range c.streams {
		names = append(names, name)
	}
	return names
}

func (c *AirbyteCatalog) ConfigureCatalog(names []string, syncMode string, destinationSyncMode string, cursorField []string, primaryKey [][]string, syncID int, minimumGenerationID int, generationID int) ([]byte, error) {
	var configured []interface{}
	for _, name := range names {
		stream, ok := c.streams[name]
		if !ok {
			continue
		}
		m := map[string]interface{}{}
		m["sync_mode"] = syncMode
		m["destination_sync_mode"] = destinationSyncMode
		if cursorField != nil {
			m["cursor_field"] = cursorField
		}
		if primaryKey != nil {
			m["primary_key"] = primaryKey
		}
		if syncID >= 0 {
			m["sync_id"] = syncID
		}
		if generationID >= 0 {
			m["generation_id"] = generationID
		}
		if minimumGenerationID >= 0 {
			m["minimum_generation_id"] = minimumGenerationID
		}
		m["stream"] = stream
		configured = append(configured, m)
	}
	m := map[string]interface{}{}
	m["streams"] = configured
	return json.MarshalIndent(m, "", "\t")
}

func NewConfiguredAirbyteCatalog(data []byte) (*ConfiguredAirbyteCatalog, error) {
	var j map[string]interface{}
	err := json.Unmarshal(data, &j)
	if err != nil {
		return nil, err
	}
	streams := j["streams"].([]interface{})
	m := map[string][]interface{}{}
	for _, s := range streams {
		stream := s.(map[string]interface{})["stream"]
		name := stream.(map[string]interface{})["name"].(string)
		primary_key, ok := s.(map[string]interface{})["primary_key"]
		if !ok {
			primary_key = stream.(map[string]interface{})["source_defined_primary_key"]
		}
		if primary_key != nil {
			m[name] = primary_key.([]interface{})
		}
	}
	return &ConfiguredAirbyteCatalog{m}, nil
}

type ConfiguredAirbyteCatalog struct {
	primaryKeys map[string][]interface{}
}

func getField(data interface{}, field []interface{}) interface{} {
	if len(field) == 0 {
		return data
	}
	return getField(data.(map[string]interface{})[field[0].(string)], field[1:])
}

func (c *ConfiguredAirbyteCatalog) ExtractID(stream string, data interface{}) ([]*adiomv1.BsonValue, error) {
	pks := c.primaryKeys[stream]
	var res []*adiomv1.BsonValue
	for _, pk := range pks {
		f := getField(data, pk.([]interface{}))
		t, v, err := bson.MarshalValue(f)
		if err != nil {
			return nil, err
		}
		res = append(res, &adiomv1.BsonValue{
			Data: v,
			Type: uint32(t),
		})
	}
	return res, nil
}

type Record struct {
	Stream string                 `json:"stream"`
	Data   map[string]interface{} `json:"data"`
}

type State struct {
	Type   string          `json:"type"`
	Stream json.RawMessage `json:"stream"`
	Data   json.RawMessage `json:"data"`
	Global json.RawMessage `json:"global"`
}

type ReadStreamMessage struct {
	Type   string `json:"type"`
	Record Record `json:"record"`
	State  State  `json:"state"`
}
