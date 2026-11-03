// Package records has the record layout and formats Kafka's and Kinesis's
// connectors share: a row as one value, in JSON, or in Avro or Protobuf with
// a Confluent-compatible schema registry, and a value read back as a row.
package records

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/twmb/franz-go/pkg/sr"

	sdk "github.com/unsuitable001/openetl/sdk/go"
)

// The formats.
const (
	JSON     = "json"
	Avro     = "avro"
	Protobuf = "protobuf"
	// Sources only: each value as text.
	String = "string"
)

// Config is how values are encoded, at the top of a connector's config.
type Config struct {
	// Avro and Protobuf need one.
	SchemaRegistryURL      string     `json:"schemaRegistryUrl"`
	SchemaRegistryUsername string     `json:"schemaRegistryUsername"`
	SchemaRegistryPassword sdk.Secret `json:"schemaRegistryPassword"`
	// json (default), avro or protobuf; sources also read string.
	Format string `json:"format"`
}

// Validate defaults the format to JSON, and fails unless it's one of
// formats with what it needs.
func (c *Config) Validate(formats ...string) error {
	if c.Format == "" {
		c.Format = JSON
	}
	if !slices.Contains(formats, c.Format) {
		return fmt.Errorf("format %q is none of %v", c.Format, formats)
	}
	if (c.Format == Avro || c.Format == Protobuf) && c.SchemaRegistryURL == "" {
		return fmt.Errorf("format %s needs schemaRegistryUrl", c.Format)
	}
	return nil
}

// Registry is nil without a schema registry URL.
func (c *Config) Registry() (*sr.Client, error) {
	if c.SchemaRegistryURL == "" {
		return nil, nil
	}
	opts := []sr.ClientOpt{sr.URLs(c.SchemaRegistryURL), sr.UserAgent("openetl")}
	if c.SchemaRegistryUsername != "" {
		opts = append(opts, sr.BasicAuth(c.SchemaRegistryUsername, c.SchemaRegistryPassword.Reveal()))
	}
	cl, err := sr.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("schema registry: %w", err)
	}
	return cl, nil
}

func (c *Col) Name() string { return c.name }

// ValueFields are the fields of subject's latest schema in the registry,
// for Avro and Protobuf: none without a registry, a schema, or a record or
// message at its root.
func ValueFields(ctx context.Context, reg *sr.Client, format, subject string) ([]arrow.Field, error) {
	if reg == nil || (format != Avro && format != Protobuf) {
		return nil, nil
	}
	ss, err := reg.SchemaByVersion(ctx, subject, -1)
	var re *sr.ResponseError
	switch {
	case errors.As(err, &re) && re.StatusCode == 404:
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("the schema of %s: %w", subject, err)
	}
	var fields []arrow.Field
	add := func(name string, t arrow.DataType, md arrow.Metadata) {
		fields = append(fields, arrow.Field{Name: name, Type: t, Nullable: true, Metadata: md})
	}
	switch {
	case format == Avro && ss.Type == sr.TypeAvro:
		a, err := parseAvro(ctx, reg, ss.Schema)
		if err != nil {
			return nil, fmt.Errorf("the schema of %s: %w", subject, err)
		}
		if a.root.Type == "record" {
			for _, f := range a.root.Fields {
				t, md := avroArrow(&f.Type)
				add(f.Name, t, md)
			}
		}
	case format == Protobuf && ss.Type == sr.TypeProtobuf:
		fd, err := protoFile(ctx, reg, ss.Schema)
		if err != nil {
			return nil, fmt.Errorf("the schema of %s: %w", subject, err)
		}
		if fd.Messages().Len() > 0 {
			fs := fd.Messages().Get(0).Fields()
			for i := range fs.Len() {
				t, md := protoArrow(fs.Get(i))
				add(string(fs.Get(i).Name()), t, md)
			}
		}
	}
	return fields, nil
}

// AppendFields appends a decoded value to a row's JSON object being built:
// an object's fields, or else the value as "value", each followed by a
// comma.
func AppendFields(b []byte, v json.RawMessage) []byte {
	if len(v) > 0 && v[0] == '{' {
		if inner := strings.TrimSpace(string(v[1 : len(v)-1])); inner != "" {
			b = append(b, inner...)
			b = append(b, ',')
		}
		return b
	}
	b = append(b, `"value":`...)
	b = append(b, v...)
	return append(b, ',')
}

const sourceNamesKey = "openetl.source_names"

// KeyColumns returns the destination names of a stream's primary key, which
// the catalog gives by source name.
func KeyColumns(sc *arrow.Schema, key []string) ([]string, error) {
	renamed := map[string]string{}
	if v, ok := sc.Metadata().GetValue(sourceNamesKey); ok {
		var names map[string]string
		if err := json.Unmarshal([]byte(v), &names); err != nil {
			return nil, fmt.Errorf("its %s metadata: %w", sourceNamesKey, err)
		}
		for dst, src := range names {
			renamed[src] = dst
		}
	}
	out := make([]string, len(key))
	for i, k := range key {
		out[i] = k
		if dst, ok := renamed[k]; ok {
			out[i] = dst
		}
		if len(sc.FieldIndices(out[i])) == 0 {
			return nil, fmt.Errorf("its key has %q, which its batches don't have", k)
		}
	}
	return out, nil
}
