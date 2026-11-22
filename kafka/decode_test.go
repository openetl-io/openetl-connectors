package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/twmb/avro"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/pkg/sr/srfake"

	"github.com/openetl-io/openetl-connectors/records"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// rawRows keeps the rows a read of whole streams emits.
type rawRows struct {
	mu   sync.Mutex
	rows []map[string]any
}

func (c *rawRows) Rows(_ string, rows []json.RawMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range rows {
		var m map[string]any
		if err := json.Unmarshal(r, &m); err != nil {
			return err
		}
		c.rows = append(c.rows, m)
	}
	return nil
}
func (c *rawRows) Records(string, arrow.RecordBatch) error    { return errors.New("records") }
func (c *rawRows) ResumePoint(string, []byte) error           { return nil }
func (c *rawRows) Complete(string, []byte) error              { return nil }
func (c *rawRows) Split(stream, _ string) source.SplitEmitter { return rawSplit{c, stream} }

type rawSplit struct {
	c      *rawRows
	stream string
}

func (s rawSplit) Rows(rows []json.RawMessage) error         { return s.c.Rows(s.stream, rows) }
func (s rawSplit) Records(arrow.RecordBatch) error           { return errors.New("records") }
func (s rawSplit) ResumePoint([]byte, ...source.Split) error { return nil }
func (s rawSplit) Complete([]byte, ...source.Split) error    { return nil }

// readRows reads topic whole from its start, to its end.
func readRows(t *testing.T, s *Source, topic string) ([]map[string]any, error) {
	t.Helper()
	out := &rawRows{}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	err := s.Read(ctx, source.ReadRequest{Streams: []source.ConfiguredStream{{Name: topic, State: encodeState(map[int32]int64{0: 0})}}, Pipeline: "p", Lanes: 1}, out)
	return out.rows, err
}

// avroWire registers schema under subject and encodes v with it, in the
// registry's wire format.
func avroWire(t *testing.T, cl *sr.Client, subject, schema string, v any) ([]byte, int) {
	t.Helper()
	ss, err := cl.CreateSchema(t.Context(), subject, sr.Schema{Schema: schema, Type: sr.TypeAvro})
	if err != nil {
		t.Fatal(err)
	}
	s, err := avro.Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	var h sr.ConfluentHeader
	b, _ := h.AppendEncode(nil, ss.ID, nil)
	if b, err = s.AppendEncode(b, v); err != nil {
		t.Fatal(err)
	}
	return b, ss.ID
}

func produceOne(t *testing.T, addrs []string, topic string, recs ...*kgo.Record) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addrs...), kgo.DefaultProduceTopic(topic))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if _, err := kadm.NewClient(cl).CreateTopic(t.Context(), 1, 1, nil, topic); err != nil {
		t.Fatal(err)
	}
	if err := cl.ProduceSync(t.Context(), recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// A key whose schema the registry doesn't serve, for now, fails the read,
// to be tried again, rather than landing as its encoded bytes. A key the
// registry answers isn't in its wire format, such as a long's eight bytes
// whose schema ID it doesn't have, still lands as its bytes' text.
func TestAKeyTheRegistryDoesntAnswerForFailsTheRead(t *testing.T) {
	addrs := fake(t)
	reg := srfake.New()
	defer reg.Close()
	cl, err := sr.NewClient(sr.URLs(reg.URL()))
	if err != nil {
		t.Fatal(err)
	}
	topic := "orders"
	value, _ := avroWire(t, cl, topic+"-value", `{"type":"record","name":"v","fields":[{"name":"n","type":"long"}]}`, map[string]any{"n": int64(1)})
	key, keyID := avroWire(t, cl, topic+"-key", `{"type":"record","name":"k","fields":[{"name":"id","type":"long"}]}`, map[string]any{"id": int64(7)})
	long := []byte{0, 0, 0, 0, 0, 0, 0, 5}
	produceOne(t, addrs, topic, &kgo.Record{Key: key, Value: value}, &kgo.Record{Key: long, Value: value})

	s, err := NewSource(fakeConfig(t, addrs, map[string]any{"topics": []string{topic}, "format": "avro", "schemaRegistryUrl": reg.URL()}))
	if err != nil {
		t.Fatal(err)
	}
	keyPath := fmt.Sprintf("/schemas/ids/%d", keyID)
	reg.Intercept(func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path != keyPath {
			return false
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		return true
	})
	rows, err := readRows(t, s, topic)
	if codeOf(err) != "ERROR_CODE_TRANSIENT" {
		t.Errorf("with the registry failing the key's schema, the read ended with %v and rows %v", err, rows)
	}
	for _, r := range rows {
		if r[offsetColumn] == 0.0 {
			t.Errorf("the record whose key's schema the registry failed landed as %v", r)
		}
	}

	reg.ClearInterceptors()
	rows, err = readRows(t, s, topic)
	if err != nil || len(rows) != 2 {
		t.Fatalf("once the registry answered: %v, %v", rows, err)
	}
	if got := rows[0][keyColumn]; got != `{"id":7}` {
		t.Errorf("the Avro key landed as %q", got)
	}
	var want string
	if err := json.Unmarshal(records.JSONText(long), &want); err != nil {
		t.Fatal(err)
	}
	if got := rows[1][keyColumn]; got != want {
		t.Errorf("the eight bytes of a long landed as %q, want %q", got, want)
	}
}

// A value whose schema the registry doesn't serve, for now, fails the read
// as transient, not as invalid data.
func TestAValueTheRegistryDoesntAnswerForIsTransient(t *testing.T) {
	addrs := fake(t)
	reg := srfake.New()
	defer reg.Close()
	cl, err := sr.NewClient(sr.URLs(reg.URL()))
	if err != nil {
		t.Fatal(err)
	}
	topic := "orders"
	value, _ := avroWire(t, cl, topic+"-value", `{"type":"record","name":"v","fields":[{"name":"n","type":"long"}]}`, map[string]any{"n": int64(1)})
	produceOne(t, addrs, topic, &kgo.Record{Value: value})
	s, err := NewSource(fakeConfig(t, addrs, map[string]any{"topics": []string{topic}, "format": "avro", "schemaRegistryUrl": reg.URL()}))
	if err != nil {
		t.Fatal(err)
	}
	reg.Intercept(func(w http.ResponseWriter, r *http.Request) bool {
		w.WriteHeader(http.StatusBadGateway)
		return true
	})
	if _, err := readRows(t, s, topic); codeOf(err) != "ERROR_CODE_TRANSIENT" {
		t.Errorf("with the registry failing, the read ended with %v (%v)", err, codeOf(err))
	}
	reg.ClearInterceptors()
	broken := append([]byte{0, 0, 0, 0, 1}, 0xff, 0xff, 0xff)
	broke := &kgo.Record{Topic: topic, Value: broken}
	pc, err := kgo.NewClient(kgo.SeedBrokers(addrs...))
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	if err := pc.ProduceSync(t.Context(), broke).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if _, err := readRows(t, s, topic); codeOf(err) != "ERROR_CODE_DATA_INVALID" {
		t.Errorf("a value that doesn't decode ended the read with %v (%v)", err, codeOf(err))
	}
}

// A header value that isn't UTF-8 lands as its bytes' base64, as a key
// does, rather than with its bytes replaced.
func TestABinaryHeaderLandsWhole(t *testing.T) {
	addrs := fake(t)
	topic := "events"
	produceOne(t, addrs, topic, &kgo.Record{Value: []byte(`{"n":1}`), Headers: []kgo.RecordHeader{
		{Key: "text", Value: []byte("é")}, {Key: "bin", Value: []byte{0xff, 0x00, 0xfe}},
	}})
	s, err := NewSource(fakeConfig(t, addrs, map[string]any{"topics": []string{topic}}))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := readRows(t, s, topic)
	if err != nil || len(rows) != 1 {
		t.Fatalf("%v, %v", rows, err)
	}
	h, ok := rows[0][headersColumn].(map[string]any)
	if !ok || h["text"] != "é" || h["bin"] != "/wD+" {
		t.Errorf("headers landed as %v", rows[0][headersColumn])
	}
}
