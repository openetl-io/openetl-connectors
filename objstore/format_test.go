package objstore

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

// What released versions wrote and read back: the descriptor, a split's
// state, and the keys under the URL.
func TestPersistedFormats(t *testing.T) {
	desc, _ := json.Marshal(staged{Generation: 3, Epoch: 7, Streams: []stagedStream{{
		Name: "s", Table: "t", Files: []stagedFile{{Staged: "a", Target: "b", Rows: 9}}, Read: "5", Ended: true,
	}}})
	state, _ := json.Marshal(readState{
		Floor: 1, Read: map[string]int64{"k": 2}, Last: "l", At: &filePos{Key: "k", Modified: 1, Size: 2, RowGroup: 3, Offset: 4, Records: 5}, Range: &hashRange{Lo: 1, Hi: 2},
		Gave: []cut{{Keep: hashRange{Lo: 1, Hi: 2}, Give: hashRange{Lo: 3, Hi: 4}, Key: "k", Modified: 5}},
	})
	w := &writer{cfg: destination.OpenConfig{Pipeline: "p"}}
	for got, want := range map[string]string{
		string(desc):                    `{"generation":3,"epoch":7,"streams":[{"name":"s","table":"t","files":[{"staged":"a","target":"b","rows":9}],"read":"5","ended":true}]}`,
		string(state):                   `{"floor":1,"read":{"k":2},"last":"l","at":{"key":"k","modified":1,"size":2,"rowGroup":3,"offset":4,"records":5},"range":{"lo":1,"hi":2},"gave":[{"keep":{"lo":1,"hi":2},"give":{"lo":3,"hi":4},"key":"k","modified":5}]}`,
		w.staging():                     "_openetl/p/staging/",
		w.marker("a/b", 7, 3):           "_openetl/p/loads/a%2Fb/00000000000000000007-g00000000000000000003.json",
		readDir("t", time.UnixMicro(5)): "t/_reads/00000000000000000005/",
		schemaKey("t"):                  "_openetl/schemas/t.json",
		hashRange{Lo: 1, Hi: 2}.id():    "files-00000001-000000002",
		"t/" + manifestPath:             "t/_symlink_format_manifest/manifest",
		extension(formatParquet, ""):    ".parquet",
	} {
		if got != want {
			t.Errorf("got %s, want %s", got, want)
		}
	}
}

// A setting that neither the core nor the store has is refused, inside a
// stream too.
func TestUnknownSettings(t *testing.T) {
	var settings struct {
		Region string `json:"region"`
	}
	for _, config := range []string{
		`{"url": "file:///x", "streams": [{"name": "s", "glob": "*", "format": "csv"}], "colour": 1}`,
		`{"url": "file:///x", "streams": [{"name": "s", "globb": "*", "format": "csv"}]}`,
		`{"url": "file:///x", "streams": [{"name": "s", "glob": "*", "format": "csv", "csv": {"delimeter": ";"}}]}`,
	} {
		if err := decode([]byte(config), &SourceConfig{}, &settings); err == nil {
			t.Errorf("%s: no error", config)
		}
	}
	if err := decode([]byte(`{"url": "file:///x", "REGION": "r"}`), &SourceConfig{}, &settings); err != nil || settings.Region != "r" {
		t.Errorf("the store's setting: %v, %q", err, settings.Region)
	}
}
