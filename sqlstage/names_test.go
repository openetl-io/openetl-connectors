package sqlstage

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

// The names and labels are those releases of postgres before them wrote.
func TestPersistedNames(t *testing.T) {
	e := destination.Epoch{Generation: 3, N: 7}
	for _, c := range []struct {
		got  any
		want string
	}{
		{StagingLabel{Pipeline: "p", Generation: 3, Epoch: 7}, `{"pipeline":"p","generation":3,"epoch":7}`},
		{ReadLabel{Pipeline: "p", Stream: "s", Table: "t"}, `{"pipeline":"p","stream":"s","table":"t"}`},
		{[]string{StagingName("p", "s", e, 0), StagingName("p", "s", e, 2), StagingName("p", "s\x00patch", e, 0), ReadName("p", "s", time.UnixMicro(1000)), IndexName("t"), KeyIndexName("t")},
			`["_openetl_stage_ced8e3942f116e6017cb2d67","_openetl_stage_f3ef35e747c935d0772bf64c","_openetl_stage_cec0f0e9013c894c4d4b7776","_openetl_read_7878dc485bb14cc50ee5b782","_openetl_id_e3b98a4da31a127d4bde6e43","_openetl_key_e3b98a4da31a127d4bde6e43"]`},
	} {
		b, err := json.Marshal(c.got)
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != c.want {
			t.Errorf("%s, want %s", b, c.want)
		}
	}
}
