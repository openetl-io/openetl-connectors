package postgres

import (
	"errors"
	"slices"
	"testing"

	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

func TestDeclaredOnly(t *testing.T) {
	users := table{name: "users", cols: []sourceColumn{{name: "a"}, {name: "k1"}, {name: "new"}, {name: "k2"}}, key: []int{3, 1}}

	got, err := declaredOnly(users, []string{"a", "k1", "k2"})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, c := range got.cols {
		names = append(names, c.name)
	}
	if !slices.Equal(names, []string{"a", "k1", "k2"}) || !slices.Equal(got.key, []int{2, 1}) {
		t.Errorf("columns %v, key %v", names, got.key)
	}
	if len(users.cols) != 4 || !slices.Equal(users.key, []int{3, 1}) {
		t.Errorf("changed the table it was given: %+v", users)
	}

	got, err = declaredOnly(users, nil)
	if err != nil || len(got.cols) != 4 {
		t.Errorf("a request that doesn't say: %+v, %v", got, err)
	}

	_, err = declaredOnly(users, []string{"a", "k1"})
	var coded interface{ ErrorCode() protocolv1.ErrorCode }
	if !errors.As(err, &coded) || coded.ErrorCode() != protocolv1.ErrorCode_ERROR_CODE_TRANSIENT {
		t.Errorf("a key column it didn't declare: %v", err)
	}
}
