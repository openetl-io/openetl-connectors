//go:build integration

package azureblob

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"testing"

	"gocloud.dev/blob"

	"github.com/openetl-io/openetl-connectors/objstore"
	"github.com/openetl-io/openetl-connectors/objstore/azurestore"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

func TestLive(t *testing.T) {
	account := os.Getenv("OPENETL_TEST_AZURE_ACCOUNT")
	if account == "" {
		t.Skip("OPENETL_TEST_AZURE_ACCOUNT isn't set")
	}
	containerName := cmp.Or(os.Getenv("OPENETL_TEST_AZURE_CONTAINER"), "openetl-test")
	endpoint := cmp.Or(os.Getenv("OPENETL_TEST_AZURE_ENDPOINT"), fmt.Sprintf("https://%s.blob.core.windows.net", account))
	prefixBase := cmp.Or(strings.Trim(os.Getenv("OPENETL_TEST_AZURE_PREFIX"), "/"), "ci-smoke")
	kind := cmp.Or(os.Getenv("OPENETL_TEST_AZURE_KIND"), "blob")
	t.Logf("azure live smoke: kind=%s account=%s container=%s endpoint=%s", kind, account, containerName, endpoint)

	t.Run("rbac", func(t *testing.T) {
		runPrefix := prefixBase + "/rbac-" + strings.ToLower(rand.Text())[:10]
		t.Cleanup(func() { cleanupAzure(t, endpoint, account, containerName, "", runPrefix) })

		cfg := map[string]any{
			"url":         "azblob://" + containerName + "/" + runPrefix,
			"endpoint":    endpoint,
			"accountName": account,
		}
		rbacStore := objstore.TestStore{
			Name:           "azure-rbac-" + kind,
			Config:         cfg,
			Unreachable:    map[string]any{"endpoint": "https://127.0.0.1:1"},
			BadCredentials: map[string]any{"accountKey": strings.Repeat("A", 86) + "=="},
		}

		t.Run("conformance", func(t *testing.T) {
			objstore.Conformance(t, "azureblob", rbacStore)
		})

		t.Run("checks", func(t *testing.T) {
			verifyChecksAndPreflight(t, cfg)
		})
	})

	t.Run("sas", func(t *testing.T) {
		sasToken := os.Getenv("OPENETL_TEST_AZURE_SAS_TOKEN")
		if sasToken == "" {
			t.Skip("OPENETL_TEST_AZURE_SAS_TOKEN isn't set")
		}
		runPrefix := prefixBase + "/sas-" + strings.ToLower(rand.Text())[:10]
		t.Cleanup(func() { cleanupAzure(t, endpoint, account, containerName, sasToken, runPrefix) })

		badSAS := "sv=2024-11-04&ss=b&srt=sco&sp=rwdlac&se=2000-01-01T00:00:00Z&st=2000-01-01T00:00:00Z&spr=https&sig=invalid"
		cfg := map[string]any{
			"url":         "azblob://" + containerName + "/" + runPrefix,
			"endpoint":    endpoint,
			"accountName": account,
			"sasToken":    sasToken,
		}
		sasStore := objstore.TestStore{
			Name:           "azure-sas-" + kind,
			Config:         cfg,
			Unreachable:    map[string]any{"endpoint": "https://127.0.0.1:1"},
			BadCredentials: map[string]any{"sasToken": badSAS},
		}

		t.Run("conformance", func(t *testing.T) {
			objstore.Conformance(t, "azureblob", sasStore)
		})

		t.Run("checks", func(t *testing.T) {
			verifyChecksAndPreflight(t, cfg)
		})

		t.Run("bad-sas", func(t *testing.T) {
			d, err := NewDestination(mustMarshal(t, map[string]any{
				"url":         cfg["url"],
				"endpoint":    endpoint,
				"accountName": account,
				"sasToken":    badSAS,
			}))
			if err != nil {
				t.Fatal(err)
			}
			err = check.Run(t.Context(), d.Checks()).Err()
			if got := codeOf(err); got != protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID {
				t.Fatalf("bad SAS code = %s (%v), want ERROR_CODE_AUTH_INVALID", got, err)
			}
		})

		t.Run("http-sas-refused", func(t *testing.T) {
			_, err := NewDestination(mustMarshal(t, map[string]any{
				"url":         cfg["url"],
				"endpoint":    fmt.Sprintf("http://%s.blob.core.windows.net", account),
				"accountName": account,
				"sasToken":    sasToken,
			}))
			if err == nil {
				t.Fatal("expected NewDestination to refuse sasToken over http://")
			}
		})
	})
}

func verifyChecksAndPreflight(t *testing.T, cfg map[string]any) {
	t.Helper()
	d, err := NewDestination(mustMarshal(t, cfg))
	if err != nil {
		t.Fatal(err)
	}
	checks := d.Checks()
	assertTLSNonAdvisory(t, checks)
	rep := check.Run(t.Context(), checks)
	if err := rep.Err(); err != nil {
		t.Fatalf("destination checks failed: %v", err)
	}
	assertPassed(t, rep, "net.dns", "net.connect", "net.tls", "store.auth", "store.bucket", "store.list", "store.write", "store.delete")

	preflight := check.Run(t.Context(), d.Preflight(destination.PreflightRequest{
		Pipeline: "live-smoke",
		Streams:  []destination.Stream{{Name: "events", Mode: destination.Overwrite}},
	}))
	if err := preflight.Err(); err != nil {
		t.Fatalf("destination preflight failed: %v", err)
	}
	assertPassed(t, preflight, "store.conditional-write")

	srcCfg := map[string]any{
		"url":         cfg["url"],
		"endpoint":    cfg["endpoint"],
		"accountName": cfg["accountName"],
		"streams": []map[string]any{
			{"name": "events", "glob": "events/*.parquet", "format": "parquet"},
		},
	}
	if sas, ok := cfg["sasToken"]; ok {
		srcCfg["sasToken"] = sas
	}
	s, err := NewSource(mustMarshal(t, srcCfg))
	if err != nil {
		t.Fatal(err)
	}
	srcChecks := s.Checks()
	assertTLSNonAdvisory(t, srcChecks)
	srcRep := check.Run(t.Context(), srcChecks)
	if err := srcRep.Err(); err != nil {
		t.Fatalf("source checks failed: %v", err)
	}
	assertPassed(t, srcRep, "net.dns", "net.connect", "net.tls", "store.auth", "store.bucket", "store.list")
}

func assertTLSNonAdvisory(t *testing.T, checks []check.Check) {
	t.Helper()
	for _, c := range checks {
		if c.Name == "net.tls" {
			if c.Advisory {
				t.Errorf("net.tls check has Advisory = true, want false when endpoint is explicit")
			}
			return
		}
	}
	t.Error("net.tls check missing from Checks()")
}

func cleanupAzure(t *testing.T, endpoint, account, containerName, sasToken, prefix string) {
	t.Helper()
	st, err := azurestore.New(azurestore.Config{
		Endpoint:    endpoint,
		AccountName: account,
		SASToken:    sdk.NewSecret(sasToken),
	})
	if err != nil {
		t.Logf("cleanup azurestore.New: %v", err)
		return
	}
	ctx := context.Background()
	b, err := st.Open(ctx, containerName)
	if err != nil {
		t.Logf("cleanup open %s: %v", containerName, err)
		return
	}
	defer func() { _ = b.Close() }()
	var keys []string
	it := b.List(&blob.ListOptions{Prefix: prefix + "/"})
	for {
		obj, err := it.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Logf("cleanup list %s/%s: %v", containerName, prefix, err)
			return
		}
		keys = append(keys, obj.Key)
	}
	// Delete deepest paths first so HNS directory blobs are empty before removal.
	slices.SortFunc(keys, func(a, b string) int {
		if d := len(b) - len(a); d != 0 {
			return d
		}
		return strings.Compare(b, a)
	})
	for _, k := range keys {
		if err := b.Delete(ctx, k); err != nil {
			t.Logf("cleanup delete %s: %v", k, err)
		}
	}
}

func assertPassed(t *testing.T, rep check.Report, names ...string) {
	t.Helper()
	statuses := map[string]check.Status{}
	for _, it := range rep.Items {
		statuses[it.Name] = it.Status
	}
	for _, name := range names {
		st, ok := statuses[name]
		if !ok {
			t.Errorf("check %q missing from report: %v", name, rep.Items)
			continue
		}
		if st != check.Passed {
			t.Errorf("check %q = %s, want passed", name, st)
		}
	}
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func codeOf(err error) protocolv1.ErrorCode {
	if c, ok := errors.AsType[interface {
		error
		ErrorCode() protocolv1.ErrorCode
	}](err); ok {
		return c.ErrorCode()
	}
	return protocolv1.ErrorCode_ERROR_CODE_INTERNAL
}
