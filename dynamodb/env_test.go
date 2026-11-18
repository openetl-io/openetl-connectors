//go:build integration

package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// endpoint is DynamoDB Local's, which takes any credentials whose access
// key is letters and digits.
func endpoint() string {
	if e := os.Getenv("OPENETL_TEST_DYNAMODB_ENDPOINT"); e != "" {
		return e
	}
	return "http://127.0.0.1:8000"
}

const closedPort = "http://127.0.0.1:1"

func testClient(t testing.TB) *dynamodb.Client {
	t.Helper()
	cl := dynamodb.New(dynamodb.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(endpoint()),
		Credentials:  credentials.NewStaticCredentialsProvider("openetl", "openetl", ""),
	})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if _, err := cl.ListTables(ctx, &dynamodb.ListTablesInput{Limit: aws.Int32(1)}); err != nil {
		t.Fatalf("DynamoDB at %s: %v", endpoint(), err)
	}
	return cl
}

// connConfig is the config's connection to the test endpoint, as JSON
// fields, with the access key given.
func connConfig(endpoint, key string) string {
	return fmt.Sprintf(`"region": "us-east-1", "endpoint": %q, "accessKeyId": %q, "secretAccessKey": "openetl"`, endpoint, key)
}

// prefix is a name prefix of the test's own.
func prefix(t testing.TB) string {
	t.Helper()
	const letters = "abcdefghijklmnopqrstuvwxyz"
	b := make([]byte, 6)
	for i := range b {
		b[i] = letters[rand.IntN(len(letters))]
	}
	p := "t" + string(b) + "_"
	cl := testClient(t)
	t.Cleanup(func() { dropPrefix(context.Background(), cl, p) })
	return p
}

func dropPrefix(ctx context.Context, cl *dynamodb.Client, p string) {
	pages := dynamodb.NewListTablesPaginator(cl, &dynamodb.ListTablesInput{})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return
		}
		for _, n := range page.TableNames {
			if strings.HasPrefix(n, p) {
				_, _ = cl.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(n)})
			}
		}
	}
}

// key is a key attribute: its name and type.
type key struct {
	name string
	typ  types.ScalarAttributeType
}

// createTable creates a table keyed on keys, the hash key first, with a
// stream of view if it isn't empty.
func createTable(t testing.TB, cl *dynamodb.Client, name string, view types.StreamViewType, keys ...key) {
	t.Helper()
	in := &dynamodb.CreateTableInput{TableName: aws.String(name), BillingMode: types.BillingModePayPerRequest}
	for i, k := range keys {
		kt := types.KeyTypeHash
		if i > 0 {
			kt = types.KeyTypeRange
		}
		in.KeySchema = append(in.KeySchema, types.KeySchemaElement{AttributeName: aws.String(k.name), KeyType: kt})
		in.AttributeDefinitions = append(in.AttributeDefinitions, types.AttributeDefinition{AttributeName: aws.String(k.name), AttributeType: k.typ})
	}
	if view != "" {
		in.StreamSpecification = &types.StreamSpecification{StreamEnabled: aws.Bool(true), StreamViewType: view}
	}
	if _, err := cl.CreateTable(t.Context(), in); err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
}

func put(t testing.TB, cl *dynamodb.Client, table string, items ...map[string]types.AttributeValue) {
	t.Helper()
	for _, it := range items {
		if _, err := cl.PutItem(t.Context(), &dynamodb.PutItemInput{TableName: aws.String(table), Item: it}); err != nil {
			t.Fatalf("put into %s: %v", table, err)
		}
	}
}

func avS(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func avN(v string) types.AttributeValue { return &types.AttributeValueMemberN{Value: v} }
func avI(v int) types.AttributeValue    { return &types.AttributeValueMemberN{Value: fmt.Sprint(v)} }

// readTable is a table's items, each attribute as Go spells it: numbers as
// json.Number, so they keep every digit; nil if the table isn't there.
func readTable(ctx context.Context, cl *dynamodb.Client, table string) ([]map[string]any, error) {
	var out []map[string]any
	pages := dynamodb.NewScanPaginator(cl, &dynamodb.ScanInput{TableName: aws.String(table), ConsistentRead: aws.Bool(true)})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if apiCode(err) == "ResourceNotFoundException" {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		for _, it := range page.Items {
			out = append(out, goItem(it))
		}
	}
	return out, nil
}

func goItem(it map[string]types.AttributeValue) map[string]any {
	m := map[string]any{}
	for k, v := range it {
		m[k] = goValue(v)
	}
	return m
}

func goValue(av types.AttributeValue) any {
	switch v := av.(type) {
	case *types.AttributeValueMemberS:
		return v.Value
	case *types.AttributeValueMemberN:
		return json.Number(v.Value)
	case *types.AttributeValueMemberB:
		return v.Value
	case *types.AttributeValueMemberBOOL:
		return v.Value
	case *types.AttributeValueMemberM:
		return goItem(v.Value)
	case *types.AttributeValueMemberL:
		out := make([]any, len(v.Value))
		for i, e := range v.Value {
			out[i] = goValue(e)
		}
		return out
	case *types.AttributeValueMemberSS:
		return v.Value
	case *types.AttributeValueMemberNS:
		out := make([]any, len(v.Value))
		for i, e := range v.Value {
			out[i] = json.Number(e)
		}
		return out
	case *types.AttributeValueMemberBS:
		return v.Value
	}
	return nil
}
