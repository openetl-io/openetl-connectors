package dynamodb

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"

	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/check/netcheck"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

var (
	_ source.CheckLister = (*Source)(nil)
	_ source.Preflighter = (*Source)(nil)
)

var (
	noRegion = check.Reason{
		Name: "region_missing", Code: protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "No AWS region is set.",
		Fix:     "Set region in the config, or AWS_REGION where the connector runs.",
	}
	noStream = check.Reason{
		Name: "stream_disabled", Code: protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "{table} has no DynamoDB stream enabled, which cdc reads.",
		Fix:     "aws dynamodb update-table --table-name {table} --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES",
	}
	noImages = check.Reason{
		Name: "stream_without_new_images", Code: protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "{table}'s stream has the view type {view}, without new images: each insert and update is read from the table as it is then, one consistent read per change.",
		Fix:     "Turn the stream off and on again with the view type NEW_AND_OLD_IMAGES, then re-sync the stream, since its position is lost.",
	}
	tooRare = check.Reason{
		Name: "syncs_beyond_retention", Code: protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "The pipeline goes up to {interval} between syncs, and {table}'s stream keeps changes for 24 hours.",
		Fix:     "Sync the stream at least every 20 hours, or continuously.",
	}
	noCursor = check.Reason{
		Name: "cursor_missing", Code: protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "{table} is read incrementally, which needs a cursor attribute.",
		Fix:     "Name the stream's cursor in the pipeline: a top-level attribute, a number or a string, whose values only grow, such as an update time.",
	}
)

// regionHint is the region the connector reaches, as far as the config and
// the environment say without reading files: the address checks need it
// before any check runs.
func (c *Connection) regionHint() string {
	for _, r := range []string{c.Region, os.Getenv("AWS_REGION"), os.Getenv("AWS_DEFAULT_REGION")} {
		if r != "" {
			return r
		}
	}
	return ""
}

// connectionChecks are the checks both roles start with: a region, the
// network path to DynamoDB, and signing in by listing tables. The last is
// named "dynamodb.auth".
func (c *Connection) connectionChecks() []check.Check {
	region := check.Check{
		Name: "aws.region", Kind: check.Configuration, Title: "Find the AWS region", Field: "region",
		Run: func(ctx context.Context, p *check.Probe) error {
			cl, err := c.connect(ctx, 1)
			if err != nil {
				if hint := c.regionHint(); hint == "" && c.Region == "" {
					return noRegion.Wrap(err)
				}
				return err
			}
			p.Param("region", cl.region)
			p.Found("The region is {region}.")
			return nil
		},
	}
	checks := []check.Check{region}
	needs := []string{"aws.region"}
	if addr, tls := c.address(c.regionHint()); addr != "" {
		net := netcheck.Reach(addr)
		if tls {
			net = netcheck.ReachTLS(addr, nil)
		}
		for i := range net {
			if c.Endpoint != "" {
				net[i].Field = "endpoint"
			} else {
				net[i].Field = "region"
			}
		}
		checks = append(checks, net...)
		needs = append(needs, net[len(net)-1].Name)
	}
	auth := check.Check{
		Name: "dynamodb.auth", Kind: check.Authentication, Title: "Sign in to DynamoDB and list its tables", Field: "accessKeyId",
		Needs: needs, Timeout: 20 * time.Second,
		Run: func(ctx context.Context, p *check.Probe) error {
			cl, err := c.connect(ctx, 2)
			if err != nil {
				return err
			}
			n := 0
			pages := dynamodb.NewListTablesPaginator(cl.db, &dynamodb.ListTablesInput{})
			for pages.HasMorePages() {
				page, err := pages.NextPage(ctx)
				if err != nil {
					return classify(fmt.Errorf("dynamodb: list tables: %w", err))
				}
				n += len(page.TableNames)
			}
			p.Param("tables", strconv.Itoa(n))
			p.Param("region", cl.region)
			p.Found("{region} has {tables} tables the credentials can list.")
			return nil
		},
	}
	return append(checks, auth)
}

// Checks reach DynamoDB, sign in, and find each table the config names and
// read an item of it.
func (s *Source) Checks() []check.Check {
	checks := s.cfg.connectionChecks()
	for _, name := range s.cfg.Tables {
		find := tableCheck(&s.cfg.Connection, name)
		find.Field = "tables"
		checks = append(checks, find, s.scanCheck(check.Name("table", name, "scan"), name, find.Name, "tables"))
	}
	return checks
}

// tableCheck finds a table, as a check named table.NAME.find.
func tableCheck(c *Connection, name string) check.Check {
	return check.Check{
		Name: check.Name("table", name, "find"), Kind: check.Resource, Title: "Find the table {table}",
		Params: check.Params{"table": name}, Needs: []string{"dynamodb.auth"},
		Run: func(ctx context.Context, p *check.Probe) error {
			cl, err := c.connect(ctx, 2)
			if err != nil {
				return err
			}
			_, err = cl.db.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(name)})
			if apiCode(err) == "ResourceNotFoundException" {
				return check.NotFound.Wrap(err, "resource", "The table "+name)
			}
			return classify(err)
		},
	}
}

// scanCheck reads an item of a table.
func (s *Source) scanCheck(name, tableName, need, field string) check.Check {
	return check.Check{
		Name: name, Kind: check.Permission, Title: "Scan {table}", Field: field,
		Params: check.Params{"table": tableName}, Needs: []string{need},
		Run: func(ctx context.Context, p *check.Probe) error {
			cl, err := s.cfg.connect(ctx, 2)
			if err != nil {
				return err
			}
			_, err = cl.db.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(tableName), Limit: aws.Int32(1)})
			if apiCode(err) == "ResourceNotFoundException" {
				return check.NotFound.Wrap(err, "resource", "The table "+tableName)
			}
			return classify(err)
		},
	}
}

// Preflight scans each stream's table, and for cdc, checks its stream is
// on, with new images, readable, and read often enough for its 24 hours.
func (s *Source) Preflight(req source.PreflightRequest) []check.Check {
	checks := s.cfg.connectionChecks()
	for _, cs := range req.Streams {
		checks = append(checks, s.scanCheck(check.Name("stream", cs.Name, "scan"), cs.Name, "dynamodb.auth", ""))
		switch cs.SyncMode {
		case source.Incremental:
			checks = append(checks, check.Check{
				Name: check.Name("stream", cs.Name, "cursor"), Kind: check.Configuration, Title: "Name {table}'s cursor",
				Params: check.Params{"table": cs.Name},
				Run: func(context.Context, *check.Probe) error {
					if cs.Cursor == "" {
						return noCursor.Fail()
					}
					return nil
				},
			})
		case source.CDC:
			checks = append(checks, s.streamChecks(cs, req)...)
		}
	}
	return checks
}

func (s *Source) streamChecks(cs source.ConfiguredStream, req source.PreflightRequest) []check.Check {
	params := check.Params{"table": cs.Name}
	enabled := check.Check{
		Name: check.Name("stream", cs.Name, "cdc"), Kind: check.Compatibility, Title: "Find {table}'s DynamoDB stream",
		Params: params, Needs: []string{"dynamodb.auth"},
		Run: func(ctx context.Context, p *check.Probe) error {
			cl, err := s.cfg.connect(ctx, 2)
			if err != nil {
				return err
			}
			t, err := describe(ctx, cl, cs.Name)
			if err != nil {
				return err
			}
			if t.streamARN == "" {
				return noStream.Fail()
			}
			p.Param("view", t.viewType)
			if t.viewType == string(streamtypes.StreamViewTypeKeysOnly) || t.viewType == string(streamtypes.StreamViewTypeOldImage) {
				return check.Warn(noImages.Fail())
			}
			p.Found("{table}'s stream has the view type {view}.")
			return nil
		},
	}
	// The stream's first shard, and an iterator at its oldest record.
	shardIterator := func(ctx context.Context) (*clients, *string, error) {
		cl, err := s.cfg.connect(ctx, 2)
		if err != nil {
			return nil, nil, err
		}
		t, err := describe(ctx, cl, cs.Name)
		if err != nil {
			return nil, nil, err
		}
		res, err := cl.streams.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{StreamArn: aws.String(t.streamARN), Limit: aws.Int32(1)})
		if err != nil {
			return nil, nil, classify(fmt.Errorf("dynamodb: describe %s's stream: %w", cs.Name, err))
		}
		if len(res.StreamDescription.Shards) == 0 {
			return cl, nil, check.Skip("{table}'s stream has no shards yet")
		}
		it, err := cl.streams.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
			StreamArn: aws.String(t.streamARN), ShardId: res.StreamDescription.Shards[0].ShardId, ShardIteratorType: streamtypes.ShardIteratorTypeTrimHorizon,
		})
		if err != nil {
			return nil, nil, classify(fmt.Errorf("dynamodb: an iterator of %s's stream: %w", cs.Name, err))
		}
		return cl, it.ShardIterator, nil
	}
	shards := check.Check{
		Name: check.Name("stream", cs.Name, "shards"), Kind: check.Permission, Title: "List {table}'s stream's shards and get an iterator",
		Params: params, Needs: []string{enabled.Name},
		Run: func(ctx context.Context, p *check.Probe) error {
			_, _, err := shardIterator(ctx)
			return err
		},
	}
	records := check.Check{
		Name: check.Name("stream", cs.Name, "records"), Kind: check.Permission, Title: "Read {table}'s stream's records",
		Params: params, Needs: []string{shards.Name},
		Run: func(ctx context.Context, p *check.Probe) error {
			cl, it, err := shardIterator(ctx)
			if err != nil {
				return err
			}
			_, err = cl.streams.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{ShardIterator: it, Limit: aws.Int32(1)})
			return classify(err)
		},
	}
	// Reads read an item where a stream has no new images, and where a
	// change has an attribute the run doesn't declare.
	items := check.Check{
		Name: check.Name("stream", cs.Name, "items"), Kind: check.Permission, Title: "Read an item of {table}",
		Params: params, Needs: []string{"dynamodb.auth"},
		Run: func(ctx context.Context, p *check.Probe) error {
			cl, err := s.cfg.connect(ctx, 2)
			if err != nil {
				return err
			}
			t, err := describe(ctx, cl, cs.Name)
			if err != nil {
				return err
			}
			key := map[string]types.AttributeValue{}
			for _, k := range t.keys {
				switch t.keyTypes[k] {
				case "N":
					key[k] = num("0")
				case "B":
					key[k] = &types.AttributeValueMemberB{Value: []byte{0}}
				default:
					key[k] = str("openetl-check")
				}
			}
			_, err = cl.db.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(cs.Name), Key: key})
			return classify(err)
		},
	}
	checks := []check.Check{enabled, shards, records, items}
	if !req.Continuous && req.Interval > 0 {
		interval := req.Interval
		checks = append(checks, check.Check{
			Name: check.Name("stream", cs.Name, "retention"), Kind: check.Configuration, Title: "Sync {table} within its stream's 24 hours",
			Params: check.Params{"table": cs.Name, "interval": interval.String()},
			Run: func(context.Context, *check.Probe) error {
				switch {
				case interval > retention:
					return tooRare.Fail()
				case interval > 20*time.Hour:
					return check.Warn(tooRare.Fail())
				}
				return nil
			},
		})
	}
	return checks
}

var (
	_ destination.CheckLister = (*Destination)(nil)
	_ destination.Preflighter = (*Destination)(nil)
)

var keyDiffers = check.Reason{
	Name: "table_key_differs", Code: protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
	Message: "{table} is keyed on {have}, and the stream needs {want}, which DynamoDB can't change.",
	Fix:     "aws dynamodb delete-table --table-name {table}, for the next sync to create it, or write the stream to another table.",
}

// Checks reach DynamoDB, sign in, and write and delete an item of the
// control table the destination stages in, where it's there; the first
// sync creates it otherwise.
func (d *Destination) Checks() []check.Check {
	control := d.cfg.TablePrefix + controlTable
	return append(d.cfg.connectionChecks(), check.Check{
		Name: "state.write", Kind: check.Permission, Title: "Write to {table}", Field: "tablePrefix",
		Params: check.Params{"table": control}, Needs: []string{"dynamodb.auth"},
		Run: func(ctx context.Context, p *check.Probe) error {
			cl, err := d.cfg.connect(ctx, 2)
			if err != nil {
				return err
			}
			_, err = cl.db.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(control)})
			if apiCode(err) == "ResourceNotFoundException" {
				return check.Skip("{table} isn't there yet: the first sync creates it, with dynamodb:CreateTable")
			}
			if err != nil {
				return classify(err)
			}
			key := map[string]types.AttributeValue{"p": str("check#" + hash(time.Now().String())), "n": str("-")}
			if _, err := cl.db.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(control), Item: key}); err != nil {
				return classify(err)
			}
			_, err = cl.db.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: aws.String(control), Key: key})
			return classify(err)
		},
	})
}

// Preflight checks each stream's table, where it's there, is keyed as the
// stream needs; one that isn't, the first sync creates.
func (d *Destination) Preflight(req destination.PreflightRequest) []check.Check {
	prefix := d.cfg.TablePrefix
	if req.Namespace != "" {
		prefix = req.Namespace + "."
	}
	checks := d.cfg.connectionChecks()
	for _, s := range req.Streams {
		schema := req.Schemas[s.Name]
		if schema == nil {
			schema = arrow.NewSchema(nil, nil)
		}
		table := prefix + tableName(schema, s.Name)
		want := keySchema(s, schema)
		checks = append(checks, check.Check{
			Name: check.Name("stream", s.Name, "table"), Kind: check.Resource, Title: "Find {table}, keyed as {stream} needs",
			Params: check.Params{"table": table, "stream": s.Name, "want": describeKey(want)}, Needs: []string{"dynamodb.auth"},
			Run: func(ctx context.Context, p *check.Probe) error {
				cl, err := d.cfg.connect(ctx, 2)
				if err != nil {
					return err
				}
				res, err := cl.db.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(table)})
				if apiCode(err) == "ResourceNotFoundException" {
					p.Found("{table} isn't there: the first sync creates it, keyed on {want}, with dynamodb:CreateTable.")
					return nil
				}
				if err != nil {
					return classify(err)
				}
				if keyMismatch(res.Table, want) != nil {
					p.Param("have", describeKey(tableKey(res.Table)))
					return keyDiffers.Fail()
				}
				p.Found("{table} is keyed on {want}.")
				return nil
			},
		})
	}
	return checks
}
