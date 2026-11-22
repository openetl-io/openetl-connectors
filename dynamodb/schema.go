package dynamodb

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

// ApplySchema creates each stream's table, billed on demand, keyed as
// keySchema says. An item has no column types, so a column takes any type,
// and a widened one lands beside the values it had, except the attributes a
// table or an index is keyed on, whose type DynamoDB fixes: a table keyed
// on other attributes than its stream fails, and a column the table or an
// index is keyed on whose type changes is refused.
func (w *writer) ApplySchema(ctx context.Context, streams []destination.StreamSchema) (destination.SchemaApplied, error) {
	out := destination.SchemaApplied{Limits: map[string]destination.TableLimits{}}
	for _, s := range streams {
		target := w.prefix + tableName(s.Schema, s.Name)
		if err := validName(target); err != nil {
			return out, configInvalid(err)
		}
		w.setTarget(s.Name, target)
		want := keySchema(w.streams[s.Name], s.Schema)
		if err := w.ensureTable(ctx, target, want); err != nil {
			return out, err
		}
		d, err := w.db.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(target)})
		if err != nil {
			return out, classify(fmt.Errorf("dynamodb: describe %s: %w", target, err))
		}
		if err := keyMismatch(d.Table, want); err != nil {
			return out, configInvalid(fmt.Errorf("dynamodb: stream %q: %w", s.Name, err))
		}
		refused, limits := columnLimits(d.Table, s.Name, s.Schema)
		out.Refused = append(out.Refused, refused...)
		out.Limits[s.Name] = limits
	}
	return out, nil
}

func describeKey(keys []keyAttr) string {
	var parts []string
	for _, k := range keys {
		parts = append(parts, k.name+" ("+string(k.typ)+")")
	}
	return strings.Join(parts, ", ")
}

// tableKey is the key a table has.
func tableKey(t *types.TableDescription) []keyAttr {
	defs := map[string]types.ScalarAttributeType{}
	for _, a := range t.AttributeDefinitions {
		defs[aws.ToString(a.AttributeName)] = a.AttributeType
	}
	var out []keyAttr
	for _, kt := range []types.KeyType{types.KeyTypeHash, types.KeyTypeRange} {
		for _, k := range t.KeySchema {
			if k.KeyType == kt {
				out = append(out, keyAttr{aws.ToString(k.AttributeName), defs[aws.ToString(k.AttributeName)]})
			}
		}
	}
	return out
}

// keyMismatch says how a table's key differs from the one its stream
// needs, but in its attributes' types, which columnLimits refuses; nil if
// it doesn't.
func keyMismatch(t *types.TableDescription, want []keyAttr) error {
	have := tableKey(t)
	if slices.EqualFunc(have, want, func(a, b keyAttr) bool { return a.name == b.name }) {
		return nil
	}
	return fmt.Errorf("table %s is keyed on %s, and its stream needs %s, which DynamoDB can't change: delete the table, for the next sync to create it, or write the stream to another table",
		aws.ToString(t.TableName), describeKey(have), describeKey(want))
}

// columnLimits are the columns of schema whose attributes an index of t is
// keyed on with another type, refused, and what t can't take: nulls and
// holds in its key's attributes, and type changes in any index's.
func columnLimits(t *types.TableDescription, stream string, schema *arrow.Schema) ([]destination.RefusedColumn, destination.TableLimits) {
	var limits destination.TableLimits
	var refused []destination.RefusedColumn
	keys := map[string]bool{}
	for _, k := range tableKey(t) {
		keys[k.name] = true
	}
	indexes := map[string]string{}
	for _, g := range t.GlobalSecondaryIndexes {
		for _, k := range g.KeySchema {
			indexes[aws.ToString(k.AttributeName)] = aws.ToString(g.IndexName)
		}
	}
	for _, l := range t.LocalSecondaryIndexes {
		for _, k := range l.KeySchema {
			if name := aws.ToString(k.AttributeName); !keys[name] {
				indexes[name] = aws.ToString(l.IndexName)
			}
		}
	}
	for _, a := range t.AttributeDefinitions {
		name := aws.ToString(a.AttributeName)
		i := index(schema, name)
		if i < 0 {
			continue
		}
		limits.Fixed = append(limits.Fixed, name)
		if keys[name] {
			limits.NotNull = append(limits.NotNull, name)
			limits.Unholdable = append(limits.Unholdable, name)
		}
		to := scalarType(schema.Field(i))
		if to == a.AttributeType {
			continue
		}
		if keys[name] {
			refused = append(refused, destination.RefusedColumn{
				Stream: stream, Column: name, FromType: string(a.AttributeType), ToType: string(to),
				Reason: fmt.Sprintf("the table is keyed on it as %s, which DynamoDB can't change", a.AttributeType),
				Fix:    fmt.Sprintf("aws dynamodb delete-table --table-name %s, for the next sync to create it keyed on %s as %s", aws.ToString(t.TableName), name, to),
			})
			continue
		}
		rc := destination.RefusedColumn{
			Stream: stream, Column: name, FromType: string(a.AttributeType), ToType: string(to),
			Reason: fmt.Sprintf("the index %s is keyed on it as %s, and DynamoDB doesn't write an item whose value there has another type", indexes[name], a.AttributeType),
		}
		if slices.ContainsFunc(t.GlobalSecondaryIndexes, func(g types.GlobalSecondaryIndexDescription) bool { return aws.ToString(g.IndexName) == indexes[name] }) {
			rc.Fix = fmt.Sprintf(`aws dynamodb update-table --table-name %s --global-secondary-index-updates '[{"Delete":{"IndexName":"%s"}}]', then create the index again with %s as %s`,
				aws.ToString(t.TableName), indexes[name], name, to)
		} else {
			rc.Fix = fmt.Sprintf("a local secondary index can't change: aws dynamodb delete-table --table-name %s, then create it again with %s keyed on %s as %s, or let the next sync create it without the index",
				aws.ToString(t.TableName), indexes[name], name, to)
		}
		refused = append(refused, rc)
	}
	return refused, limits
}
