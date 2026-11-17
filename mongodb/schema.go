package mongodb

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

// ApplySchema creates each stream's collection. A collection has no column
// types, so a column takes any type, and a widened one lands beside the
// values it had, unless the collection's $jsonSchema validator, which only a
// user gives it, names the BSON types a field takes: then a column whose
// type it doesn't take is refused, with the collMod that adds it, or, where
// the stream allows type changes, given that collMod. A field the validator
// requires takes no null, which the writes leave out.
func (w *writer) ApplySchema(ctx context.Context, streams []destination.StreamSchema) (destination.SchemaApplied, error) {
	out := destination.SchemaApplied{Limits: map[string]destination.TableLimits{}}
	for _, s := range streams {
		target := targetCollection(s.Schema, s.Name)
		w.setTarget(s.Name, target)
		err := w.db.CreateCollection(ctx, target)
		if se, ok := errors.AsType[mongo.ServerError](err); err != nil && (!ok || !se.HasErrorCode(codeNamespaceExists)) {
			return out, classify(fmt.Errorf("mongodb: create %s: %w", target, err))
		}
		v, err := w.validator(ctx, target)
		if err != nil {
			return out, err
		}
		var limits destination.TableLimits
		for _, f := range s.Schema.Fields() {
			if v == nil {
				break
			}
			name := f.Name
			if strings.HasPrefix(name, "_openetl_") || name == "_id" {
				continue
			}
			if slices.Contains(v.required(), name) {
				limits.NotNull = append(limits.NotNull, name)
			}
			_, to, err := converter(f)
			if err != nil {
				return out, err
			}
			from := v.kinds(name)
			if to == "" || from == nil || accepts(from, to) {
				continue
			}
			fix, err := v.widen(name, to)
			if err != nil {
				return out, err
			}
			cmd := bson.D{{Key: "collMod", Value: target}, {Key: "validator", Value: fix}}
			if w.streams[s.Name].AllowTypeChanges {
				if err := w.db.RunCommand(ctx, cmd).Err(); err != nil {
					return out, classify(fmt.Errorf("mongodb: widen %s's validator for %s: %w", target, name, err))
				}
				if v, err = w.validator(ctx, target); err != nil {
					return out, err
				}
				continue
			}
			text, err := bson.MarshalExtJSON(cmd, false, false)
			if err != nil {
				return out, err
			}
			out.Refused = append(out.Refused, destination.RefusedColumn{
				Stream: s.Name, Column: name,
				Reason:   "the collection's $jsonSchema validator doesn't take the type in the field, and changing the validator is the user's",
				FromType: strings.Join(from, "|"), ToType: to,
				Fix: "db.runCommand(" + string(text) + ")",
			})
		}
		out.Limits[s.Name] = limits
	}
	return out, nil
}

// jsonSchema is a collection's validator that MongoDB enforces, as a
// document whose $jsonSchema names its fields' types.
type jsonSchema struct{ raw bson.Raw }

// validator is the collection's $jsonSchema validator; nil if it has none,
// or one that only warns or isn't on.
func (w *writer) validator(ctx context.Context, coll string) (*jsonSchema, error) {
	specs, err := w.db.ListCollectionSpecifications(ctx, bson.D{{Key: "name", Value: coll}})
	if err != nil {
		return nil, classify(fmt.Errorf("mongodb: list collections of %s: %w", w.db.Name(), err))
	}
	if len(specs) == 0 {
		return nil, nil
	}
	return enforced(specs[0].Options), nil
}

// enforced is the $jsonSchema validator among a collection's options o; nil
// if it has none, or one that only warns or isn't on.
func enforced(o bson.Raw) *jsonSchema {
	level, _ := o.Lookup("validationLevel").StringValueOK()
	action, _ := o.Lookup("validationAction").StringValueOK()
	v, ok := o.Lookup("validator").DocumentOK()
	if !ok || level == "off" || action == "warn" {
		return nil
	}
	if _, ok := v.Lookup("$jsonSchema").DocumentOK(); !ok {
		return nil
	}
	return &jsonSchema{v}
}

// closed is whether the validator takes no field its properties don't name,
// and the fields they name; false where patternProperties may take others.
func (v *jsonSchema) closed() (bool, []string) {
	open, ok := v.raw.Lookup("$jsonSchema", "additionalProperties").BooleanOK()
	if _, patterns := v.raw.Lookup("$jsonSchema", "patternProperties").DocumentOK(); !ok || open || patterns {
		return false, nil
	}
	props, _ := v.raw.Lookup("$jsonSchema", "properties").DocumentOK()
	elems, _ := props.Elements()
	names := make([]string, len(elems))
	for i, e := range elems {
		names[i] = e.Key()
	}
	return true, names
}

func (v *jsonSchema) required() []string {
	arr, _ := v.raw.Lookup("$jsonSchema", "required").ArrayOK()
	vals, _ := arr.Values()
	var out []string
	for _, x := range vals {
		if s, ok := x.StringValueOK(); ok {
			out = append(out, s)
		}
	}
	return out
}

// kinds are the BSON types the validator takes in a field, by bsonType or
// by JSON Schema's type; nil for any.
func (v *jsonSchema) kinds(field string) []string {
	p, ok := v.raw.Lookup("$jsonSchema", "properties", field).DocumentOK()
	if !ok {
		return nil
	}
	var out []string
	for _, key := range []string{"bsonType", "type"} {
		t := p.Lookup(key)
		if s, ok := t.StringValueOK(); ok {
			out = append(out, s)
		}
		if arr, ok := t.ArrayOK(); ok {
			vals, _ := arr.Values()
			for _, x := range vals {
				if s, ok := x.StringValueOK(); ok {
					out = append(out, s)
				}
			}
		}
	}
	for i, k := range out {
		if k == "boolean" {
			out[i] = "bool"
		}
	}
	return out
}

func accepts(kinds []string, t string) bool {
	if slices.Contains(kinds, t) {
		return true
	}
	return slices.Contains(kinds, "number") && slices.Contains([]string{"int", "long", "double", "decimal"}, t)
}

// widen is the validator with the field's bsonType taking t too.
func (v *jsonSchema) widen(field, t string) (bson.D, error) {
	var d bson.D
	if err := bson.Unmarshal(v.raw, &d); err != nil {
		return nil, err
	}
	js, _ := get(d, "$jsonSchema").(bson.D)
	props, _ := get(js, "properties").(bson.D)
	p, _ := get(props, field).(bson.D)
	kinds := bson.A{}
	for _, k := range append(v.kinds(field), t) {
		kinds = append(kinds, k)
	}
	p = slices.DeleteFunc(p, func(e bson.E) bool { return e.Key == "type" })
	p = set(p, "bsonType", kinds)
	return set(d, "$jsonSchema", set(js, "properties", set(props, field, p))), nil
}

func get(d bson.D, key string) any {
	for _, e := range d {
		if e.Key == key {
			return e.Value
		}
	}
	return nil
}

func set(d bson.D, key string, v any) bson.D {
	for i, e := range d {
		if e.Key == key {
			d[i].Value = v
			return d
		}
	}
	return append(d, bson.E{Key: key, Value: v})
}
