package kinesis

import (
	"bytes"
	"crypto/md5"
	"errors"
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

// kplMagic starts a record the Kinesis Producer Library aggregated: the
// magic, an AggregatedRecord message, and the message's MD5.
var kplMagic = []byte{0xf3, 0x89, 0x9a, 0xc2}

// userRecord is one record of an aggregated record.
type userRecord struct {
	partitionKey string
	data         []byte
}

// aggregated says data is in the KPL's aggregated format, whole.
func aggregated(data []byte) bool {
	if len(data) < len(kplMagic)+md5.Size || !bytes.HasPrefix(data, kplMagic) {
		return false
	}
	body := data[len(kplMagic) : len(data)-md5.Size]
	sum := md5.Sum(body)
	return bytes.Equal(sum[:], data[len(data)-md5.Size:])
}

// deaggregate splits an aggregated record into its user records, each with
// its partition key from the record's table.
func deaggregate(data []byte) ([]userRecord, error) {
	body := data[len(kplMagic) : len(data)-md5.Size]
	var keys []string
	var subs []parsedRecord
	for len(body) > 0 {
		num, typ, n := protowire.ConsumeTag(body)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		body = body[n:]
		if typ != protowire.BytesType {
			if n = protowire.ConsumeFieldValue(num, typ, body); n < 0 {
				return nil, protowire.ParseError(n)
			}
			body = body[n:]
			continue
		}
		v, n := protowire.ConsumeBytes(body)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		body = body[n:]
		switch num {
		case 1:
			keys = append(keys, string(v))
		case 3:
			s, err := parseUserRecord(v)
			if err != nil {
				return nil, err
			}
			subs = append(subs, s)
		}
	}
	out := make([]userRecord, len(subs))
	for i, s := range subs {
		if s.key >= uint64(len(keys)) {
			return nil, fmt.Errorf("user record %d names partition key %d of %d", i, s.key, len(keys))
		}
		out[i] = userRecord{partitionKey: keys[s.key], data: s.data}
	}
	return out, nil
}

// parsedRecord is a user record with its partition key's index.
type parsedRecord struct {
	key  uint64
	data []byte
}

func parseUserRecord(b []byte) (parsedRecord, error) {
	var r parsedRecord
	haveKey := false
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return r, protowire.ParseError(n)
		}
		b = b[n:]
		switch {
		case num == 1 && typ == protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return r, protowire.ParseError(n)
			}
			r.key, haveKey, b = v, true, b[n:]
		case num == 3 && typ == protowire.BytesType:
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return r, protowire.ParseError(n)
			}
			r.data, b = v, b[n:]
		default:
			if n = protowire.ConsumeFieldValue(num, typ, b); n < 0 {
				return r, protowire.ParseError(n)
			}
			b = b[n:]
		}
	}
	if !haveKey {
		return r, errors.New("a user record without a partition key index")
	}
	return r, nil
}
