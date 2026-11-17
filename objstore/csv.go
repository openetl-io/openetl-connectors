package objstore

import (
	"bufio"
	"errors"
	"fmt"
	"io"
)

// csvReader reads CSV records, as RFC 4180 has them with a delimiter and
// quote of one's choice, and says which fields were quoted, so a quoted
// empty field can be an empty text and an unquoted one a null.
type csvReader struct {
	r            *bufio.Reader
	delim, quote byte
	// The bytes read so far, up to the end of the last record.
	offset int64
}

func newCSVReader(r io.Reader, delim, quote byte) *csvReader {
	return &csvReader{r: bufio.NewReaderSize(r, 1<<16), delim: delim, quote: quote}
}

// read returns the next record, or io.EOF. A blank line is a record of one
// empty field.
func (c *csvReader) read() (fields []string, quoted []bool, err error) {
	var field []byte
	inQuotes, wasQuoted, started := false, false, false
	end := func() {
		fields = append(fields, string(field))
		quoted = append(quoted, wasQuoted)
		field, wasQuoted = field[:0], false
	}
	for {
		b, err := c.r.ReadByte()
		if err == io.EOF {
			switch {
			case inQuotes:
				return nil, nil, errors.New("a quoted field doesn't end")
			case !started:
				return nil, nil, io.EOF
			}
			end()
			return fields, quoted, nil
		}
		if err != nil {
			return nil, nil, err
		}
		c.offset++
		started = true
		switch {
		case inQuotes && b == c.quote:
			next, err := c.r.Peek(1)
			if err == nil && next[0] == c.quote {
				_, _ = c.r.ReadByte()
				c.offset++
				field = append(field, c.quote)
				continue
			}
			inQuotes = false
		case inQuotes:
			field = append(field, b)
		case b == c.quote && len(field) == 0 && !wasQuoted:
			inQuotes, wasQuoted = true, true
		case b == c.delim:
			end()
		case b == '\n':
			end()
			return fields, quoted, nil
		case b == '\r':
			if next, err := c.r.Peek(1); err == nil && next[0] == '\n' {
				_, _ = c.r.ReadByte()
				c.offset++
			}
			end()
			return fields, quoted, nil
		case wasQuoted:
			return nil, nil, fmt.Errorf("%q follows a quoted field", b)
		default:
			field = append(field, b)
		}
	}
}
