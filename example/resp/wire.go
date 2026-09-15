package resp

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
)

// errNeedMore means the buffer does not yet hold a complete RESP value.
var errNeedMore = errors.New("resp: need more data")

// Value is one decoded RESP2 value.
type Value struct {
	Type  byte // '+', '-', ':', '$', '*'
	Str   string
	Int   int64
	Array []Value
	Null  bool // bulk null ($-1) or array null (*-1)
}

// EncodeSimple encodes a RESP simple string (+...).
func EncodeSimple(s string) []byte {
	return []byte("+" + s + "\r\n")
}

// EncodeError encodes a RESP error (-...).
func EncodeError(msg string) []byte {
	return []byte("-" + msg + "\r\n")
}

// EncodeBulk encodes a RESP bulk string. EncodeBulkNull encodes a null bulk.
func EncodeBulk(s string) []byte {
	return []byte("$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n")
}

// EncodeBulkNull encodes $-1\r\n.
func EncodeBulkNull() []byte {
	return []byte("$-1\r\n")
}

// EncodeArray encodes a RESP array of bulk strings (typical Redis command/args).
func EncodeArray(elems ...string) []byte {
	var b bytes.Buffer
	b.WriteByte('*')
	b.WriteString(strconv.Itoa(len(elems)))
	b.WriteString("\r\n")
	for _, e := range elems {
		b.Write(EncodeBulk(e))
	}
	return b.Bytes()
}

// EncodeArgs is an alias for EncodeArray — request payloads carry command args
// only; Framing prepends the method name as the Redis command.
func EncodeArgs(args ...string) []byte {
	return EncodeArray(args...)
}

// DecodeArgs parses a RESP array of bulk strings into Go strings.
func DecodeArgs(b []byte) ([]string, error) {
	v, n, err := parseValue(b)
	if err != nil {
		return nil, err
	}
	if n != len(b) {
		return nil, fmt.Errorf("resp: trailing bytes after args array")
	}
	if v.Type != '*' || v.Null {
		return nil, fmt.Errorf("resp: args must be a RESP array")
	}
	out := make([]string, len(v.Array))
	for i, el := range v.Array {
		if el.Type != '$' || el.Null {
			return nil, fmt.Errorf("resp: arg %d must be a bulk string", i)
		}
		out[i] = el.Str
	}
	return out, nil
}

// parseValue parses one RESP value from b. It returns errNeedMore if incomplete.
func parseValue(b []byte) (Value, int, error) {
	if len(b) == 0 {
		return Value{}, 0, errNeedMore
	}
	switch b[0] {
	case '+', '-', ':':
		line, n, err := readLine(b)
		if err != nil {
			return Value{}, 0, err
		}
		body := string(line[1:])
		switch b[0] {
		case '+':
			return Value{Type: '+', Str: body}, n, nil
		case '-':
			return Value{Type: '-', Str: body}, n, nil
		default:
			i, err := strconv.ParseInt(body, 10, 64)
			if err != nil {
				return Value{}, 0, fmt.Errorf("resp: bad integer %q", body)
			}
			return Value{Type: ':', Int: i}, n, nil
		}
	case '$':
		line, n, err := readLine(b)
		if err != nil {
			return Value{}, 0, err
		}
		size, err := strconv.Atoi(string(line[1:]))
		if err != nil {
			return Value{}, 0, fmt.Errorf("resp: bad bulk length: %w", err)
		}
		if size < 0 {
			return Value{Type: '$', Null: true}, n, nil
		}
		if len(b) < n+size+2 {
			return Value{}, 0, errNeedMore
		}
		if b[n+size] != '\r' || b[n+size+1] != '\n' {
			return Value{}, 0, fmt.Errorf("resp: bulk missing CRLF")
		}
		return Value{Type: '$', Str: string(b[n : n+size])}, n + size + 2, nil
	case '*':
		line, n, err := readLine(b)
		if err != nil {
			return Value{}, 0, err
		}
		count, err := strconv.Atoi(string(line[1:]))
		if err != nil {
			return Value{}, 0, fmt.Errorf("resp: bad array length: %w", err)
		}
		if count < 0 {
			return Value{Type: '*', Null: true}, n, nil
		}
		off := n
		elems := make([]Value, 0, count)
		for i := 0; i < count; i++ {
			el, m, err := parseValue(b[off:])
			if err != nil {
				return Value{}, 0, err
			}
			elems = append(elems, el)
			off += m
		}
		return Value{Type: '*', Array: elems}, off, nil
	default:
		return Value{}, 0, fmt.Errorf("resp: unknown type byte %q", b[0])
	}
}

func readLine(b []byte) (line []byte, n int, err error) {
	i := bytes.Index(b, []byte("\r\n"))
	if i < 0 {
		return nil, 0, errNeedMore
	}
	return b[:i], i + 2, nil
}

// MarshalValue re-encodes a Value (used for forwarding replies).
func MarshalValue(v Value) []byte {
	switch v.Type {
	case '+':
		return EncodeSimple(v.Str)
	case '-':
		return EncodeError(v.Str)
	case ':':
		return []byte(":" + strconv.FormatInt(v.Int, 10) + "\r\n")
	case '$':
		if v.Null {
			return EncodeBulkNull()
		}
		return EncodeBulk(v.Str)
	case '*':
		if v.Null {
			return []byte("*-1\r\n")
		}
		var b bytes.Buffer
		b.WriteByte('*')
		b.WriteString(strconv.Itoa(len(v.Array)))
		b.WriteString("\r\n")
		for _, el := range v.Array {
			b.Write(MarshalValue(el))
		}
		return b.Bytes()
	default:
		return EncodeError("unknown value type")
	}
}
