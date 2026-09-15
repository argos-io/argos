package synth

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Wire magic and frame types for the synthetic protocol.
const (
	magicGreeting = "SYN1"
	magicAck      = "ACK1"

	DefaultGreeting = "argos-synth/1"

	typeOpen   byte = 1
	typeData   byte = 2
	typeEnd    byte = 3
	typeStatus byte = 4

	flagOpenEnd byte = 1 << 0 // OPEN also ends the request (zero messages)
)

// Service and method full names used by tests and BindingFunc demos.
const (
	ServiceName     = "synth.v1.Synth"
	MethodPing      = "synth.v1.Synth.Ping"
	MethodEcho      = "synth.v1.Synth.Echo"
	MethodExclusive = "synth.v1.Synth.Exclusive"
)

var (
	errBadMagic      = errors.New("synth: bad handshake magic")
	errTruncated     = errors.New("synth: truncated frame")
	errUnknownType   = errors.New("synth: unknown frame type")
	errFrameTooLarge = errors.New("synth: frame too large")
)

type frame struct {
	typ     byte
	method  string // OPEN only — custom on-wire method field
	flags   byte
	data    []byte
	code    uint32
	message string
}

func writeGreeting(w io.Writer, greeting string) error {
	if len(greeting) > 0xffff {
		return fmt.Errorf("synth: greeting too long")
	}
	var hdr [6]byte
	copy(hdr[:4], magicGreeting)
	binary.BigEndian.PutUint16(hdr[4:], uint16(len(greeting)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write([]byte(greeting))
	return err
}

func readGreeting(r io.Reader) (string, error) {
	var hdr [6]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return "", err
	}
	if string(hdr[:4]) != magicGreeting {
		return "", errBadMagic
	}
	n := int(binary.BigEndian.Uint16(hdr[4:]))
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func writeAck(w io.Writer) error {
	_, err := io.WriteString(w, magicAck)
	return err
}

func readAck(r io.Reader) error {
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return err
	}
	if string(buf[:]) != magicAck {
		return errBadMagic
	}
	return nil
}

func writeFrame(w io.Writer, f frame) error {
	body, err := marshalBody(f)
	if err != nil {
		return err
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(body)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err = w.Write(body)
	return err
}

func marshalBody(f frame) ([]byte, error) {
	switch f.typ {
	case typeOpen:
		if len(f.method) > 0xffff {
			return nil, fmt.Errorf("synth: method too long")
		}
		out := make([]byte, 1+2+len(f.method)+1)
		out[0] = typeOpen
		binary.BigEndian.PutUint16(out[1:], uint16(len(f.method)))
		copy(out[3:], f.method)
		out[3+len(f.method)] = f.flags
		return out, nil
	case typeData:
		out := make([]byte, 1+len(f.data))
		out[0] = typeData
		copy(out[1:], f.data)
		return out, nil
	case typeEnd:
		return []byte{typeEnd}, nil
	case typeStatus:
		if len(f.message) > 0xffff {
			return nil, fmt.Errorf("synth: status message too long")
		}
		out := make([]byte, 1+4+2+len(f.message))
		out[0] = typeStatus
		binary.BigEndian.PutUint32(out[1:], f.code)
		binary.BigEndian.PutUint16(out[5:], uint16(len(f.message)))
		copy(out[7:], f.message)
		return out, nil
	default:
		return nil, errUnknownType
	}
}

func readFrame(r io.Reader, maxFrame int64) (frame, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return frame{}, err
	}
	n := int(binary.BigEndian.Uint32(hdr[:]))
	if n < 1 {
		return frame{}, errTruncated
	}
	if maxFrame > 0 && int64(n) > maxFrame {
		return frame{}, errFrameTooLarge
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return frame{}, err
	}
	return parseBody(body)
}

func parseBody(body []byte) (frame, error) {
	if len(body) < 1 {
		return frame{}, errTruncated
	}
	f := frame{typ: body[0]}
	switch f.typ {
	case typeOpen:
		if len(body) < 1+2+1 {
			return frame{}, errTruncated
		}
		mlen := int(binary.BigEndian.Uint16(body[1:3]))
		if len(body) < 3+mlen+1 {
			return frame{}, errTruncated
		}
		f.method = string(body[3 : 3+mlen])
		f.flags = body[3+mlen]
		return f, nil
	case typeData:
		f.data = append([]byte(nil), body[1:]...)
		return f, nil
	case typeEnd:
		return f, nil
	case typeStatus:
		if len(body) < 1+4+2 {
			return frame{}, errTruncated
		}
		f.code = binary.BigEndian.Uint32(body[1:5])
		mlen := int(binary.BigEndian.Uint16(body[5:7]))
		if len(body) < 7+mlen {
			return frame{}, errTruncated
		}
		f.message = string(body[7 : 7+mlen])
		return f, nil
	default:
		return frame{}, errUnknownType
	}
}
