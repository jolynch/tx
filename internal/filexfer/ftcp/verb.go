package ftcp

import (
	"fmt"
	"strings"
)

type Verb uint8

const (
	VerbUnknown Verb = iota
	VerbAUTH
	VerbTXFER
	VerbSEND
	VerbACK
	VerbCXSUM
	VerbSTATUS
	VerbPROBE
	VerbSYNC
)

func ParseVerb(token string) (Verb, error) {
	switch strings.ToUpper(strings.TrimSpace(token)) {
	case "AUTH":
		return VerbAUTH, nil
	case "TXFER":
		return VerbTXFER, nil
	case "SEND":
		return VerbSEND, nil
	case "ACK":
		return VerbACK, nil
	case "CXSUM":
		return VerbCXSUM, nil
	case "STATUS":
		return VerbSTATUS, nil
	case "PROBE":
		return VerbPROBE, nil
	case "SYNC":
		return VerbSYNC, nil
	default:
		return VerbUnknown, fmt.Errorf("unknown verb: %s", token)
	}
}

// String returns the wire spelling of v.
func (v Verb) String() string {
	switch v {
	case VerbAUTH:
		return "AUTH"
	case VerbTXFER:
		return "TXFER"
	case VerbSEND:
		return "SEND"
	case VerbACK:
		return "ACK"
	case VerbCXSUM:
		return "CXSUM"
	case VerbSTATUS:
		return "STATUS"
	case VerbPROBE:
		return "PROBE"
	case VerbSYNC:
		return "SYNC"
	}
	return "UNKNOWN"
}
