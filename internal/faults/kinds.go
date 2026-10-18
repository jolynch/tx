package faults

import (
	"fmt"
	"strings"
)

// Family groups fault kinds by the IO they disturb. Each family has its own
// environment variable.
type Family uint8

const (
	FamilyNet Family = iota
	FamilyMsg
	FamilyDisk
	FamilySys
	familyCount
)

var familyNames = [familyCount]string{"NET", "MSG", "DISK", "SYS"}

func (f Family) String() string { return familyNames[f] }

// Kind is one fault an injector can fire, such as NET reset or SYS
// fdatasync.fail.
type Kind uint16

type kindInfo struct {
	family Family
	name   string
	// timed kinds take a beta duration: latencies, pauses, and delays.
	timed bool
}

// Kinds in the NET, MSG, and DISK families. SYS kinds follow, three per
// syscall, in syscallKind.
const (
	NetDelay Kind = iota
	NetPause
	NetReset
	NetPartial
	NetEOF
	NetFlip
	NetDrop
	NetDup
	NetReorder
	NetDialFail
	NetDialSlow
	NetAcceptFail
	MsgLoseResponse
	MsgDup
	MsgDelay
	MsgBusy
	DiskOpenENOENT
	DiskOpenEIO
	DiskReadEIO
	DiskReadLatency
	DiskReadFlip
	DiskWriteEIO
	DiskWriteENOSPC
	DiskWriteShort
	DiskWriteTorn
	DiskWriteLatency
	DiskWriteLost
	DiskWriteFlip
	sysKindBase
)

// A SYS kind is a syscall and one of these actions.
const (
	sysFail = iota
	sysLatency
	sysShort
	sysActions
)

var sysActionNames = [sysActions]string{"fail", "latency", "short"}

// kindCount is how many kinds exist: the fixed kinds, then three per syscall.
const kindCount = int(sysKindBase) + int(sysCount)*sysActions

// syscallKind returns the SYS kind for sc and an action.
func syscallKind(sc Syscall, action int) Kind {
	return sysKindBase + Kind(int(sc)*sysActions+action)
}

var kinds = func() [kindCount]kindInfo {
	var k [kindCount]kindInfo
	fixed := []struct {
		kind   Kind
		family Family
		name   string
		timed  bool
	}{
		{NetDelay, FamilyNet, "delay", true},
		{NetPause, FamilyNet, "pause", true},
		{NetReset, FamilyNet, "reset", false},
		{NetPartial, FamilyNet, "partial", false},
		{NetEOF, FamilyNet, "eof", false},
		{NetFlip, FamilyNet, "flip", false},
		{NetDrop, FamilyNet, "drop", false},
		{NetDup, FamilyNet, "dup", false},
		{NetReorder, FamilyNet, "reorder", false},
		{NetDialFail, FamilyNet, "dial.fail", false},
		{NetDialSlow, FamilyNet, "dial.slow", true},
		{NetAcceptFail, FamilyNet, "accept.fail", false},
		{MsgLoseResponse, FamilyMsg, "lose-response", false},
		{MsgDup, FamilyMsg, "dup", false},
		{MsgDelay, FamilyMsg, "delay", true},
		{MsgBusy, FamilyMsg, "busy", false},
		{DiskOpenENOENT, FamilyDisk, "open.enoent", false},
		{DiskOpenEIO, FamilyDisk, "open.eio", false},
		{DiskReadEIO, FamilyDisk, "read.eio", false},
		{DiskReadLatency, FamilyDisk, "read.latency", true},
		{DiskReadFlip, FamilyDisk, "read.flip", false},
		{DiskWriteEIO, FamilyDisk, "write.eio", false},
		{DiskWriteENOSPC, FamilyDisk, "write.enospc", false},
		{DiskWriteShort, FamilyDisk, "write.short", false},
		{DiskWriteTorn, FamilyDisk, "write.torn", false},
		{DiskWriteLatency, FamilyDisk, "write.latency", true},
		{DiskWriteLost, FamilyDisk, "write.lost", false},
		{DiskWriteFlip, FamilyDisk, "write.flip", false},
	}
	for _, f := range fixed {
		k[f.kind] = kindInfo{family: f.family, name: f.name, timed: f.timed}
	}
	for sc := Syscall(1); sc < sysCount; sc++ {
		for action := range sysActions {
			k[syscallKind(sc, action)] = kindInfo{
				family: FamilySys,
				name:   syscallNames[sc] + "." + sysActionNames[action],
				timed:  action == sysLatency,
			}
		}
	}
	return k
}()

func (k Kind) String() string {
	if int(k) >= kindCount || kinds[k].name == "" {
		return fmt.Sprintf("kind(%d)", int(k))
	}
	return strings.ToLower(kinds[k].family.String()) + "." + kinds[k].name
}

// Family returns the family k belongs to.
func (k Kind) Family() Family { return kinds[k].family }

// lookupKinds returns the kinds a spec entry's name selects in family f. In
// SYS, "*.action" selects that action of every syscall that supports it.
func lookupKinds(f Family, name string) ([]Kind, bool) {
	if f == FamilySys {
		sc, action, ok := strings.Cut(name, ".")
		if !ok {
			return nil, false
		}
		a := -1
		for i, n := range sysActionNames {
			if n == action {
				a = i
			}
		}
		if a < 0 {
			return nil, false
		}
		if a == sysShort && sc != "*" && !shortSyscalls[syscallByName(sc)] {
			return nil, false
		}
		if sc == "*" {
			var out []Kind
			for s := Syscall(1); s < sysCount; s++ {
				if a != sysShort || shortSyscalls[s] {
					out = append(out, syscallKind(s, a))
				}
			}
			return out, true
		}
		if s := syscallByName(sc); s != 0 {
			return []Kind{syscallKind(s, a)}, true
		}
		return nil, false
	}
	for k := range Kind(sysKindBase) {
		if kinds[k].family == f && kinds[k].name == name {
			return []Kind{k}, true
		}
	}
	return nil, false
}

func syscallByName(name string) Syscall {
	for sc := Syscall(1); sc < sysCount; sc++ {
		if syscallNames[sc] == name {
			return sc
		}
	}
	return 0
}
