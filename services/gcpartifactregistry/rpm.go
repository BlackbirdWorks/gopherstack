package gcpartifactregistry

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	rpmLeadLen     = 96
	rpmHdrMagic    = 0x8eade801
	rpmIndexLen    = 16
	rpmMaxEntries  = 1 << 16
	rpmInt32Len    = 4
	rpmHdrPreamble = 16
	rpmAlign       = 8

	tagName        = 1000
	tagVersion     = 1001
	tagRelease     = 1002
	tagEpoch       = 1003
	tagSummary     = 1004
	tagDescription = 1005
	tagBuildTime   = 1006
	tagBuildHost   = 1007
	tagSize        = 1009
	tagVendor      = 1011
	tagLicense     = 1014
	tagPackager    = 1015
	tagGroup       = 1016
	tagURL         = 1020
	tagArch        = 1022
	tagSourceRPM   = 1044
	tagProvideName = 1047
	tagRequireName = 1049
	tagRequireFlag = 1048
	tagRequireVer  = 1050
	tagProvideFlag = 1112
	tagProvideVer  = 1113

	typeInt32       = 4
	typeString      = 6
	typeStringArray = 8
	typeI18N        = 9
)

var errNotRPM = errors.New("not an RPM package")

// rpmDep is one provides or requires entry.
type rpmDep struct {
	name  string
	ver   string
	flags uint32
}

// rpmMeta is the header data the yum metadata needs.
type rpmMeta struct {
	strs     map[int]string
	ints     map[int]uint32
	provides []rpmDep
	requires []rpmDep
	hdrStart int
	hdrEnd   int
}

type rpmIndexEntry struct {
	tag, typ, off, count int
}

// readRPMHeader parses one header structure at data[pos:], returning its entries, its store and the end offset.
func readRPMHeader(data []byte, pos int) ([]rpmIndexEntry, []byte, int, error) {
	if pos+rpmAlign+rpmAlign > len(data) || binary.BigEndian.Uint32(data[pos:]) != rpmHdrMagic {
		return nil, nil, 0, errNotRPM
	}

	n := int(binary.BigEndian.Uint32(data[pos+8:]))
	size := int(binary.BigEndian.Uint32(data[pos+12:]))
	idxStart := pos + rpmHdrPreamble

	if n < 0 || n > rpmMaxEntries || size < 0 || idxStart+n*rpmIndexLen+size > len(data) {
		return nil, nil, 0, errNotRPM
	}

	entries := make([]rpmIndexEntry, n)

	for i := range entries {
		o := idxStart + i*rpmIndexLen
		entries[i] = rpmIndexEntry{
			tag: int(binary.BigEndian.Uint32(data[o:])), typ: int(binary.BigEndian.Uint32(data[o+4:])),
			off: int(binary.BigEndian.Uint32(data[o+8:])), count: int(binary.BigEndian.Uint32(data[o+12:])),
		}
	}

	storeStart := idxStart + n*rpmIndexLen

	return entries, data[storeStart : storeStart+size], storeStart + size, nil
}

func cstring(store []byte, off int) (string, int) {
	if off < 0 || off >= len(store) {
		return "", off
	}

	end := bytes.IndexByte(store[off:], 0)
	if end < 0 {
		return string(store[off:]), len(store)
	}

	return string(store[off : off+end]), off + end + 1
}

// values decodes an index entry into its strings or int32s.
func values(e rpmIndexEntry, store []byte) ([]string, []uint32) {
	switch e.typ {
	case typeString, typeI18N, typeStringArray:
		strs := make([]string, 0, max(e.count, 1))
		off := e.off

		for range max(e.count, 1) {
			var s string

			s, off = cstring(store, off)
			strs = append(strs, s)
		}

		return strs, nil
	case typeInt32:
		ints := make([]uint32, 0, e.count)

		for i := range e.count {
			o := e.off + i*rpmInt32Len
			if o+rpmInt32Len > len(store) {
				break
			}

			ints = append(ints, binary.BigEndian.Uint32(store[o:]))
		}

		return nil, ints
	default:
		return nil, nil
	}
}

func zipDeps(names, vers []string, flags []uint32) []rpmDep {
	out := make([]rpmDep, 0, len(names))

	for i, n := range names {
		d := rpmDep{name: n}
		if i < len(vers) {
			d.ver = vers[i]
		}

		if i < len(flags) {
			d.flags = flags[i]
		}

		out = append(out, d)
	}

	return out
}

// parseRPM reads the main header of an RPM file.
func parseRPM(data []byte) (*rpmMeta, error) {
	if len(data) < rpmLeadLen || data[0] != 0xed || data[1] != 0xab || data[2] != 0xee || data[3] != 0xdb {
		return nil, errNotRPM
	}

	_, _, sigEnd, err := readRPMHeader(data, rpmLeadLen)
	if err != nil {
		return nil, fmt.Errorf("signature header: %w", err)
	}

	hdrStart := (sigEnd + rpmAlign - 1) / rpmAlign * rpmAlign

	entries, store, end, err := readRPMHeader(data, hdrStart)
	if err != nil {
		return nil, fmt.Errorf("main header: %w", err)
	}

	m := &rpmMeta{strs: map[int]string{}, ints: map[int]uint32{}, hdrStart: hdrStart, hdrEnd: end}

	var cols depColumns

	for _, e := range entries {
		ss, is := values(e, store)
		if !cols.take(e.tag, ss, is) {
			m.set(e.tag, ss, is)
		}
	}

	m.provides, m.requires = zipDeps(cols.provN, cols.provV, cols.provF), zipDeps(cols.reqN, cols.reqV, cols.reqF)

	if m.strs[tagName] == "" || m.strs[tagVersion] == "" || m.strs[tagArch] == "" {
		return nil, fmt.Errorf("%w: header lacks name, version or arch", errNotRPM)
	}

	return m, nil
}

// depColumns collects the parallel provides and requires arrays.
type depColumns struct {
	provN, provV, reqN, reqV []string
	provF, reqF              []uint32
}

// take stores a provides or requires column and reports whether the tag was one.
func (c *depColumns) take(tag int, ss []string, is []uint32) bool {
	switch tag {
	case tagProvideName:
		c.provN = ss
	case tagProvideVer:
		c.provV = ss
	case tagProvideFlag:
		c.provF = is
	case tagRequireName:
		c.reqN = ss
	case tagRequireVer:
		c.reqV = ss
	case tagRequireFlag:
		c.reqF = is
	default:
		return false
	}

	return true
}

func (m *rpmMeta) set(tag int, ss []string, is []uint32) {
	if len(ss) > 0 {
		m.strs[tag] = ss[0]
	}

	if len(is) > 0 {
		m.ints[tag] = is[0]
	}
}
