package sparql

import (
	"bytes"
	"encoding/binary"
)

const (
	brtrMagic    = "BRTR"
	brtrVersion  = 3
	brtrNull     = 0
	brtrRepeat   = 1
	brtrURI      = 4
	brtrBNode    = 5
	brtrPlain    = 6
	brtrLang     = 7
	brtrDatatype = 8
	brtrEmptyRow = 9
	brtrTableEnd = 127
	brtrIntMask  = 0xFFFFFFFF
)

func brtrString(buf *bytes.Buffer, s string) {
	buf.Write(binary.BigEndian.AppendUint32(nil, uint32(len(s)&brtrIntMask)))
	buf.WriteString(s)
}

func brtrTerm(buf *bytes.Buffer, t Term) {
	switch t.Kind {
	case KindIRI:
		buf.WriteByte(brtrURI)
		brtrString(buf, t.Value)
	case KindBlank:
		buf.WriteByte(brtrBNode)
		brtrString(buf, t.Value)
	case KindNone:
		buf.WriteByte(brtrNull)
	case KindLiteral:
		switch {
		case t.Lang != "":
			buf.WriteByte(brtrLang)
			brtrString(buf, t.Value)
			brtrString(buf, t.Lang)
		case t.Datatype != "":
			buf.WriteByte(brtrDatatype)
			brtrString(buf, t.Value)
			buf.WriteByte(brtrURI)
			brtrString(buf, t.Datatype)
		default:
			buf.WriteByte(brtrPlain)
			brtrString(buf, t.Value)
		}
	}
}

// writeSelectBinary renders the RDF4J binary table result format, version 3 (IRIs are written in full, never as
// namespace/qname pairs).
func writeSelectBinary(r *Result) []byte {
	var buf bytes.Buffer

	buf.WriteString(brtrMagic)
	buf.Write(binary.BigEndian.AppendUint32(nil, brtrVersion))
	buf.Write(binary.BigEndian.AppendUint32(nil, uint32(len(r.Vars)&brtrIntMask)))

	for _, v := range r.Vars {
		brtrString(&buf, v)
	}

	prev := make([]Term, len(r.Vars))

	for _, row := range r.Rows {
		if len(r.Vars) == 0 {
			buf.WriteByte(brtrEmptyRow)

			continue
		}

		for i, v := range r.Vars {
			t := row[v]

			if t.Bound() && t == prev[i] {
				buf.WriteByte(brtrRepeat)

				continue
			}

			brtrTerm(&buf, t)
			prev[i] = t
		}
	}

	buf.WriteByte(brtrTableEnd)

	return buf.Bytes()
}
