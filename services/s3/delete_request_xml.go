package s3

import (
	"bytes"
	"encoding/xml"
	"io"
	"strconv"
	"strings"
)

// parseDeleteRequestFast handles the plain shape SDKs emit (no entities, CDATA, comments or non-ASCII
// text). ok is false for anything else so the caller falls back to encoding/xml.
func parseDeleteRequestFast(b []byte) (DeleteRequest, bool) {
	var req DeleteRequest

	p := fastXML{b: b}
	if !p.rootOpen() {
		return req, false
	}

	req.XMLName = xml.Name{Local: "Delete"}

	for {
		p.skipSpace()

		switch {
		case p.hasPrefix("</Delete>"):
			p.pos += len("</Delete>")
			p.skipSpace()

			return req, p.pos == len(b)
		case p.hasPrefix("<Object>"):
			p.pos += len("<Object>")

			obj, ok := p.object()
			if !ok {
				return req, false
			}

			req.Objects = append(req.Objects, obj)
		case p.hasPrefix("<Quiet>"):
			p.pos += len("<Quiet>")

			quiet, ok := p.quiet()
			if !ok {
				return req, false
			}

			req.Quiet = quiet
		default:
			return req, false
		}
	}
}

type fastXML struct {
	b   []byte
	pos int
}

func (p *fastXML) hasPrefix(s string) bool {
	return len(p.b)-p.pos >= len(s) && string(p.b[p.pos:p.pos+len(s)]) == s
}

func (p *fastXML) skipSpace() {
	for p.pos < len(p.b) {
		switch p.b[p.pos] {
		case ' ', '\t', '\r', '\n':
			p.pos++
		default:
			return
		}
	}
}

func (p *fastXML) skipPast(s string) bool {
	i := strings.Index(string(p.b[p.pos:]), s)
	if i < 0 {
		return false
	}

	p.pos += i + len(s)

	return true
}

// openTagEnd consumes the rest of a start tag holding at most plain attributes and rejects empty-element tags.
func (p *fastXML) openTagEnd() bool {
	for p.pos < len(p.b) {
		c := p.b[p.pos]
		p.pos++

		switch c {
		case '>':
			return p.b[p.pos-2] != '/'
		case '<', '&':
			return false
		}
	}

	return false
}

// text returns the plain text up to end and consumes end.
func (p *fastXML) text(end string) (string, bool) {
	start := p.pos

	for p.pos < len(p.b) {
		c := p.b[p.pos]

		switch {
		case c == '<':
			if !p.hasPrefix(end) {
				return "", false
			}

			s := string(p.b[start:p.pos])
			p.pos += len(end)

			return s, true
		case c == '&' || c >= utf8Start || (c < ' ' && c != '\t' && c != '\n' && c != '\r'):
			return "", false
		}

		p.pos++
	}

	return "", false
}

const utf8Start = 0x80

func (p *fastXML) object() (DeleteObject, bool) {
	var obj DeleteObject

	for {
		p.skipSpace()

		switch {
		case p.hasPrefix("</Object>"):
			p.pos += len("</Object>")

			return obj, true
		case p.hasPrefix("<Key>"):
			p.pos += len("<Key>")

			k, ok := p.text("</Key>")
			if !ok {
				return obj, false
			}

			obj.Key = k
		case p.hasPrefix("<VersionId>"):
			p.pos += len("<VersionId>")

			v, ok := p.text("</VersionId>")
			if !ok {
				return obj, false
			}

			obj.VersionID = &v
		default:
			return obj, false
		}
	}
}

func decodeDeleteRequest(body io.Reader) (DeleteRequest, error) {
	raw, err := io.ReadAll(body)
	if err != nil {
		return DeleteRequest{}, err
	}

	if req, ok := parseDeleteRequestFast(raw); ok {
		return req, nil
	}

	var req DeleteRequest

	err = xml.NewDecoder(bytes.NewReader(raw)).Decode(&req)

	return req, err
}

// rootOpen consumes an optional XML declaration and the <Delete ...> start tag.
func (p *fastXML) rootOpen() bool {
	p.skipSpace()

	if p.hasPrefix("<?xml") {
		if !p.skipPast("?>") {
			return false
		}

		p.skipSpace()
	}

	if !p.hasPrefix("<Delete") {
		return false
	}

	p.pos += len("<Delete")

	if p.pos >= len(p.b) || (p.b[p.pos] != '>' && p.b[p.pos] != ' ') {
		return false
	}

	return p.openTagEnd()
}

func (p *fastXML) quiet() (bool, bool) {
	text, ok := p.text("</Quiet>")
	if !ok {
		return false, false
	}

	if text == "" {
		return false, true
	}

	q, err := strconv.ParseBool(strings.TrimSpace(text))

	return q, err == nil
}
