package appsync

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strings"

	"golang.org/x/text/unicode/norm"

	"github.com/blackbirdworks/gopherstack/pkgs/ctxval"
)

const (
	s3ObjectArgs = 3
	s3RegionArg  = 2
)

// respHeaderKey carries the sink that util.http.addResponseHeader(s) writes to.
//
//nolint:gochecknoglobals // ctxval.Key is a singleton by design (see pkgs/ctxval).
var respHeaderKey = ctxval.NewKey[func(name, value string)]("appsync-response-header")

// copyHeadersDenied reports the headers util.http.copyHeaders drops, as AppSync does.
func copyHeadersDenied(name string) bool {
	switch strings.ToLower(name) {
	case "connection", "content-length", "content-type", "content-encoding", "host", "transfer-encoding",
		"upgrade", "via", "x-forwarded-for", "x-forwarded-port", "x-forwarded-proto", "x-amz-cf-id",
		"x-amz-date", "x-amz-security-token", "x-amz-user-agent", "x-amzn-trace-id", "x-api-key",
		"authorization", "user-agent":
		return true
	}

	return false
}

func utilExtra() map[string]utilFn {
	return map[string]utilFn{
		"dynamodb.toS3Object":     func(_ *utilEnv, a []any) (any, error) { return s3Object(a) },
		"dynamodb.toS3ObjectJson": s3ObjectJSON,
		"dynamodb.fromS3ObjectJson": func(_ *utilEnv, a []any) (any, error) {
			return fromS3ObjectJSON(argStr(a, 0))
		},
		"str.normalize":    normalizeStr,
		"xml.toMap":        xmlToMap,
		"xml.toJsonString": xmlToJSONString,
		"http.copyHeaders": copyHeaders,
		"http.addResponseHeader": func(u *utilEnv, a []any) (any, error) {
			u.addHeader(argStr(a, 0), argStr(a, 1))

			return "", nil
		},
		"http.addResponseHeaders": func(u *utilEnv, a []any) (any, error) {
			m, _ := argAt(a, 0).(map[string]any)
			for _, k := range sortedKeys(m) {
				u.addHeader(k, vrender(m[k]))
			}

			return "", nil
		},
		"transform.toSubscriptionFilter": toSubscriptionFilter,
	}
}

func (u *utilEnv) addHeader(name, value string) {
	if u.headerSink != nil && name != "" {
		u.headerSink(name, value)
	}
}

type s3Ref struct {
	Key     string `json:"key"`
	Bucket  string `json:"bucket"`
	Region  string `json:"region"`
	Version string `json:"version,omitempty"`
}

func s3Object(a []any) (any, error) {
	if len(a) < s3ObjectArgs {
		return nil, fmt.Errorf("%w: toS3Object requires key, bucket and region", ErrValidation)
	}

	ref := s3Ref{Key: argStr(a, 0), Bucket: argStr(a, 1), Region: argStr(a, s3RegionArg)}
	if len(a) > s3ObjectArgs {
		ref.Version = argStr(a, s3ObjectArgs)
	}

	b, err := json.Marshal(map[string]any{"s3": ref})
	if err != nil {
		return nil, fmt.Errorf("encode s3 object: %w", err)
	}

	return map[string]any{"S": string(b)}, nil
}

func s3ObjectJSON(_ *utilEnv, a []any) (any, error) {
	v, err := s3Object(a)
	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(v)

	return string(b), err
}

func fromS3ObjectJSON(doc string) (any, error) {
	var wrapper struct {
		S3 *s3Ref `json:"s3"`
	}

	if err := json.Unmarshal([]byte(doc), &wrapper); err != nil || wrapper.S3 == nil {
		return nil, fmt.Errorf("%w: not a DynamoDB S3 object", ErrValidation)
	}

	out := map[string]any{"key": wrapper.S3.Key, "bucket": wrapper.S3.Bucket, "region": wrapper.S3.Region}
	if wrapper.S3.Version != "" {
		out["version"] = wrapper.S3.Version
	}

	return out, nil
}

func normalizeStr(_ *utilEnv, a []any) (any, error) {
	forms := map[string]norm.Form{"nfc": norm.NFC, "nfd": norm.NFD, "nfkc": norm.NFKC, "nfkd": norm.NFKD}

	f, ok := forms[strings.ToLower(argStr(a, 1))]
	if !ok {
		return nil, fmt.Errorf("%w: normalization form must be nfc, nfd, nfkc or nfkd", ErrValidation)
	}

	return f.String(argStr(a, 0)), nil
}

func copyHeaders(_ *utilEnv, a []any) (any, error) {
	in, _ := argAt(a, 0).(map[string]any)
	out := make(map[string]any, len(in))

	for k, v := range in {
		if !copyHeadersDenied(k) {
			out[k] = v
		}
	}

	return out, nil
}

type xmlNode struct {
	children map[string][]any
	order    []string
	text     strings.Builder
}

func xmlToMap(_ *utilEnv, a []any) (any, error) {
	dec := xml.NewDecoder(strings.NewReader(argStr(a, 0)))

	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("%w: invalid XML", ErrValidation)
		}

		if err != nil {
			return nil, fmt.Errorf("%w: invalid XML: %w", ErrValidation, err)
		}

		if start, ok := tok.(xml.StartElement); ok {
			val, derr := decodeXMLElement(dec)
			if derr != nil {
				return nil, derr
			}

			return map[string]any{start.Name.Local: val}, nil
		}
	}
}

// decodeXMLElement consumes up to the matching end element and returns the
// text (leaf) or a map of children; repeated children become lists.
func decodeXMLElement(dec *xml.Decoder) (any, error) {
	n := &xmlNode{children: map[string][]any{}}

	for {
		tok, err := dec.Token()
		if err != nil {
			return nil, fmt.Errorf("%w: invalid XML: %w", ErrValidation, err)
		}

		switch t := tok.(type) {
		case xml.StartElement:
			child, cerr := decodeXMLElement(dec)
			if cerr != nil {
				return nil, cerr
			}

			if _, seen := n.children[t.Name.Local]; !seen {
				n.order = append(n.order, t.Name.Local)
			}

			n.children[t.Name.Local] = append(n.children[t.Name.Local], child)
		case xml.CharData:
			n.text.Write(t)
		case xml.EndElement:
			return n.value(), nil
		}
	}
}

func (n *xmlNode) value() any {
	if len(n.children) == 0 {
		return strings.TrimSpace(n.text.String())
	}

	out := make(map[string]any, len(n.children))
	for k, vs := range n.children {
		if len(vs) == 1 {
			out[k] = vs[0]
		} else {
			out[k] = vs
		}
	}

	return out
}

func xmlToJSONString(u *utilEnv, a []any) (any, error) {
	m, err := xmlToMap(u, a)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer

	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)

	if err = enc.Encode(m); err != nil {
		return nil, fmt.Errorf("encode xml: %w", err)
	}

	return strings.TrimSpace(buf.String()), nil
}
