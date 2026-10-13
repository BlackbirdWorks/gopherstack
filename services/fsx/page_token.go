package fsx

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
)

const (
	maxNextTokenLen = 255
	nextTokenField  = "NextToken"
)

// tokenFromBody returns the body's decoded fields and its non-empty NextToken, if any.
func tokenFromBody(body []byte) (map[string]json.RawMessage, string, bool) {
	if !bytes.Contains(body, []byte(`"`+nextTokenField+`"`)) {
		return nil, "", false
	}

	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, "", false
	}

	var tok string
	if err := json.Unmarshal(m[nextTokenField], &tok); err != nil || tok == "" {
		return nil, "", false
	}

	return m, tok, true
}

// decodeNextToken turns the opaque client token back into the backend's cursor key and rejects
// tokens this service never issued.
func decodeNextToken(body []byte) ([]byte, error) {
	m, tok, ok := tokenFromBody(body)
	if !ok {
		return body, nil
	}

	key, err := base64.RawURLEncoding.DecodeString(tok)
	if err != nil || len(tok) > maxNextTokenLen {
		return nil, fmt.Errorf("%w: NextToken is not valid", ErrValidation)
	}

	m[nextTokenField], _ = json.Marshal(string(key))

	out, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrValidation, err)
	}

	return out, nil
}

// encodeNextToken makes the backend cursor key opaque in a response body.
func encodeNextToken(body []byte) []byte {
	m, key, ok := tokenFromBody(body)
	if !ok {
		return body
	}

	m[nextTokenField], _ = json.Marshal(base64.RawURLEncoding.EncodeToString([]byte(key)))

	out, err := json.Marshal(m)
	if err != nil {
		return body
	}

	return out
}
