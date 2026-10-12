package kinesis

import (
	"encoding/base64"
	"encoding/json"
	"time"
)

// pageTokenLifetime is the documented lifetime of ListShards and
// ListStreamConsumers NextToken values (api_op_ListShards.go: "Tokens expire
// after 300 seconds").
const pageTokenLifetime = 300 * time.Second

const (
	pageTokenKindShards    = "shards"
	pageTokenKindConsumers = "consumers"
)

// pageToken is the decoded form of an opaque ListShards/ListStreamConsumers
// NextToken: the stream it belongs to, the exclusive-start cursor, and when it
// was issued.
type pageToken struct {
	Kind     string `json:"k"`
	Stream   string `json:"s"`
	Cursor   string `json:"c"`
	IssuedAt int64  `json:"t"`
	Created  int64  `json:"u"`
}

func encodePageToken(kind string, stream *Stream, cursor string, now time.Time) string {
	raw, _ := json.Marshal(pageToken{
		Kind:     kind,
		Stream:   stream.Name,
		Cursor:   cursor,
		IssuedAt: now.UnixNano(),
		Created:  stream.CreatedAt.UnixNano(),
	})

	return base64.RawURLEncoding.EncodeToString(raw)
}

// decodePageToken returns ErrInvalidArgument for a malformed or foreign token
// and ErrExpiredNextToken once the token is older than pageTokenLifetime.
func decodePageToken(kind, token string, now time.Time) (pageToken, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return pageToken{}, ErrInvalidArgument
	}

	var pt pageToken
	if err = json.Unmarshal(raw, &pt); err != nil || pt.Kind != kind || pt.Stream == "" {
		return pageToken{}, ErrInvalidArgument
	}

	if now.Sub(time.Unix(0, pt.IssuedAt)) > pageTokenLifetime {
		return pageToken{}, ErrExpiredNextToken
	}

	return pt, nil
}

// matchesStream reports whether the token was issued for this incarnation of
// the stream (a deleted and recreated stream has a new creation time).
func (pt pageToken) matchesStream(stream *Stream) bool {
	return pt.Stream == stream.Name && pt.Created == stream.CreatedAt.UnixNano()
}
