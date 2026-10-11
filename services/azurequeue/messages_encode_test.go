package azurequeue //nolint:testpackage // exercises the unexported encoder against encoding/xml.

import (
	"encoding/xml"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeMessagesList_MatchesXMLMarshal(t *testing.T) {
	t.Parallel()

	count := int64(3)
	zero := int64(0)

	tests := []struct {
		name string
		msgs []queueMessageXML
	}{
		{name: "empty"},
		{name: "put_shape", msgs: []queueMessageXML{{
			MessageID: "id", InsertionTime: "t1", ExpirationTime: "t2", PopReceipt: "pr", TimeNextVisible: "t3",
		}}},
		{name: "get_shape", msgs: []queueMessageXML{
			{
				MessageID:       "a",
				InsertionTime:   "t1",
				ExpirationTime:  "t2",
				PopReceipt:      "pr",
				TimeNextVisible: "t3",
				DequeueCount:    &count,
				MessageText:     "aGVsbG8=",
			},
			{MessageID: "b", InsertionTime: "t1", ExpirationTime: "t2", DequeueCount: &zero},
		}},
		{name: "escapes", msgs: []queueMessageXML{
			{
				MessageID:      "a",
				InsertionTime:  "t1",
				ExpirationTime: "t2",
				DequeueCount:   &count,
				MessageText:    "<x>&\"'\né\x01",
			},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			want, err := xml.Marshal(queueMessagesList{Messages: tt.msgs})
			require.NoError(t, err)
			assert.Equal(t, string(want), string(encodeMessagesList(tt.msgs)))
		})
	}
}
