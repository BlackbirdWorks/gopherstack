package amp_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/klauspost/compress/snappy"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/encoding/protowire"
)

func metaBody(metric, help string, typ uint64) []byte {
	var m []byte

	m = protowire.AppendTag(m, 1, protowire.VarintType)
	m = protowire.AppendVarint(m, typ)
	m = protowire.AppendTag(m, 2, protowire.BytesType)
	m = protowire.AppendString(m, metric)
	m = protowire.AppendTag(m, 4, protowire.BytesType)
	m = protowire.AppendString(m, help)

	var raw []byte

	raw = protowire.AppendTag(raw, 3, protowire.BytesType)
	raw = protowire.AppendBytes(raw, m)

	return snappy.Encode(nil, raw)
}

func TestMetadataIngestAndQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want string
		miss string
	}{
		{name: "all", path: "metadata", want: `"http_requests_total":[{"type":"counter","help":"Requests."`},
		{name: "by metric", path: "metadata?metric=http_requests_total", want: `"type":"counter"`},
		{name: "other metric", path: "metadata?metric=nope", want: `"data":{}`},
		{name: "limit", path: "metadata?limit=1", want: `"data":{"http_requests_total"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			ws := f.workspace(t)

			r := f.do(t, http.MethodPost, "/workspaces/"+ws+"/api/v1/remote_write",
				strings.NewReader(string(metaBody("http_requests_total", "Requests.", 1))), nil)
			assert.Equal(t, http.StatusOK, r.status, string(r.body))

			got := f.do(t, http.MethodGet, "/workspaces/"+ws+"/api/v1/"+tt.path, nil, nil)
			assert.Contains(t, string(got.body), tt.want)
		})
	}
}
