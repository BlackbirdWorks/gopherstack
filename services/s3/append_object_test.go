package s3_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPutObject_WriteOffsetAppend(t *testing.T) {
	t.Parallel()

	const dir = "app--use1-az4--x-s3"

	tests := []struct {
		name       string
		bucket     string
		wantBody   string
		wantSize   string
		offsets    []string
		wantStatus []int
	}{
		{
			name: "append twice", bucket: dir, offsets: []string{"0", "3", "6"},
			wantStatus: []int{200, 200, 200}, wantBody: "abcabcabc", wantSize: "9",
		},
		{
			name: "offset mismatch", bucket: dir, offsets: []string{"0", "2"},
			wantStatus: []int{200, 400}, wantBody: "abc",
		},
		{
			name: "nonzero offset on missing object", bucket: dir, offsets: []string{"5"},
			wantStatus: []int{400},
		},
		{
			name: "ignored on general purpose bucket", bucket: "gp-bucket", offsets: []string{"0", "3"},
			wantStatus: []int{200, 200}, wantBody: "abc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, tt.bucket)

			var last map[string][]string

			for i, off := range tt.offsets {
				rec := doRequest(handler, http.MethodPut, "/"+tt.bucket+"/k", strings.NewReader("abc"),
					map[string]string{"X-Amz-Write-Offset-Bytes": off})
				require.Equal(t, tt.wantStatus[i], rec.Code)

				if rec.Code == http.StatusBadRequest {
					assert.Contains(t, rec.Body.String(), "InvalidWriteOffset")
				}

				last = rec.Header()
			}

			if tt.wantBody == "" {
				return
			}

			got := doRequest(handler, http.MethodGet, "/"+tt.bucket+"/k", nil, nil)
			require.Equal(t, http.StatusOK, got.Code)
			assert.Equal(t, tt.wantBody, got.Body.String())

			if tt.wantSize != "" {
				assert.Equal(t, []string{tt.wantSize}, last["X-Amz-Object-Size"])
			}
		})
	}
}
