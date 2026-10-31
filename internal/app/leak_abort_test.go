package app

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdks3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// TestLeakAbortedRequests drives client-cancelled, half-read and half-sent
// requests through the full router stack and asserts goroutines and heap stay bounded.
//
//nolint:paralleltest // measures process-wide heap and goroutine counts
func TestLeakAbortedRequests(t *testing.T) {
	skipLeakInShort(t)

	env := newLeakEnv(t)
	c := leakS3(env)
	ctx := context.Background()
	big := bytes.Repeat([]byte("s"), 256<<10)

	mustS3Bucket(t, c, "leak-abort", false)
	_, err := c.PutObject(ctx, &sdks3.PutObjectInput{
		Bucket: aws.String("leak-abort"), Key: aws.String("big"), Body: bytes.NewReader(big),
	})
	require.NoError(t, err)

	endpoint := aws.ToString(env.cfg.BaseEndpoint)
	url := endpoint + "/leak-abort/big"
	hc := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	t.Cleanup(hc.CloseIdleConnections)

	tests := []struct {
		op   func(t *testing.T, i int)
		name string
	}{
		{name: "cancel_before_response", op: func(t *testing.T, _ int) {
			t.Helper()

			rctx, cancel := context.WithCancel(ctx)
			cancel()

			req, rerr := http.NewRequestWithContext(rctx, http.MethodGet, url, nil)
			require.NoError(t, rerr)

			resp, derr := hc.Do(req)
			if derr == nil {
				_ = resp.Body.Close()
			}
		}},
		{name: "read_partial_body_then_close", op: func(t *testing.T, _ int) {
			t.Helper()

			rctx, cancel := context.WithCancel(ctx)
			defer cancel()

			req, rerr := http.NewRequestWithContext(rctx, http.MethodGet, url, nil)
			require.NoError(t, rerr)

			resp, derr := hc.Do(req)
			require.NoError(t, derr)

			_, rerr = io.CopyN(io.Discard, resp.Body, 1024)
			require.NoError(t, rerr)
			cancel()
			_ = resp.Body.Close()
		}},
		{name: "abort_upload_midstream", op: func(t *testing.T, i int) {
			t.Helper()

			rctx, cancel := context.WithCancel(ctx)
			defer cancel()

			pr, pw := io.Pipe()
			upURL := endpoint + "/leak-abort/up" + strconv.Itoa(i%4)
			req, rerr := http.NewRequestWithContext(rctx, http.MethodPut, upURL, pr)
			require.NoError(t, rerr)

			req.ContentLength = int64(len(big))

			done := make(chan struct{})

			go func() {
				defer close(done)

				resp, derr := hc.Do(req)
				if derr == nil {
					_ = resp.Body.Close()
				}
			}()

			_, werr := pw.Write(big[:4096])
			require.NoError(t, werr)
			cancel()
			_ = pw.CloseWithError(context.Canceled)
			<-done
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			budget := leakBudget{heapBytes: 2 << 20, heapObjs: 10_000, series: 40}
			assertNoLeak(t, env, budget, func(i int) { tt.op(t, i) })
		})
	}
}
