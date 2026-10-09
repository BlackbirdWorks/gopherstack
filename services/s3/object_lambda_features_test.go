package s3_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/s3"
)

var errStopInvoke = errors.New("stop")

type payloadRecorder struct {
	payload chan []byte
	once    sync.Once
}

func (p *payloadRecorder) InvokeFunction(_ context.Context, _, _ string, payload []byte) ([]byte, int, error) {
	p.once.Do(func() { p.payload <- payload })

	return nil, 0, errStopInvoke
}

func TestObjectLambdaAllowedFeatures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		method  string
		query   string
		rng     string
		allowed []string
		want501 bool
	}{
		{"get_range_denied", http.MethodGet, "", "bytes=0-1", nil, true},
		{"get_range_allowed", http.MethodGet, "", "bytes=0-1", []string{"GetObject-Range"}, false},
		{"get_part_denied", http.MethodGet, "?partNumber=1", "", []string{"GetObject-Range"}, true},
		{"get_part_allowed", http.MethodGet, "?partNumber=1", "", []string{"GetObject-PartNumber"}, false},
		{"plain_get", http.MethodGet, "", "", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "olf-bucket")

			handler.SetObjectLambdaAccessPoint("olf-bucket", s3.StoredObjectLambdaAccessPoint{
				Name: "olf", AccountID: "000000000000", Alias: "olf-alias--ol-s3",
				LambdaARN:       "arn:aws:lambda:us-east-1:000000000000:function:f",
				AllowedFeatures: tt.allowed,
			})

			req := httptest.NewRequest(tt.method, "/olf-alias--ol-s3/k"+tt.query, nil)
			if tt.rng != "" {
				req.Header.Set("Range", tt.rng)
			}

			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			if tt.want501 {
				assert.Equal(t, http.StatusNotImplemented, rec.Code)

				return
			}

			assert.NotEqual(t, http.StatusNotImplemented, rec.Code)
		})
	}
}

func TestObjectLambdaEventUserIdentity(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "oli-bucket")

	rec := &payloadRecorder{payload: make(chan []byte, 1)}
	handler.SetNotificationDispatcher(
		s3.NewNotificationDispatcher(&s3.NotificationTargets{LambdaInvoker: rec}, "us-east-1"),
	)
	handler.SetObjectLambdaAccessPoint("oli-bucket", s3.StoredObjectLambdaAccessPoint{
		Name: "oli", AccountID: "000000000000", Alias: "oli-alias--ol-s3",
		LambdaARN: "arn:aws:lambda:us-east-1:000000000000:function:f",
	})

	req := httptest.NewRequest(http.MethodGet, "/oli-alias--ol-s3/k", nil)
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{Principal: &awsmeta.Principal{
		Kind: awsmeta.PrincipalKindUser, Arn: "arn:aws:iam::000000000000:user/alice", AccountID: "000000000000",
		UserName: "alice", UserID: "AIDAALICE",
	}}))

	done := make(chan struct{})

	go func() {
		defer close(done)

		serveS3Handler(handler, httptest.NewRecorder(), req)
	}()

	var event struct {
		UserIdentity struct {
			Type string `json:"type"`
			ARN  string `json:"arn"`
		} `json:"userIdentity"`
	}

	select {
	case p := <-rec.payload:
		require.NoError(t, json.Unmarshal(p, &event))
	case <-t.Context().Done():
		t.Fatal("lambda not invoked")
	}

	<-done

	assert.Equal(t, "User", event.UserIdentity.Type)
	assert.True(t, strings.HasSuffix(event.UserIdentity.ARN, "user/alice"))
}
