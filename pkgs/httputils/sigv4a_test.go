package httputils_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// captureSigV4a signs a PutEvents call with the SDK's real SigV4a signer (global endpoint) and returns the
// request the server saw.
func captureSigV4a(t *testing.T, secret string) *http.Request {
	t.Helper()

	var got *http.Request

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got = r.Clone(r.Context())
		got.Host = r.Host
		got.Body = io.NopCloser(bytes.NewReader(body))
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		_, _ = w.Write([]byte(`{"FailedEntryCount":0,"Entries":[]}`))
	}))
	t.Cleanup(srv.Close)

	client := eventbridge.New(eventbridge.Options{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("AKIDEXAMPLE", secret, ""),
		BaseEndpoint: aws.String(srv.URL),
	})

	_, err := client.PutEvents(t.Context(), &eventbridge.PutEventsInput{
		EndpointId: aws.String("abc123.xyz"),
		Entries: []ebtypes.PutEventsRequestEntry{
			{Source: aws.String("s"), DetailType: aws.String("d"), Detail: aws.String("{}")},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Contains(t, got.Header.Get("Authorization"), httputils.SigV4aAlgorithm)

	return got
}

func TestVerifyV4a(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tamper   func(*http.Request)
		name     string
		verifier string
		wantErr  bool
	}{
		{name: "valid", verifier: "secret1"},
		{name: "wrong secret", verifier: "other", wantErr: true},
		{name: "tampered header", verifier: "secret1", wantErr: true, tamper: func(r *http.Request) {
			r.Header.Set("X-Amz-Target", "AWSEvents.Other")
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := captureSigV4a(t, "secret1")
			if tt.tamper != nil {
				tt.tamper(r)
			}

			err := httputils.NewSigV4Validator(tt.verifier).VerifyV4a(r)
			if tt.wantErr {
				require.NotNil(t, err)
				assert.Equal(t, "InvalidSignatureException", err.Code)

				return
			}

			assert.Nil(t, err)
		})
	}
}
