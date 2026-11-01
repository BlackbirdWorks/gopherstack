package app

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/iampolicystream"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

func streamFixture() (*echo.Echo, *iampolicystream.Recorder) {
	rec := iampolicystream.New(50)
	e := echo.New()
	registerIAMPolicyStreamEndpoints(e, rec)

	rec.Record(iampolicystream.Event{
		Service: "s3", Action: "s3:GetObject", Resource: "arn:aws:s3:::b/k", Principal: "alice",
		Decision: iampolicystream.DecisionDeny, Reason: iampolicystream.ReasonImplicitDeny,
	})
	rec.Record(iampolicystream.Event{
		Service: "sqs", Action: "sqs:SendMessage", Resource: "*", Principal: "bob",
		Decision: iampolicystream.DecisionAllow, Reason: iampolicystream.ReasonIdentityPolicy,
	})

	return e, rec
}

func TestIAMPolicyStreamList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		query      string
		wantCount  int
		wantStatus int
	}{
		{"all", "", 2, http.StatusOK},
		{"deny", "?decision=deny", 1, http.StatusOK},
		{"service", "?service=SQS", 1, http.StatusOK},
		{"principal", "?principal=alice", 1, http.StatusOK},
		{"since_duration", "?since=1h", 0, http.StatusOK},
		{"limit", "?n=1", 1, http.StatusOK},
		{"bad_decision", "?decision=maybe", 0, http.StatusBadRequest},
		{"bad_since", "?since=yesterday", 0, http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, rec := streamFixture()
			if tt.name == "since_duration" {
				rec.Clear()
			}

			w := httptest.NewRecorder()
			e.ServeHTTP(w, httptest.NewRequest(http.MethodGet, iamPolicyStreamPath+tt.query, nil))
			require.Equal(t, tt.wantStatus, w.Code)

			if tt.wantStatus != http.StatusOK {
				return
			}

			var body struct {
				Events []iampolicystream.Event `json:"events"`
			}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
			assert.Len(t, body.Events, tt.wantCount)
		})
	}
}

func TestIAMPolicySuggest(t *testing.T) {
	t.Parallel()

	e, _ := streamFixture()
	w := httptest.NewRecorder()
	e.ServeHTTP(w, httptest.NewRequest(http.MethodGet, iamPolicyStreamPath+"/suggest?principal=alice", nil))
	require.Equal(t, http.StatusOK, w.Code)

	var s iampolicystream.Suggestion
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &s))
	require.Len(t, s.Policy.Statement, 1)
	assert.Equal(t, []string{"s3:GetObject"}, s.Policy.Statement[0].Action)
}

func TestIAMPolicyStreamFollow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		accept string
		prefix string
	}{
		{"ndjson", "", `{"timestamp"`},
		{"sse", "text/event-stream", "data: {"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, rec := streamFixture()
			srv := httptest.NewServer(e)
			defer srv.Close()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			req, err := http.NewRequestWithContext(
				ctx,
				http.MethodGet,
				srv.URL+iamPolicyStreamPath+"?follow=1&decision=deny",
				nil,
			)
			require.NoError(t, err)

			if tt.accept != "" {
				req.Header.Set("Accept", tt.accept)
			}

			resp, err := testclient.New(t).Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			sc := bufio.NewScanner(resp.Body)
			require.True(t, sc.Scan())
			assert.Contains(t, sc.Text(), tt.prefix)
			assert.Contains(t, sc.Text(), "s3:GetObject")

			require.Eventually(t, func() bool { return rec.Subscribers() == 1 }, 5*time.Second, time.Millisecond)

			rec.Record(iampolicystream.Event{Action: "ec2:RunInstances", Decision: iampolicystream.DecisionDeny})

			for sc.Scan() {
				if sc.Text() != "" {
					assert.Contains(t, sc.Text(), "ec2:RunInstances")

					break
				}
			}

			cancel()
			require.Eventually(t, func() bool { return rec.Subscribers() == 0 }, 5*time.Second, time.Millisecond,
				"subscriber leaked after client disconnect")
		})
	}
}
