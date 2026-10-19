package iam

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecideRequestBoundaryImplicitDeny(t *testing.T) {
	t.Parallel()

	allow := func(sid, action string) string {
		return `{"Version":"2012-10-17","Statement":[{"Sid":"` + sid + `","Effect":"Allow","Action":"` + action +
			`","Resource":"*"}]}`
	}

	tests := []struct {
		name       string
		identity   string
		boundary   string
		wantReason string
		wantSid    string
		wantAllow  bool
	}{
		{
			name: "boundary lacks statement", identity: allow("IdAllow", "s3:GetObject"),
			boundary: allow("B", "sqs:*"), wantReason: reasonPermissionBoundary,
		},
		{
			name: "identity lacks statement", identity: allow("IdAllow", "sqs:*"),
			boundary: allow("B", "s3:*"), wantReason: reasonImplicitDeny,
		},
		{
			name: "both allow", identity: allow("IdAllow", "s3:GetObject"),
			boundary: allow("B", "s3:*"), wantReason: reasonIdentityPolicy, wantSid: "IdAllow", wantAllow: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dec := decideRequest([]string{tt.identity}, []string{tt.boundary}, nil,
				"s3:GetObject", "arn:aws:s3:::b/k", ConditionContext{})

			assert.Equal(t, tt.wantAllow, dec.allow)
			assert.Equal(t, tt.wantReason, dec.reason)
			assert.Equal(t, tt.wantSid, dec.match.Sid)
		})
	}
}
