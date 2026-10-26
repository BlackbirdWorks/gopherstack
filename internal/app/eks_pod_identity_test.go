package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	eksbackend "github.com/blackbirdworks/gopherstack/services/eks"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

func TestEKSPodIdentityAssumerIssuesSTSSessions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		req     eksbackend.PodIdentityRoleRequest
		wantTag string
	}{
		{
			name: "first hop carries session tags",
			req: eksbackend.PodIdentityRoleRequest{
				RoleARN: "arn:aws:iam::123456789012:role/app", SessionName: "eks-c1-pod-1",
				Tags: []eksbackend.PodIdentityTag{{Key: "kubernetes-namespace", Value: "default"}},
			},
			wantTag: "kubernetes-namespace",
		},
		{
			name: "chained hop",
			req: eksbackend.PodIdentityRoleRequest{
				RoleARN: "arn:aws:iam::123456789012:role/target", SessionName: "eks-c1-pod-1",
				CallerARN: "arn:aws:sts::123456789012:assumed-role/app/eks-c1-pod-1", ExternalID: "ext",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sts := stsbackend.NewInMemoryBackend()
			creds, err := eksPodIdentityAssumer{sts: sts}.AssumePodIdentityRole(tt.req)
			require.NoError(t, err)

			assert.Contains(t, creds.AssumedRoleARN, ":assumed-role/")
			assert.False(t, creds.Expiration.IsZero())

			sess := sts.LookupSession(creds.AccessKeyID, creds.SessionToken)
			require.NotNil(t, sess)
			assert.Equal(t, creds.AssumedRoleARN, sess.AssumedRoleArn)

			if tt.wantTag != "" {
				require.Len(t, sess.Tags, 1)
				assert.Equal(t, tt.wantTag, sess.Tags[0].Key)
			}
		})
	}
}
