package eks_test

import (
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/eksauth"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/services/eks"
)

type fakePodAssumer struct {
	err   error
	calls []eks.PodIdentityRoleRequest
}

func (f *fakePodAssumer) AssumePodIdentityRole(req eks.PodIdentityRoleRequest) (eks.PodIdentityCredentials, error) {
	f.calls = append(f.calls, req)
	if f.err != nil {
		return eks.PodIdentityCredentials{}, f.err
	}

	return eks.PodIdentityCredentials{
		AccessKeyID: "ASIAPODIDENTITY", SecretAccessKey: "secret", SessionToken: "token",
		Expiration:     time.Date(2031, 1, 2, 3, 4, 5, 0, time.UTC),
		AssumedRoleARN: "arn:aws:sts::123456789012:assumed-role/app/" + req.SessionName,
		AssumedRoleID:  "AROAEXAMPLE:" + req.SessionName,
	}, nil
}

type podAuthEnv struct {
	client  *eksauth.Client
	backend *eks.InMemoryBackend
	assumer *fakePodAssumer
}

func newPodAuthEnv(t *testing.T) *podAuthEnv {
	t.Helper()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	a := &fakePodAssumer{}
	b.SetPodIdentityRoleAssumer(a)

	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	url := sdktest.Serve(t, eks.NewHandler(b))
	client := eksauth.New(eksauth.Options{
		Region: "us-east-1", BaseEndpoint: aws.String(url),
		Credentials: credentials.NewStaticCredentialsProvider("AK", "SK", ""),
		Retryer:     aws.NopRetryer{},
	})

	return &podAuthEnv{client: client, backend: b, assumer: a}
}

func (e *podAuthEnv) token(t *testing.T, ns, sa, pod string, aud ...string) string {
	t.Helper()

	tok, err := e.backend.MintServiceAccountToken("c1", eks.ServiceAccountTokenRequest{
		Namespace: ns, Name: sa, PodName: pod, Audiences: aud,
	})
	require.NoError(t, err)

	return tok.Token
}

func apiErrCode(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}

	return ""
}

func TestAssumeRoleForPodIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cluster  string
		assoc    *eks.PodIdentityAssociationInput
		token    func(t *testing.T, e *podAuthEnv) string
		stsErr   error
		wantCode string
		wantTags int
	}{
		{
			name: "credentials", cluster: "c1", assoc: &eks.PodIdentityAssociationInput{}, wantTags: 6,
			token: func(t *testing.T, e *podAuthEnv) string {
				return e.token(t, "default", "app", "app-pod-1", eks.PodIdentityAudience)
			},
		},
		{
			name: "session tags disabled", cluster: "c1", assoc: &eks.PodIdentityAssociationInput{DisableSessionTags: true},
			token: func(t *testing.T, e *podAuthEnv) string {
				return e.token(t, "default", "app", "app-pod-1", eks.PodIdentityAudience)
			},
		},
		{
			name: "no association", cluster: "c1", wantCode: "AccessDeniedException",
			token: func(t *testing.T, e *podAuthEnv) string {
				return e.token(t, "other", "app", "p", eks.PodIdentityAudience)
			},
		},
		{
			name: "irsa audience rejected", cluster: "c1", assoc: &eks.PodIdentityAssociationInput{},
			wantCode: "InvalidTokenException",
			token: func(t *testing.T, e *podAuthEnv) string {
				return e.token(t, "default", "app", "p")
			},
		},
		{
			name: "garbage token", cluster: "c1", wantCode: "InvalidParameterException",
			token: func(*testing.T, *podAuthEnv) string { return "not-a-jwt" },
		},
		{
			name: "forged signature", cluster: "c1", wantCode: "InvalidTokenException",
			token: func(*testing.T, *podAuthEnv) string { return "eyJhbGciOiJSUzI1NiJ9.e30.c2ln" },
		},
		{
			name: "unknown cluster", cluster: "nope", wantCode: "ResourceNotFoundException",
			token: func(*testing.T, *podAuthEnv) string { return "a.b.c" },
		},
		{
			name: "sts refuses", cluster: "c1", assoc: &eks.PodIdentityAssociationInput{},
			stsErr: errors.New("trust policy does not allow pods.eks.amazonaws.com"), wantCode: "AccessDeniedException",
			token: func(t *testing.T, e *podAuthEnv) string {
				return e.token(t, "default", "app", "p", eks.PodIdentityAudience)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newPodAuthEnv(t)
			e.assumer.err = tt.stsErr

			if tt.assoc != nil {
				_, err := e.backend.CreatePodIdentityAssociation(
					"c1", "default", "app", "arn:aws:iam::123456789012:role/app", nil, *tt.assoc)
				require.NoError(t, err)
			}

			out, err := e.client.AssumeRoleForPodIdentity(t.Context(), &eksauth.AssumeRoleForPodIdentityInput{
				ClusterName: aws.String(tt.cluster), Token: aws.String(tt.token(t, e)),
			})

			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, apiErrCode(err))

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "pods.eks.amazonaws.com", aws.ToString(out.Audience))
			assert.Equal(t, "default", aws.ToString(out.Subject.Namespace))
			assert.Equal(t, "app", aws.ToString(out.Subject.ServiceAccount))
			assert.Equal(t, "ASIAPODIDENTITY", aws.ToString(out.Credentials.AccessKeyId))
			assert.True(t, out.Credentials.Expiration.Equal(time.Date(2031, 1, 2, 3, 4, 5, 0, time.UTC)))
			assert.Contains(t, aws.ToString(out.PodIdentityAssociation.AssociationArn), "podidentityassociation/")
			assert.Contains(t, aws.ToString(out.AssumedRoleUser.Arn), ":assumed-role/app/eks-c1-app-pod-1-")
			assert.Regexp(t, `^AROAEXAMPLE:eks-c1-app-pod-1-[0-9a-f-]{36}$`, aws.ToString(out.AssumedRoleUser.AssumeRoleId))

			require.Len(t, e.assumer.calls, 1)
			assert.Len(t, e.assumer.calls[0].Tags, tt.wantTags)
			assert.LessOrEqual(t, len(e.assumer.calls[0].SessionName), 64)
		})
	}
}

func TestAssumeRoleForPodIdentityTargetRole(t *testing.T) {
	t.Parallel()

	e := newPodAuthEnv(t)

	_, err := e.backend.CreatePodIdentityAssociation("c1", "default", "app", "arn:aws:iam::123456789012:role/app", nil,
		eks.PodIdentityAssociationInput{TargetRoleARN: "arn:aws:iam::210987654321:role/target", Policy: `{"Version":"2012-10-17"}`})
	require.NoError(t, err)

	_, err = e.client.AssumeRoleForPodIdentity(t.Context(), &eksauth.AssumeRoleForPodIdentityInput{
		ClusterName: aws.String("c1"), Token: aws.String(e.token(t, "default", "app", "p", eks.PodIdentityAudience)),
	})
	require.NoError(t, err)

	require.Len(t, e.assumer.calls, 2)
	assert.Empty(t, e.assumer.calls[0].CallerARN)
	assert.Empty(t, e.assumer.calls[0].Policy)
	assert.Equal(t, "arn:aws:iam::210987654321:role/target", e.assumer.calls[1].RoleARN)
	assert.Contains(t, e.assumer.calls[1].CallerARN, ":assumed-role/app/")
	assert.JSONEq(t, `{"Version":"2012-10-17"}`, e.assumer.calls[1].Policy)
}
