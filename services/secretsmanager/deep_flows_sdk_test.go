package secretsmanager_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	secretsmanagersdk "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

func newDeepClient(t *testing.T) (*secretsmanagersdk.Client, *secretsmanager.InMemoryBackend) {
	t.Helper()

	b := secretsmanager.NewInMemoryBackend()
	t.Cleanup(b.StopRotationScheduler)

	return newTestSecretsManagerClient(t, secretsmanager.NewHandler(b)), b
}

func TestPutSecretValue_VersionStagesPending_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantCur    string
		wantPend   string
		stages     []string
		pendingGet bool
	}{
		{name: "pending only", stages: []string{"AWSPENDING"}, wantCur: "v1", wantPend: "v2", pendingGet: true},
		{name: "default current", stages: nil, wantCur: "v2"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, _ := newDeepClient(t)
			_, err := client.CreateSecret(t.Context(), &secretsmanagersdk.CreateSecretInput{
				Name: aws.String("rot"), SecretString: aws.String("v1"),
			})
			require.NoError(t, err)

			_, err = client.PutSecretValue(t.Context(), &secretsmanagersdk.PutSecretValueInput{
				SecretId: aws.String("rot"), SecretString: aws.String("v2"), VersionStages: tc.stages,
			})
			require.NoError(t, err)

			cur, err := client.GetSecretValue(t.Context(), &secretsmanagersdk.GetSecretValueInput{
				SecretId: aws.String("rot"),
			})
			require.NoError(t, err)
			assert.Equal(t, tc.wantCur, aws.ToString(cur.SecretString))

			if tc.pendingGet {
				pend, perr := client.GetSecretValue(t.Context(), &secretsmanagersdk.GetSecretValueInput{
					SecretId: aws.String("rot"), VersionStage: aws.String("AWSPENDING"),
				})
				require.NoError(t, perr)
				assert.Equal(t, tc.wantPend, aws.ToString(pend.SecretString))
			}
		})
	}
}

func TestReplicateSecretToRegions_RegionRules_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		region  string
		wantErr string
	}{
		{name: "valid", region: "us-west-2"},
		{name: "primary region", region: "us-east-1", wantErr: "InvalidRequestException"},
		{name: "malformed", region: "xx-bogus-9!", wantErr: "InvalidParameterException"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, _ := newDeepClient(t)
			_, err := client.CreateSecret(t.Context(), &secretsmanagersdk.CreateSecretInput{
				Name: aws.String("rep"), SecretString: aws.String("v"),
			})
			require.NoError(t, err)

			_, err = client.ReplicateSecretToRegions(t.Context(), &secretsmanagersdk.ReplicateSecretToRegionsInput{
				SecretId:          aws.String("rep"),
				AddReplicaRegions: []types.ReplicaRegionType{{Region: aws.String(tc.region)}},
			})
			if tc.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.ErrorContains(t, err, tc.wantErr)

			_, err = client.DeleteSecret(t.Context(), &secretsmanagersdk.DeleteSecretInput{
				SecretId: aws.String("rep"), RecoveryWindowInDays: aws.Int64(6),
			})
			require.ErrorContains(t, err, "InvalidParameterException")

			got, err := client.DescribeSecret(t.Context(), &secretsmanagersdk.DescribeSecretInput{
				SecretId: aws.String("rep"),
			})
			require.NoError(t, err)
			assert.Empty(t, got.ReplicationStatus)
		})
	}
}

func TestCreateSecret_ReplicaRegionRules_SDK(t *testing.T) {
	t.Parallel()

	client, _ := newDeepClient(t)
	_, err := client.CreateSecret(t.Context(), &secretsmanagersdk.CreateSecretInput{
		Name:              aws.String("cs"),
		SecretString:      aws.String("v"),
		AddReplicaRegions: []types.ReplicaRegionType{{Region: aws.String("us-east-1")}},
	})
	require.ErrorContains(t, err, "InvalidRequestException")

	_, err = client.GetSecretValue(t.Context(), &secretsmanagersdk.GetSecretValueInput{SecretId: aws.String("cs")})
	require.ErrorContains(t, err, "ResourceNotFoundException")
}

func TestSecretID_ARNResolution_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate  func(arn string) string
		name    string
		wantErr bool
	}{
		{name: "full arn", mutate: func(a string) string { return a }},
		{name: "partial arn", mutate: func(a string) string { return a[:len(a)-7] }},
		{name: "wrong suffix", mutate: func(a string) string { return a[:len(a)-6] + "zzzzzz" }, wantErr: true},
		{
			name:    "wrong account",
			mutate:  func(a string) string { return strings.Replace(a, ":000000000000:", ":999999999999:", 1) },
			wantErr: true,
		},
		{
			name:    "wrong region",
			mutate:  func(a string) string { return strings.Replace(a, ":us-east-1:", ":eu-west-1:", 1) },
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, _ := newDeepClient(t)
			created, err := client.CreateSecret(t.Context(), &secretsmanagersdk.CreateSecretInput{
				Name: aws.String("my-long-secret"), SecretString: aws.String("v"),
			})
			require.NoError(t, err)

			got, err := client.DescribeSecret(t.Context(), &secretsmanagersdk.DescribeSecretInput{
				SecretId: aws.String(tc.mutate(aws.ToString(created.ARN))),
			})
			if tc.wantErr {
				require.ErrorContains(t, err, "ResourceNotFoundException")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "my-long-secret", aws.ToString(got.Name))
		})
	}
}

const (
	policyHead = `{"Version":"2012-10-17","Statement":[{"Effect":"`
	policyTail = `,"Action":"secretsmanager:GetSecretValue","Resource":"*"}]}`
)

func TestPutResourcePolicy_PublicPolicy_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		block    *bool
		name     string
		policy   string
		wantFail bool
	}{
		{name: "wildcard default allowed", policy: policyHead + `Allow","Principal":"*"` + policyTail},
		{
			name:     "wildcard blocked",
			policy:   policyHead + `Allow","Principal":{"AWS":"*"}` + policyTail,
			block:    aws.Bool(true),
			wantFail: true,
		},
		{
			name:     "wildcard in list blocked",
			policy:   policyHead + `Allow","Principal":{"AWS":["arn:aws:iam::1:root","*"]}` + policyTail,
			block:    aws.Bool(true),
			wantFail: true,
		},
		{name: "deny wildcard ok", policy: policyHead + `Deny","Principal":"*"` + policyTail, block: aws.Bool(true)},
		{
			name:   "account principal ok",
			policy: policyHead + `Allow","Principal":{"AWS":"arn:aws:iam::123456789012:root"}` + policyTail,
			block:  aws.Bool(true),
		},
		{
			name:   "spaced account principal ok",
			policy: policyHead + `Allow", "Principal" : { "AWS" : "arn:aws:iam::123456789012:root" }` + policyTail,
			block:  aws.Bool(true),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, _ := newDeepClient(t)
			_, err := client.CreateSecret(t.Context(), &secretsmanagersdk.CreateSecretInput{
				Name: aws.String("pol"), SecretString: aws.String("v"),
			})
			require.NoError(t, err)

			_, err = client.PutResourcePolicy(t.Context(), &secretsmanagersdk.PutResourcePolicyInput{
				SecretId: aws.String("pol"), ResourcePolicy: aws.String(tc.policy), BlockPublicPolicy: tc.block,
			})
			if tc.wantFail {
				var target *types.PublicPolicyException
				require.ErrorAs(t, err, &target)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestValidateResourcePolicy_PublicPolicy_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy string
		want   bool
	}{
		{name: "wildcard fails", policy: policyHead + `Allow","Principal":"*"` + policyTail},
		{
			name:   "account passes",
			policy: policyHead + `Allow","Principal":{"AWS":"arn:aws:iam::123456789012:root"}` + policyTail,
			want:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, _ := newDeepClient(t)
			out, err := client.ValidateResourcePolicy(t.Context(), &secretsmanagersdk.ValidateResourcePolicyInput{
				ResourcePolicy: aws.String(tc.policy),
			})
			require.NoError(t, err)
			assert.Equal(t, tc.want, out.PolicyValidationPassed)
		})
	}
}

var errAliasNotFound = errors.New("alias not found")

type failingKMS struct{ err error }

func (f failingKMS) Encrypt(context.Context, string, []byte) ([]byte, error) { return nil, f.err }

func (f failingKMS) Decrypt(context.Context, []byte) ([]byte, error) { return nil, f.err }

func TestCreateSecret_KMSFailureIsClientError_SDK(t *testing.T) {
	t.Parallel()

	client, b := newDeepClient(t)
	b.SetKMSEncryptor(failingKMS{err: errAliasNotFound})

	_, err := client.CreateSecret(t.Context(), &secretsmanagersdk.CreateSecretInput{
		Name: aws.String("kms"), SecretString: aws.String("v"), KmsKeyId: aws.String("alias/nope"),
	})

	var target *types.EncryptionFailure
	require.ErrorAs(t, err, &target)
}
