package secretsmanager_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	secretsmanagersdk "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

const (
	testPrimaryRegion = "us-east-1"
	testReplicaRegion = "us-west-2"
)

// TestRealClient_RotationAndReplication drives secretsmanager's typed-
// coverage-blind ops (gopherstack-n3zi) through the real aws-sdk-go-v2
// client.
func TestRealClient_RotationAndReplication(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "batch get secret value",
			run: func(t *testing.T) {
				t.Helper()

				h := secretsmanager.NewHandler(secretsmanager.NewInMemoryBackend())
				client := newTestSecretsManagerClient(t, h)
				ctx := t.Context()

				for _, name := range []string{"s11-batch-a", "s11-batch-b"} {
					_, err := client.CreateSecret(ctx, &secretsmanagersdk.CreateSecretInput{
						Name:         aws.String(name),
						SecretString: aws.String("value-" + name),
					})
					require.NoError(t, err)
				}

				out, err := client.BatchGetSecretValue(ctx, &secretsmanagersdk.BatchGetSecretValueInput{
					SecretIdList: []string{"s11-batch-a", "s11-batch-b", "s11-batch-missing"},
				})
				require.NoError(t, err)
				require.Len(t, out.SecretValues, 2)
				require.Len(t, out.Errors, 1)

				values := map[string]string{}
				for _, e := range out.SecretValues {
					values[aws.ToString(e.Name)] = aws.ToString(e.SecretString)
				}
				assert.Equal(t, "value-s11-batch-a", values["s11-batch-a"])
				assert.Equal(t, "value-s11-batch-b", values["s11-batch-b"])
				assert.Equal(t, "s11-batch-missing", aws.ToString(out.Errors[0].SecretId))
			},
		},
		{
			name: "cancel rotate secret",
			run: func(t *testing.T) {
				t.Helper()

				h := secretsmanager.NewHandler(secretsmanager.NewInMemoryBackend())
				client := newTestSecretsManagerClient(t, h)
				ctx := t.Context()

				_, err := client.CreateSecret(ctx, &secretsmanagersdk.CreateSecretInput{
					Name:         aws.String("s11-cancel-rotate"),
					SecretString: aws.String("v1"),
				})
				require.NoError(t, err)

				_, err = client.RotateSecret(ctx, &secretsmanagersdk.RotateSecretInput{
					SecretId:          aws.String("s11-cancel-rotate"),
					RotationLambdaARN: aws.String("arn:aws:lambda:us-east-1:000000000000:function:rotator"),
				})
				require.NoError(t, err)

				before, err := client.DescribeSecret(ctx, &secretsmanagersdk.DescribeSecretInput{
					SecretId: aws.String("s11-cancel-rotate"),
				})
				require.NoError(t, err)
				require.True(t, aws.ToBool(before.RotationEnabled))

				cancelOut, err := client.CancelRotateSecret(ctx, &secretsmanagersdk.CancelRotateSecretInput{
					SecretId: aws.String("s11-cancel-rotate"),
				})
				require.NoError(t, err)
				assert.Equal(t, "s11-cancel-rotate", aws.ToString(cancelOut.Name))

				after, err := client.DescribeSecret(ctx, &secretsmanagersdk.DescribeSecretInput{
					SecretId: aws.String("s11-cancel-rotate"),
				})
				require.NoError(t, err)
				assert.False(t, aws.ToBool(after.RotationEnabled),
					"real AWS: CancelRotateSecret turns off automatic rotation")
			},
		},
		{
			name: "validate resource policy",
			run: func(t *testing.T) {
				t.Helper()

				h := secretsmanager.NewHandler(secretsmanager.NewInMemoryBackend())
				client := newTestSecretsManagerClient(t, h)
				ctx := t.Context()

				_, err := client.CreateSecret(ctx, &secretsmanagersdk.CreateSecretInput{
					Name:         aws.String("s11-validate-policy"),
					SecretString: aws.String("v"),
				})
				require.NoError(t, err)

				goodPolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
					`"Principal":{"AWS":"arn:aws:iam::123456789012:root"},` +
					`"Action":"secretsmanager:GetSecretValue","Resource":"*"}]}`

				validOut, err := client.ValidateResourcePolicy(ctx, &secretsmanagersdk.ValidateResourcePolicyInput{
					SecretId:       aws.String("s11-validate-policy"),
					ResourcePolicy: aws.String(goodPolicy),
				})
				require.NoError(t, err)
				assert.True(t, validOut.PolicyValidationPassed)
				assert.Empty(t, validOut.ValidationErrors)

				invalidOut, err := client.ValidateResourcePolicy(ctx, &secretsmanagersdk.ValidateResourcePolicyInput{
					SecretId:       aws.String("s11-validate-policy"),
					ResourcePolicy: aws.String("not json"),
				})
				require.NoError(t, err)
				assert.False(t, invalidOut.PolicyValidationPassed)
				assert.NotEmpty(t, invalidOut.ValidationErrors)
			},
		},
		{
			name: "replicate and remove regions from replication",
			run: func(t *testing.T) {
				t.Helper()

				h := secretsmanager.NewHandler(secretsmanager.NewInMemoryBackend())
				primary := newTestSMClientWithRegion(t, h, testPrimaryRegion)
				replica := newTestSMClientWithRegion(t, h, testReplicaRegion)
				ctx := t.Context()

				_, err := primary.CreateSecret(ctx, &secretsmanagersdk.CreateSecretInput{
					Name:         aws.String("s11-replicated"),
					SecretString: aws.String("replicated-value"),
				})
				require.NoError(t, err)

				replOut, err := primary.ReplicateSecretToRegions(ctx, &secretsmanagersdk.ReplicateSecretToRegionsInput{
					SecretId: aws.String("s11-replicated"),
					AddReplicaRegions: []smtypes.ReplicaRegionType{
						{Region: aws.String(testReplicaRegion)},
					},
				})
				require.NoError(t, err)
				require.Len(t, replOut.ReplicationStatus, 1)
				assert.Equal(t, testReplicaRegion, aws.ToString(replOut.ReplicationStatus[0].Region))

				getFromReplica, err := replica.GetSecretValue(ctx, &secretsmanagersdk.GetSecretValueInput{
					SecretId: aws.String("s11-replicated"),
				})
				require.NoError(t, err, "a configured replica region must have a real, independently readable secret")
				assert.Equal(t, "replicated-value", aws.ToString(getFromReplica.SecretString))

				removeOut, err := primary.RemoveRegionsFromReplication(
					ctx, &secretsmanagersdk.RemoveRegionsFromReplicationInput{
						SecretId:             aws.String("s11-replicated"),
						RemoveReplicaRegions: []string{testReplicaRegion},
					},
				)
				require.NoError(t, err)
				assert.Empty(t, removeOut.ReplicationStatus)

				_, err = replica.GetSecretValue(ctx, &secretsmanagersdk.GetSecretValueInput{
					SecretId: aws.String("s11-replicated"),
				})
				assert.Error(t, err, "the replica secret must be removed once its region is dropped from replication")
			},
		},
		{
			name: "stop replication to replica",
			run: func(t *testing.T) {
				t.Helper()

				h := secretsmanager.NewHandler(secretsmanager.NewInMemoryBackend())
				primary := newTestSMClientWithRegion(t, h, testPrimaryRegion)
				replica := newTestSMClientWithRegion(t, h, testReplicaRegion)
				ctx := t.Context()

				_, err := primary.CreateSecret(ctx, &secretsmanagersdk.CreateSecretInput{
					Name:         aws.String("s11-promote"),
					SecretString: aws.String("promote-value"),
				})
				require.NoError(t, err)

				_, err = primary.ReplicateSecretToRegions(ctx, &secretsmanagersdk.ReplicateSecretToRegionsInput{
					SecretId: aws.String("s11-promote"),
					AddReplicaRegions: []smtypes.ReplicaRegionType{
						{Region: aws.String(testReplicaRegion)},
					},
				})
				require.NoError(t, err)

				beforeDesc, err := replica.DescribeSecret(ctx, &secretsmanagersdk.DescribeSecretInput{
					SecretId: aws.String("s11-promote"),
				})
				require.NoError(t, err)
				require.Equal(t, testPrimaryRegion, aws.ToString(beforeDesc.PrimaryRegion))

				stopOut, err := replica.StopReplicationToReplica(ctx, &secretsmanagersdk.StopReplicationToReplicaInput{
					SecretId: aws.String("s11-promote"),
				})
				require.NoError(t, err)
				assert.NotEmpty(t, aws.ToString(stopOut.ARN))

				afterDesc, err := replica.DescribeSecret(ctx, &secretsmanagersdk.DescribeSecretInput{
					SecretId: aws.String("s11-promote"),
				})
				require.NoError(t, err)
				// Real AWS: PrimaryRegion is "The Region the secret is in" (always
				// populated, api_op_DescribeSecret.go), not merely a replica-link
				// marker -- once promoted, the secret's own region becomes its
				// PrimaryRegion rather than the field going empty.
				assert.Equal(t, testReplicaRegion, aws.ToString(afterDesc.PrimaryRegion),
					"a promoted replica's PrimaryRegion becomes its own (former replica) region")

				primaryReplStatus, err := primary.ReplicateSecretToRegions(
					ctx, &secretsmanagersdk.ReplicateSecretToRegionsInput{
						SecretId: aws.String("s11-promote"),
						AddReplicaRegions: []smtypes.ReplicaRegionType{
							{Region: aws.String("eu-west-1")},
						},
					},
				)
				require.NoError(t, err)
				for _, st := range primaryReplStatus.ReplicationStatus {
					assert.NotEqual(t, testReplicaRegion, aws.ToString(st.Region),
						"a promoted replica's region must be dropped from the primary's outgoing replication config")
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
