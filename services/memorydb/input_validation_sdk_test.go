package memorydb_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	memorydbsdk "github.com/aws/aws-sdk-go-v2/service/memorydb"
	memorydbtypes "github.com/aws/aws-sdk-go-v2/service/memorydb/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSDK_CreateUserValidation(t *testing.T) {
	t.Parallel()

	const goodPass = "a-sufficiently-long-pass"

	tests := []struct {
		name       string
		access     string
		authType   memorydbtypes.InputAuthenticationType
		wantCode   string
		passwords  []string
		wantNotErr bool
	}{
		{
			name:       "valid",
			access:     "on ~* &* +@all",
			authType:   "password",
			passwords:  []string{goodPass},
			wantNotErr: true,
		},
		{
			name:      "short_password",
			access:    "on ~* +@all",
			authType:  "password",
			passwords: []string{"short"},
			wantCode:  "InvalidParameterValueException",
		},
		{
			name:     "password_missing",
			access:   "on ~* +@all",
			authType: "password",
			wantCode: "InvalidParameterValueException",
		},
		{
			name:      "slash_password",
			access:    "on ~* +@all",
			authType:  "password",
			passwords: []string{"has/slash-in-the-password"},
			wantCode:  "InvalidParameterValueException",
		},
		{
			name:      "garbage_access",
			access:    "garbage",
			authType:  "password",
			passwords: []string{goodPass},
			wantCode:  "InvalidParameterValueException",
		},
		{name: "iam", access: "on ~* +@all", authType: "iam", wantNotErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestMemoryDBClient(t, newTestHandler(t))
			_, err := client.CreateUser(t.Context(), &memorydbsdk.CreateUserInput{
				UserName:     aws.String("u1"),
				AccessString: aws.String(tt.access),
				AuthenticationMode: &memorydbtypes.AuthenticationMode{
					Type: tt.authType, Passwords: tt.passwords,
				},
			})

			if tt.wantNotErr {
				require.NoError(t, err)

				return
			}

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			assert.NotContains(t, apiErr.ErrorMessage(), "Exception")
		})
	}
}

func TestSDK_ResourceNameAndReferenceValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run      func(context.Context, *memorydbsdk.Client) error
		name     string
		wantCode string
	}{
		{
			name: "acl_unknown_user", wantCode: "UserNotFoundFault",
			run: func(ctx context.Context, c *memorydbsdk.Client) error {
				_, err := c.CreateACL(ctx, &memorydbsdk.CreateACLInput{
					ACLName: aws.String("a1"), UserNames: []string{"nobody"},
				})

				return err
			},
		},
		{
			name: "parameter_group_family", wantCode: "InvalidParameterValueException",
			run: func(ctx context.Context, c *memorydbsdk.Client) error {
				_, err := c.CreateParameterGroup(ctx, &memorydbsdk.CreateParameterGroupInput{
					ParameterGroupName: aws.String("pg1"), Family: aws.String("bogus"),
				})

				return err
			},
		},
		{
			name: "snapshot_name", wantCode: "InvalidParameterValueException",
			run: func(ctx context.Context, c *memorydbsdk.Client) error {
				_, err := c.CreateSnapshot(ctx, &memorydbsdk.CreateSnapshotInput{
					ClusterName: aws.String("c1"), SnapshotName: aws.String("s_bad"),
				})

				return err
			},
		},
		{
			name: "cluster_not_found_message", wantCode: "ClusterNotFoundFault",
			run: func(ctx context.Context, c *memorydbsdk.Client) error {
				_, err := c.DescribeClusters(ctx, &memorydbsdk.DescribeClustersInput{ClusterName: aws.String("nope")})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.run(t.Context(), newTestMemoryDBClient(t, newTestHandler(t)))

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			assert.NotContains(t, apiErr.ErrorMessage(), "Fault")
			assert.NotContains(t, apiErr.ErrorMessage(), "Exception")
		})
	}
}

func TestSDK_ClusterDefaultParameterGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		engine  string
		version string
		want    string
	}{
		{name: "redis7", engine: "redis", version: "7.0", want: "default.memorydb-redis7"},
		{name: "redis6", engine: "redis", version: "6.2", want: "default.memorydb-redis6"},
		{name: "valkey8", engine: "valkey", version: "8.0", want: "default.memorydb-valkey8"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestMemoryDBClient(t, newTestHandler(t))
			ctx := t.Context()

			out, err := client.CreateCluster(ctx, &memorydbsdk.CreateClusterInput{
				ClusterName: aws.String("c1"),
				NodeType:    aws.String("db.r6g.large"),
				ACLName: aws.String(
					"open-access",
				),
				Engine:        aws.String(tt.engine),
				EngineVersion: aws.String(tt.version),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.want, aws.ToString(out.Cluster.ParameterGroupName))
		})
	}
}
