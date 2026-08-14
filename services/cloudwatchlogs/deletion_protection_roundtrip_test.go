package cloudwatchlogs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwlsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

// TestDeleteLogGroup_DeletionProtectionRoundTrip proves PutLogGroupDeletionProtection's
// setting actually blocks DeleteLogGroup, not just what IsLogGroupDeletionProtected
// (previously called only from this package's own tests) says internally. Real AWS's
// PutLogGroupDeletionProtection doc says "deletion protection blocks all deletion
// operations until it is explicitly disabled", and DeleteLogGroup's own deserializer
// (cloudwatchlogs@v1.81.1 deserializers.go:2553) models OperationAbortedException as a
// typed error for this op -- before the fix, gopherstack stored the flag and never read
// it back anywhere, so DeleteLogGroup always succeeded regardless.
//
// The identifier case also exercises a second bug in the same handler: real
// PutLogGroupDeletionProtection accepts the log group's ARN as well as its bare name, but
// gopherstack stored whatever identifier the client sent without normalizing an ARN down
// to a name first, so protection set via ARN silently never matched the name-keyed lookup
// DeleteLogGroup performs.
func TestDeleteLogGroup_DeletionProtectionRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		identifier func(logGroupName, arn string) string
		name       string
		logGroup   string
		protected  bool
		wantErr    bool
	}{
		{
			name: "protected by name blocks delete", logGroup: "/dp-rt/by-name",
			identifier: func(n, _ string) string { return n }, protected: true, wantErr: true,
		},
		{
			name: "protected by arn blocks delete", logGroup: "/dp-rt/by-arn",
			identifier: func(_, a string) string { return a }, protected: true, wantErr: true,
		},
		{
			name: "unprotected allows delete", logGroup: "/dp-rt/unprotected",
			identifier: func(n, _ string) string { return n }, protected: false, wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := cloudwatchlogs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			h := cloudwatchlogs.NewHandler(backend)
			client := newTestCloudWatchLogsClient(t, h)
			ctx := t.Context()

			logGroupName := tt.logGroup
			_, err := client.CreateLogGroup(ctx, &cwlsdk.CreateLogGroupInput{
				LogGroupName: aws.String(logGroupName),
			})
			require.NoError(t, err)

			desc, err := client.DescribeLogGroups(ctx, &cwlsdk.DescribeLogGroupsInput{
				LogGroupNamePrefix: aws.String(logGroupName),
			})
			require.NoError(t, err)
			require.Len(t, desc.LogGroups, 1)
			arn := aws.ToString(desc.LogGroups[0].Arn)

			_, err = client.PutLogGroupDeletionProtection(ctx, &cwlsdk.PutLogGroupDeletionProtectionInput{
				LogGroupIdentifier:        aws.String(tt.identifier(logGroupName, arn)),
				DeletionProtectionEnabled: aws.Bool(tt.protected),
			})
			require.NoError(t, err)

			_, err = client.DeleteLogGroup(ctx, &cwlsdk.DeleteLogGroupInput{
				LogGroupName: aws.String(logGroupName),
			})

			if tt.wantErr {
				require.Error(t, err)

				var aborted *cwltypes.OperationAbortedException
				require.ErrorAs(t, err, &aborted,
					"expected a typed OperationAbortedException, got %v", err)

				return
			}

			require.NoError(t, err)
		})
	}
}
