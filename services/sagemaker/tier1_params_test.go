package sagemaker_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// CreateImageVersionInput.ClientToken is an idempotency token (api_op_CreateImageVersion.go).
func TestCreateImageVersion_ClientTokenIdempotent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		token2    string
		wantSame  bool
		wantCount int
	}{
		{name: "same-token-replays", token2: "tok-1", wantSame: true, wantCount: 1},
		{name: "new-token-new-version", token2: "tok-2", wantCount: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestSageMakerClient(t, newTestHandler(t))
			ctx := t.Context()

			_, err := client.CreateImage(ctx, &sagemakersdk.CreateImageInput{
				ImageName: aws.String("img"), RoleArn: aws.String("arn:test"),
			})
			require.NoError(t, err)

			create := func(token string) string {
				out, createErr := client.CreateImageVersion(ctx, &sagemakersdk.CreateImageVersionInput{
					ImageName: aws.String("img"), ClientToken: aws.String(token),
					BaseImage: aws.String("111111111111.dkr.ecr.us-east-1.amazonaws.com/repo:tag"),
				})
				require.NoError(t, createErr)

				return aws.ToString(out.ImageVersionArn)
			}

			first := create("tok-1")
			second := create(tt.token2)
			assert.Equal(t, tt.wantSame, first == second)

			list, err := client.ListImageVersions(
				ctx,
				&sagemakersdk.ListImageVersionsInput{ImageName: aws.String("img")},
			)
			require.NoError(t, err)
			assert.Len(t, list.ImageVersions, tt.wantCount)
		})
	}
}

// DeleteDomainInput.RetentionPolicy.HomeEfsFileSystem accepts Retain or Delete only.
func TestDeleteDomain_RetentionPolicyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		policy  *smtypes.RetentionPolicy
		name    string
		wantErr bool
	}{
		{name: "none"},
		{name: "retain", policy: &smtypes.RetentionPolicy{HomeEfsFileSystem: smtypes.RetentionTypeRetain}},
		{name: "delete", policy: &smtypes.RetentionPolicy{HomeEfsFileSystem: smtypes.RetentionTypeDelete}},
		{name: "bogus", policy: &smtypes.RetentionPolicy{HomeEfsFileSystem: "Archive"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestSageMakerClient(t, newTestHandler(t))
			ctx := t.Context()

			dom, err := client.CreateDomain(ctx, &sagemakersdk.CreateDomainInput{
				DomainName: aws.String("d"), AuthMode: smtypes.AuthModeIam, VpcId: aws.String("vpc-1"),
				SubnetIds: []string{"subnet-1"},
				DefaultUserSettings: &smtypes.UserSettings{
					ExecutionRole: aws.String("arn:aws:iam::000000000000:role/r"),
				},
			})
			require.NoError(t, err)

			domainID := aws.ToString(dom.DomainArn)
			domainID = domainID[len("arn:aws:sagemaker:us-east-1:000000000000:domain/"):]

			_, err = client.DeleteDomain(ctx, &sagemakersdk.DeleteDomainInput{
				DomainId: aws.String(domainID), RetentionPolicy: tt.policy,
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}
