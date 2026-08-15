package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestEmptyResultElement_RealClient covers cloudformation ops whose real output shape
// has zero members but whose deserializer still calls
// decoder.GetElement("<Op>Result") (cloudformation@v1.76.1 deserializers.go, confirmed
// per-op). gopherstack omitted the element for these nine, so every real SDK client
// failed deserialization with "deserialization failed: failed to decode response
// body ... node not found" even though the backend mutation succeeded. The assertion
// is exactly that the call deserializes without error -- there is nothing else to
// check on an empty output. StopStackSetOperation is covered separately
// (stopstacksetoperation_realclient_test.go) because it needs a stack-set operation
// left in a RUNNING state, which no exported API can produce -- gopherstack's stack-set
// operations are always recorded as SUCCEEDED synchronously, a separate gap out of
// scope here.
func TestEmptyResultElement_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(t *testing.T, client *cfnsdk.Client) error
		name string
	}{
		{
			name: "activateorganizationsaccess",
			call: func(t *testing.T, client *cfnsdk.Client) error {
				t.Helper()

				_, err := client.ActivateOrganizationsAccess(
					t.Context(),
					&cfnsdk.ActivateOrganizationsAccessInput{},
				)

				return err
			},
		},
		{
			name: "deactivateorganizationsaccess",
			call: func(t *testing.T, client *cfnsdk.Client) error {
				t.Helper()

				_, err := client.DeactivateOrganizationsAccess(
					t.Context(),
					&cfnsdk.DeactivateOrganizationsAccessInput{},
				)

				return err
			},
		},
		{
			name: "deletechangeset",
			call: func(t *testing.T, client *cfnsdk.Client) error {
				t.Helper()

				_, err := client.CreateChangeSet(t.Context(), &cfnsdk.CreateChangeSetInput{
					StackName:     aws.String("empty-result-delete-cs-stack"),
					ChangeSetName: aws.String("empty-result-delete-cs"),
					TemplateBody:  aws.String(simpleTemplate),
				})
				require.NoError(t, err)

				_, err = client.DeleteChangeSet(t.Context(), &cfnsdk.DeleteChangeSetInput{
					StackName:     aws.String("empty-result-delete-cs-stack"),
					ChangeSetName: aws.String("empty-result-delete-cs"),
				})

				return err
			},
		},
		{
			name: "deletestackset",
			call: func(t *testing.T, client *cfnsdk.Client) error {
				t.Helper()

				_, err := client.DeleteStackSet(t.Context(), &cfnsdk.DeleteStackSetInput{
					StackSetName: aws.String("empty-result-nonexistent-stackset"),
				})

				return err
			},
		},
		{
			name: "deregistertype",
			call: func(t *testing.T, client *cfnsdk.Client) error {
				t.Helper()

				typeName := "GopherStack::EmptyResult::DeregisterType"

				_, err := client.RegisterType(t.Context(), &cfnsdk.RegisterTypeInput{
					TypeName:             aws.String(typeName),
					SchemaHandlerPackage: aws.String("s3://bucket/key.zip"),
				})
				require.NoError(t, err)

				_, err = client.DeregisterType(t.Context(), &cfnsdk.DeregisterTypeInput{
					Arn: aws.String("arn:aws:cloudformation:::type/resource/" + typeName),
				})

				return err
			},
		},
		{
			name: "executechangeset",
			call: func(t *testing.T, client *cfnsdk.Client) error {
				t.Helper()

				_, err := client.CreateChangeSet(t.Context(), &cfnsdk.CreateChangeSetInput{
					StackName:     aws.String("empty-result-execute-cs-stack"),
					ChangeSetName: aws.String("empty-result-execute-cs"),
					TemplateBody:  aws.String(simpleTemplate),
				})
				require.NoError(t, err)

				_, err = client.ExecuteChangeSet(t.Context(), &cfnsdk.ExecuteChangeSetInput{
					StackName:     aws.String("empty-result-execute-cs-stack"),
					ChangeSetName: aws.String("empty-result-execute-cs"),
				})

				return err
			},
		},
		{
			name: "recordhandlerprogress",
			call: func(t *testing.T, client *cfnsdk.Client) error {
				t.Helper()

				_, err := client.RecordHandlerProgress(
					t.Context(),
					&cfnsdk.RecordHandlerProgressInput{
						BearerToken:     aws.String(uuid.New().String()),
						OperationStatus: "IN_PROGRESS",
					},
				)

				return err
			},
		},
		{
			name: "settypedefaultversion",
			call: func(t *testing.T, client *cfnsdk.Client) error {
				t.Helper()

				typeName := "GopherStack::EmptyResult::SetTypeDefaultVersion"

				_, err := client.RegisterType(t.Context(), &cfnsdk.RegisterTypeInput{
					TypeName:             aws.String(typeName),
					SchemaHandlerPackage: aws.String("s3://bucket/key.zip"),
				})
				require.NoError(t, err)

				_, err = client.SetTypeDefaultVersion(
					t.Context(),
					&cfnsdk.SetTypeDefaultVersionInput{
						Arn:       aws.String("arn:aws:cloudformation:::type/resource/" + typeName),
						VersionId: aws.String("00000001"),
					},
				)

				return err
			},
		},
		{
			name: "deactivatetype",
			call: func(t *testing.T, client *cfnsdk.Client) error {
				t.Helper()

				typeName := "GopherStack::EmptyResult::DeactivateType"

				_, err := client.ActivateType(t.Context(), &cfnsdk.ActivateTypeInput{
					TypeName: aws.String(typeName),
				})
				require.NoError(t, err)

				_, err = client.DeactivateType(t.Context(), &cfnsdk.DeactivateTypeInput{
					TypeName: aws.String(typeName),
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)

			require.NoError(t, tt.call(t, client))
		})
	}
}
