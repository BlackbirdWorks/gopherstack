package s3control_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3csdk "github.com/aws/aws-sdk-go-v2/service/s3control"
	"github.com/aws/aws-sdk-go-v2/service/s3control/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/s3control"
)

const (
	createTagsTestAccountID = "123456789012"
	createTagsTestRegion    = "us-east-1"
)

// newTestS3ControlClient stands up the real aws-sdk-go-v2 S3 Control client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestS3ControlClient(t *testing.T, h *s3control.Handler) *s3csdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	// S3 Control's endpoint ruleset always prepends AccountId as a host
	// label onto BaseEndpoint's authority (endpoints.go case 52), even for a
	// custom endpoint -- so the request host becomes
	// "<accountID>.127.0.0.1", which has no DNS entry. Redirect every dial
	// to the httptest listener regardless of requested host; gopherstack
	// reads the account ID from the X-Amz-Account-Id header, not the Host.
	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				var d net.Dialer

				return d.DialContext(ctx, network, srv.Listener.Addr().String())
			},
		},
	}

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(createTagsTestRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
		awscfg.WithHTTPClient(httpClient),
	)
	require.NoError(t, err)

	return s3csdk.NewFromConfig(cfg, func(o *s3csdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOps_TagsRoundTrip drives every S3 Control Create op that accepts
// Tags in the real SDK (s3control@v1.73.4) through a real SDK client and
// asserts ListTagsForResource sees what was supplied at creation
// (gopherstack-2mwl). Verified against the pinned SDK's Input structs:
// api_op_CreateAccessGrant.go:113, api_op_CreateAccessGrantsInstance.go:66,
// api_op_CreateAccessGrantsLocation.go:83, api_op_CreateAccessPoint.go:133,
// api_op_CreateJob.go:117, api_op_CreateStorageLensGroup.go:67. Every Create
// except CreateAccessPoint had its Tags field entirely missing from
// gopherstack's XML request struct (a pure decode-drop, elasticache-shaped).
// Asserted through a real SDK client since S3 Control is REST-XML and a
// wrong element name would pass a handler-level assertion on the same map --
// verified correct (Tags>Tag, not Tags>member) against
// awsRestxml_deserializeDocumentTagList, s3control@v1.73.4 deserializers.go:26225.
func TestCreateOps_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	tags := []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}}
	s3Tags := []types.S3Tag{{Key: aws.String("env"), Value: aws.String("prod")}}

	requireTags := func(t *testing.T, client *s3csdk.Client, resourceARN *string) {
		t.Helper()
		out, err := client.ListTagsForResource(t.Context(), &s3csdk.ListTagsForResourceInput{
			AccountId:   aws.String(createTagsTestAccountID),
			ResourceArn: resourceARN,
		})
		require.NoError(t, err)
		require.Len(t, out.Tags, 1)
		assert.Equal(t, "env", aws.ToString(out.Tags[0].Key))
		assert.Equal(t, "prod", aws.ToString(out.Tags[0].Value))
	}

	newHandler := func(t *testing.T) *s3control.Handler {
		t.Helper()

		return s3control.NewHandler(
			s3control.NewInMemoryBackendWithConfig(createTagsTestAccountID, createTagsTestRegion),
		)
	}

	t.Run("createaccesspoint", func(t *testing.T) {
		t.Parallel()

		client := newTestS3ControlClient(t, newHandler(t))

		out, err := client.CreateAccessPoint(t.Context(), &s3csdk.CreateAccessPointInput{
			AccountId: aws.String(createTagsTestAccountID),
			Name:      aws.String("tagged-ap"),
			Bucket:    aws.String("tagged-bucket"),
			Tags:      tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.AccessPointArn)
	})

	t.Run("createjob", func(t *testing.T) {
		t.Parallel()

		client := newTestS3ControlClient(t, newHandler(t))

		out, err := client.CreateJob(t.Context(), &s3csdk.CreateJobInput{
			AccountId:          aws.String(createTagsTestAccountID),
			ClientRequestToken: aws.String("token-1"),
			Operation: &types.JobOperation{
				LambdaInvoke: &types.LambdaInvokeOperation{
					FunctionArn: aws.String("arn:aws:lambda:us-east-1:123456789012:function:tagged-fn"),
				},
			},
			Priority: aws.Int32(1),
			Report:   &types.JobReport{Enabled: false},
			RoleArn:  aws.String("arn:aws:iam::123456789012:role/batch-ops"),
			Tags:     s3Tags,
		})
		require.NoError(t, err)

		jobARN := fmt.Sprintf(
			"arn:aws:s3:%s:%s:job/%s", createTagsTestRegion, createTagsTestAccountID, aws.ToString(out.JobId),
		)
		requireTags(t, client, aws.String(jobARN))
	})

	t.Run("createaccessgrantsinstance", func(t *testing.T) {
		t.Parallel()

		client := newTestS3ControlClient(t, newHandler(t))

		out, err := client.CreateAccessGrantsInstance(t.Context(), &s3csdk.CreateAccessGrantsInstanceInput{
			AccountId: aws.String(createTagsTestAccountID),
			Tags:      tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.AccessGrantsInstanceArn)
	})

	t.Run("createaccessgrantslocation", func(t *testing.T) {
		t.Parallel()

		client := newTestS3ControlClient(t, newHandler(t))

		out, err := client.CreateAccessGrantsLocation(t.Context(), &s3csdk.CreateAccessGrantsLocationInput{
			AccountId:     aws.String(createTagsTestAccountID),
			LocationScope: aws.String("s3://tagged-bucket/*"),
			IAMRoleArn:    aws.String("arn:aws:iam::123456789012:role/access-grants"),
			Tags:          tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.AccessGrantsLocationArn)
	})

	t.Run("createaccessgrant", func(t *testing.T) {
		t.Parallel()

		client := newTestS3ControlClient(t, newHandler(t))

		out, err := client.CreateAccessGrant(t.Context(), &s3csdk.CreateAccessGrantInput{
			AccountId:              aws.String(createTagsTestAccountID),
			AccessGrantsLocationId: aws.String("default"),
			Grantee: &types.Grantee{
				GranteeType:       types.GranteeTypeIam,
				GranteeIdentifier: aws.String("arn:aws:iam::123456789012:role/reader"),
			},
			Permission: types.PermissionRead,
			Tags:       tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.AccessGrantArn)
	})

	t.Run("createstoragelensgroup", func(t *testing.T) {
		t.Parallel()

		client := newTestS3ControlClient(t, newHandler(t))

		_, err := client.CreateStorageLensGroup(t.Context(), &s3csdk.CreateStorageLensGroupInput{
			AccountId: aws.String(createTagsTestAccountID),
			StorageLensGroup: &types.StorageLensGroup{
				Name: aws.String("tagged-group"),
				Filter: &types.StorageLensGroupFilter{
					MatchAnyPrefix: []string{"logs/"},
				},
			},
			Tags: tags,
		})
		require.NoError(t, err)

		arn := fmt.Sprintf(
			"arn:aws:s3:%s:%s:storage-lens-group/%s",
			createTagsTestRegion, createTagsTestAccountID, "tagged-group",
		)
		requireTags(t, client, aws.String(arn))
	})
}
