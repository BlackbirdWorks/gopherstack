package cleanrooms_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	cleanroomssdk "github.com/aws/aws-sdk-go-v2/service/cleanrooms"
	"github.com/aws/aws-sdk-go-v2/service/cleanrooms/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cleanrooms"
)

// newTestCleanRoomsClient stands up the real aws-sdk-go-v2 Clean Rooms
// client against an httptest server running this package's Handler, wired
// through the same pkgs/service registry/router used in production.
func newTestCleanRoomsClient(t *testing.T, h *cleanrooms.Handler) *cleanroomssdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return cleanroomssdk.NewFromConfig(cfg, func(o *cleanroomssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOps_TagsRoundTrip drives a representative sample of Clean Rooms
// Create ops through a real SDK client and asserts ListTagsForResource sees
// what was supplied at creation (gopherstack-2mwl). All 10 tag-accepting
// Create ops (cleanrooms@v1.49.4: CreateAnalysisTemplate, CreateCollaboration,
// CreateConfiguredAudienceModelAssociation, CreateConfiguredTableAssociation,
// CreateConfiguredTable, CreateIdMappingTable, CreateIdNamespaceAssociation,
// CreateIntermediateTable, CreateMembership, CreatePrivacyBudgetTemplate)
// were read against the pinned SDK and found uniformly wired -- each handler
// decodes a "tags" field (confirmed against serializers.go's
// object.Key("tags")) and each backend writes b.tagsByArn[arn] = tags
// unconditionally on len(tags) > 0. This test exercises that wiring for one
// independent resource (Collaboration), one nested-under-Collaboration
// resource (Membership), and two nested-under-Membership resources
// (ConfiguredTable, AnalysisTemplate) as real-client-verified spot checks
// across the three distinct nesting shapes; the remaining six follow the
// identical pattern.
func TestCreateOps_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	tags := map[string]string{"env": "prod"}

	requireTags := func(t *testing.T, client *cleanroomssdk.Client, resourceARN string) {
		t.Helper()
		out, err := client.ListTagsForResource(t.Context(), &cleanroomssdk.ListTagsForResourceInput{
			ResourceArn: aws.String(resourceARN),
		})
		require.NoError(t, err)
		assert.Equal(t, tags, out.Tags)
	}

	t.Run("createcollaboration", func(t *testing.T) {
		t.Parallel()

		h := cleanrooms.NewHandler(cleanrooms.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestCleanRoomsClient(t, h)

		out, err := client.CreateCollaboration(t.Context(), &cleanroomssdk.CreateCollaborationInput{
			Name:                   aws.String("tagged-collab"),
			Description:            aws.String("test"),
			CreatorDisplayName:     aws.String("creator"),
			CreatorMemberAbilities: []types.MemberAbility{types.MemberAbilityCanQuery},
			QueryLogStatus:         types.CollaborationQueryLogStatusDisabled,
			Members:                []types.MemberSpecification{},
			Tags:                   tags,
		})
		require.NoError(t, err)
		requireTags(t, client, aws.ToString(out.Collaboration.Arn))
	})

	t.Run("createmembership", func(t *testing.T) {
		t.Parallel()

		h := cleanrooms.NewHandler(cleanrooms.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestCleanRoomsClient(t, h)

		collab, err := client.CreateCollaboration(t.Context(), &cleanroomssdk.CreateCollaborationInput{
			Name:                   aws.String("collab-for-membership"),
			Description:            aws.String("test"),
			CreatorDisplayName:     aws.String("creator"),
			CreatorMemberAbilities: []types.MemberAbility{types.MemberAbilityCanQuery},
			QueryLogStatus:         types.CollaborationQueryLogStatusDisabled,
			Members:                []types.MemberSpecification{},
		})
		require.NoError(t, err)

		out, err := client.CreateMembership(t.Context(), &cleanroomssdk.CreateMembershipInput{
			CollaborationIdentifier: collab.Collaboration.Id,
			QueryLogStatus:          types.MembershipQueryLogStatusDisabled,
			Tags:                    tags,
		})
		require.NoError(t, err)
		requireTags(t, client, aws.ToString(out.Membership.Arn))
	})

	t.Run("createconfiguredtable", func(t *testing.T) {
		t.Parallel()

		h := cleanrooms.NewHandler(cleanrooms.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestCleanRoomsClient(t, h)

		out, err := client.CreateConfiguredTable(t.Context(), &cleanroomssdk.CreateConfiguredTableInput{
			Name: aws.String("tagged-configured-table"),
			TableReference: &types.TableReferenceMemberGlue{
				Value: types.GlueTableReference{
					DatabaseName: aws.String("db"),
					TableName:    aws.String("table"),
				},
			},
			AllowedColumns: []string{"col1"},
			AnalysisMethod: types.AnalysisMethodDirectQuery,
			Tags:           tags,
		})
		require.NoError(t, err)
		requireTags(t, client, aws.ToString(out.ConfiguredTable.Arn))
	})

	t.Run("createanalysistemplate", func(t *testing.T) {
		t.Parallel()

		h := cleanrooms.NewHandler(cleanrooms.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestCleanRoomsClient(t, h)

		collab, err := client.CreateCollaboration(t.Context(), &cleanroomssdk.CreateCollaborationInput{
			Name:                   aws.String("collab-for-template"),
			Description:            aws.String("test"),
			CreatorDisplayName:     aws.String("creator"),
			CreatorMemberAbilities: []types.MemberAbility{types.MemberAbilityCanQuery},
			QueryLogStatus:         types.CollaborationQueryLogStatusDisabled,
			Members:                []types.MemberSpecification{},
		})
		require.NoError(t, err)

		membership, err := client.CreateMembership(t.Context(), &cleanroomssdk.CreateMembershipInput{
			CollaborationIdentifier: collab.Collaboration.Id,
			QueryLogStatus:          types.MembershipQueryLogStatusDisabled,
		})
		require.NoError(t, err)

		out, err := client.CreateAnalysisTemplate(t.Context(), &cleanroomssdk.CreateAnalysisTemplateInput{
			MembershipIdentifier: membership.Membership.Id,
			Name:                 aws.String("tagged-template"),
			Format:               types.AnalysisFormatSql,
			Source:               &types.AnalysisSourceMemberText{Value: "SELECT 1"},
			Tags:                 tags,
		})
		require.NoError(t, err)
		requireTags(t, client, aws.ToString(out.AnalysisTemplate.Arn))
	})
}
