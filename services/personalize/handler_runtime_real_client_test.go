package personalize_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/personalizeruntime"
	personalizeruntimetypes "github.com/aws/aws-sdk-go-v2/service/personalizeruntime/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/personalize"
)

// newTestPersonalizeRuntimeClient stands up the real aws-sdk-go-v2
// personalizeruntime client against an httptest server running this
// package's Handler, wired through the same pkgs/service registry/router
// used in production. personalizeruntime is a separate, REST-JSON1 SDK
// client from classic Personalize's JSON-RPC one (personalizesdk.Client),
// and unlike that client it carries no X-Amz-Target header at all -- it
// POSTs directly to /recommendations and /personalize-ranking (see
// handler.go's personalizeRuntimeRecommendationsPath/
// personalizeRuntimeRankingPath comment for the serializers.go citation).
// Routing this through RouteMatcher (rather than calling h.Handler()(c)
// directly) is the point: RouteMatcher is what a real client's request has
// to pass before dispatch is even reached.
func newTestPersonalizeRuntimeClient(t *testing.T, h *personalize.Handler) *personalizeruntime.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(config.DefaultRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return personalizeruntime.NewFromConfig(cfg, func(o *personalizeruntime.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestPersonalizeRuntime_RealSDKClient drives GetRecommendations and
// GetPersonalizedRanking through the real personalizeruntime client. Before
// this fix, RouteMatcher required an X-Amz-Target header under a fabricated
// "AmazonPersonalizeRuntime." prefix (see gopherstack-92ft); the real
// REST-JSON1 client sends no such header, so every real call 404'd at the
// router before ever reaching the handler -- a hand-built request setting
// that header, as this package's other runtime tests do, cannot catch that
// because it never exercises RouteMatcher's actual gate.
func TestPersonalizeRuntime_RealSDKClient(t *testing.T) {
	t.Parallel()

	t.Run("get_recommendations", func(t *testing.T) {
		t.Parallel()

		b := personalize.NewInMemoryBackend("000000000000", "us-east-1")
		h := personalize.NewHandler(b)
		client := newTestPersonalizeRuntimeClient(t, h)
		personalizeCreateCampaign(t, h, "sdk-recs-campaign")

		out, err := client.GetRecommendations(t.Context(), &personalizeruntime.GetRecommendationsInput{
			CampaignArn: aws.String(
				"arn:aws:personalize:us-east-1:000000000000:campaign/sdk-recs-campaign",
			),
			UserId:     aws.String("user-real-sdk"),
			NumResults: 5,
		})
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(out.RecommendationId))
		assert.Len(t, out.ItemList, 5)
		assert.NotEmpty(t, aws.ToString(out.ItemList[0].ItemId))
	})

	t.Run("get_personalized_ranking", func(t *testing.T) {
		t.Parallel()

		b := personalize.NewInMemoryBackend("000000000000", "us-east-1")
		h := personalize.NewHandler(b)
		client := newTestPersonalizeRuntimeClient(t, h)
		personalizeCreateCampaign(t, h, "sdk-ranking-campaign")

		out, err := client.GetPersonalizedRanking(
			t.Context(),
			&personalizeruntime.GetPersonalizedRankingInput{
				CampaignArn: aws.String(
					"arn:aws:personalize:us-east-1:000000000000:campaign/sdk-ranking-campaign",
				),
				UserId:    aws.String("user-real-sdk"),
				InputList: []string{"item-a", "item-b", "item-c"},
			},
		)
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(out.RecommendationId))
		require.Len(t, out.PersonalizedRanking, 3)
		assert.NotEmpty(t, aws.ToString(out.PersonalizedRanking[0].ItemId))
	})

	t.Run("not_found_maps_to_resource_not_found_exception", func(t *testing.T) {
		t.Parallel()

		b := personalize.NewInMemoryBackend("000000000000", "us-east-1")
		h := personalize.NewHandler(b)
		client := newTestPersonalizeRuntimeClient(t, h)

		_, err := client.GetRecommendations(t.Context(), &personalizeruntime.GetRecommendationsInput{
			CampaignArn: aws.String(
				"arn:aws:personalize:us-east-1:000000000000:campaign/does-not-exist",
			),
			UserId: aws.String("user-real-sdk"),
		})
		require.Error(t, err)

		var nf *personalizeruntimetypes.ResourceNotFoundException
		assert.ErrorAs(t, err, &nf)
	})
}
