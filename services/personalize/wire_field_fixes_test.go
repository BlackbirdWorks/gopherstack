package personalize_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	personalizesdk "github.com/aws/aws-sdk-go-v2/service/personalize"
	"github.com/aws/aws-sdk-go-v2/service/personalize/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/personalize"
)

// newTestPersonalizeClient stands up the real aws-sdk-go-v2 personalize
// (control-plane, JSON-RPC 1.1) client against an httptest server running
// this package's Handler, wired through the same pkgs/service registry/
// router used in production.
func newTestPersonalizeClient(t *testing.T, h *personalize.Handler) *personalizesdk.Client {
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

	return personalizesdk.NewFromConfig(cfg, func(o *personalizesdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestListFilters_RealSDKClient locks ListFiltersOutput's real wrapper key.
// The real deserializer's case is "Filters" (PascalCase) -- the sole
// PascalCase top-level wrapper key in this JSON-RPC 1.1 service, unlike
// every sibling List op's lowerCamelCase. gopherstack emitted "filters"
// (lowercase); a raw-body test that only checks for a "filters" key can't
// catch this since both sides agreed on the wrong name -- only a real typed
// client, whose case-sensitive deserializer silently drops unrecognised
// keys, proves it.
func TestListFilters_RealSDKClient(t *testing.T) {
	t.Parallel()

	b := personalize.NewInMemoryBackend("000000000000", "us-east-1")
	h := personalize.NewHandler(b)
	client := newTestPersonalizeClient(t, h)

	dgArn := personalizeCreateDatasetGroup(t, h, "list-filters-real-dg")
	rec := personalizeDo(t, h, "CreateFilter", map[string]any{
		"name":             "list-filters-real",
		"datasetGroupArn":  dgArn,
		"filterExpression": "INCLUDE ItemID WHERE Items.CATEGORY IN ($CATEGORIES)",
	})
	require.Equal(t, 200, rec.Code)

	out, err := client.ListFilters(t.Context(), &personalizesdk.ListFiltersInput{
		DatasetGroupArn: aws.String(dgArn),
	})
	require.NoError(t, err)
	require.Len(t, out.Filters, 1)
	assert.Equal(t, "list-filters-real", aws.ToString(out.Filters[0].Name))
}

// TestDescribeEventTracker_AccountID locks that DescribeEventTracker emits
// accountId -- a real, always-populated EventTracker member ("The Amazon Web
// Services account that owns the event tracker") this backend already knows
// (the same accountID used to build every ARN) but never wired onto this
// one response before this fix.
func TestDescribeEventTracker_AccountID(t *testing.T) {
	t.Parallel()

	b := personalize.NewInMemoryBackend("000000000000", "us-east-1")
	h := personalize.NewHandler(b)
	client := newTestPersonalizeClient(t, h)

	dgArn := personalizeCreateDatasetGroup(t, h, "event-tracker-account-dg")
	rec := personalizeDo(t, h, "CreateEventTracker", map[string]any{
		"name":            "event-tracker-account",
		"datasetGroupArn": dgArn,
	})
	require.Equal(t, 200, rec.Code)
	etArn, _ := personalizeUnmarshal(t, rec)["eventTrackerArn"].(string)
	require.NotEmpty(t, etArn)

	out, err := client.DescribeEventTracker(t.Context(), &personalizesdk.DescribeEventTrackerInput{
		EventTrackerArn: aws.String(etArn),
	})
	require.NoError(t, err)
	assert.Equal(t, "000000000000", aws.ToString(out.EventTracker.AccountId))
}

// TestUpdateSolution_SolutionUpdateConfig proves UpdateSolution applies the
// real, caller-supplied UpdateSolutionInput.SolutionUpdateConfig member
// (AutoTrainingConfig/EventsConfig, api_op_UpdateSolution.go -- added to the
// pinned v1.50.4 SDK; the package's own doc comment claimed
// UpdateSolutionInput "only carries performAutoTraining and
// performIncrementalUpdate", which was true against an older SDK but not
// this pinned one) onto the solution's SolutionConfig. Previously this
// field was accepted by the real client but silently dropped -- neither the
// handler nor the backend's UpdateSolution signature read it at all. This
// is a round trip through the real UpdateSolution/DescribeSolution ops.
func TestUpdateSolution_SolutionUpdateConfig(t *testing.T) {
	t.Parallel()

	b := personalize.NewInMemoryBackend("000000000000", "us-east-1")
	h := personalize.NewHandler(b)
	client := newTestPersonalizeClient(t, h)

	dgArn := personalizeCreateDatasetGroup(t, h, "update-solution-config-dg")
	rec := personalizeDo(t, h, "CreateSolution", map[string]any{
		"name":            "update-solution-config",
		"datasetGroupArn": dgArn,
		"recipeArn":       "arn:aws:personalize:::recipe/aws-user-personalization",
		"solutionConfig": map[string]any{
			"autoTrainingConfig": map[string]any{"schedulingExpression": "rate(1 day)"},
		},
	})
	require.Equal(t, 200, rec.Code)
	solArn, _ := personalizeUnmarshal(t, rec)["solutionArn"].(string)
	require.NotEmpty(t, solArn)

	_, err := client.UpdateSolution(t.Context(), &personalizesdk.UpdateSolutionInput{
		SolutionArn: aws.String(solArn),
		SolutionUpdateConfig: &types.SolutionUpdateConfig{
			AutoTrainingConfig: &types.AutoTrainingConfig{SchedulingExpression: aws.String("rate(7 days)")},
			EventsConfig: &types.EventsConfig{
				EventParametersList: []types.EventParameters{{EventType: aws.String("click"), Weight: aws.Float64(1)}},
			},
		},
	})
	require.NoError(t, err)

	described, err := client.DescribeSolution(t.Context(), &personalizesdk.DescribeSolutionInput{
		SolutionArn: aws.String(solArn),
	})
	require.NoError(t, err)
	require.NotNil(t, described.Solution.SolutionConfig)
	require.NotNil(t, described.Solution.SolutionConfig.AutoTrainingConfig)
	assert.Equal(
		t,
		"rate(7 days)",
		aws.ToString(described.Solution.SolutionConfig.AutoTrainingConfig.SchedulingExpression),
	)
	require.NotNil(t, described.Solution.SolutionConfig.EventsConfig)
	require.Len(t, described.Solution.SolutionConfig.EventsConfig.EventParametersList, 1)
	assert.Equal(
		t,
		"click",
		aws.ToString(described.Solution.SolutionConfig.EventsConfig.EventParametersList[0].EventType),
	)

	require.NotNil(t, described.Solution.LatestSolutionUpdate)
	require.NotNil(t, described.Solution.LatestSolutionUpdate.SolutionUpdateConfig)
	assert.Equal(
		t,
		"rate(7 days)",
		aws.ToString(
			described.Solution.LatestSolutionUpdate.SolutionUpdateConfig.AutoTrainingConfig.SchedulingExpression,
		),
	)
}

// TestDescribeRecommender_ModelMetrics proves DescribeRecommender populates
// the real, always-present Recommender.ModelMetrics member (types.go:1697,
// deserializers.go:14660) -- previously absent entirely (not documented as
// a structural gap anywhere in PARITY.md either, an audit miss rather than
// a scoped-down decision). Values are a deterministic ARN-hash mock (no
// real training pipeline exists here), matching the same convention already
// used for SolutionVersion metrics -- this test locks that the value is
// non-empty and stable across repeated Describe calls for the same ARN.
func TestDescribeRecommender_ModelMetrics(t *testing.T) {
	t.Parallel()

	b := personalize.NewInMemoryBackend("000000000000", "us-east-1")
	h := personalize.NewHandler(b)
	client := newTestPersonalizeClient(t, h)

	dgArn := personalizeCreateDatasetGroup(t, h, "recommender-metrics-dg")
	rec := personalizeDo(t, h, "CreateRecommender", map[string]any{
		"name":            "recommender-metrics",
		"datasetGroupArn": dgArn,
		"recipeArn":       "arn:aws:personalize:::recipe/aws-user-personalization",
	})
	require.Equal(t, 200, rec.Code)
	recArn, _ := personalizeUnmarshal(t, rec)["recommenderArn"].(string)
	require.NotEmpty(t, recArn)

	first, err := client.DescribeRecommender(t.Context(), &personalizesdk.DescribeRecommenderInput{
		RecommenderArn: aws.String(recArn),
	})
	require.NoError(t, err)
	require.NotEmpty(t, first.Recommender.ModelMetrics)

	second, err := client.DescribeRecommender(t.Context(), &personalizesdk.DescribeRecommenderInput{
		RecommenderArn: aws.String(recArn),
	})
	require.NoError(t, err)
	assert.Equal(t, first.Recommender.ModelMetrics, second.Recommender.ModelMetrics, "metrics must be stable per ARN")
}

// TestCreateSolutionVersion_Name proves CreateSolutionVersion stores and
// DescribeSolutionVersion echoes the real, optional
// CreateSolutionVersionInput.Name member (api_op_CreateSolutionVersion.go) --
// previously accepted by the real client but never read by the handler at
// all (input["name"] was never looked up), so it was silently dropped.
// types.SolutionVersionSummary has no Name member (types.go:2164), so this
// is scoped to the full DescribeSolutionVersion shape only.
func TestCreateSolutionVersion_Name(t *testing.T) {
	t.Parallel()

	b := personalize.NewInMemoryBackend("000000000000", "us-east-1")
	h := personalize.NewHandler(b)
	client := newTestPersonalizeClient(t, h)

	dgArn := personalizeCreateDatasetGroup(t, h, "solution-version-name-dg")
	rec := personalizeDo(t, h, "CreateSolution", map[string]any{
		"name":            "solution-version-name",
		"datasetGroupArn": dgArn,
		"recipeArn":       "arn:aws:personalize:::recipe/aws-user-personalization",
	})
	require.Equal(t, 200, rec.Code)
	solArn, _ := personalizeUnmarshal(t, rec)["solutionArn"].(string)
	require.NotEmpty(t, solArn)

	created, err := client.CreateSolutionVersion(t.Context(), &personalizesdk.CreateSolutionVersionInput{
		SolutionArn: aws.String(solArn),
		Name:        aws.String("my-solution-version"),
	})
	require.NoError(t, err)

	described, err := client.DescribeSolutionVersion(t.Context(), &personalizesdk.DescribeSolutionVersionInput{
		SolutionVersionArn: created.SolutionVersionArn,
	})
	require.NoError(t, err)
	assert.Equal(t, "my-solution-version", aws.ToString(described.SolutionVersion.Name))
}
