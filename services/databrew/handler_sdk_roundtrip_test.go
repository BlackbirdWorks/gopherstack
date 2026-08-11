package databrew_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	databrewsdk "github.com/aws/aws-sdk-go-v2/service/databrew"
	"github.com/aws/aws-sdk-go-v2/service/databrew/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/databrew"
)

const rtTestRegion = "us-east-1"

// newRoundTripClient stands up the real aws-sdk-go-v2 DataBrew client against
// an httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production -- including the
// RouteMatcher, which unit tests that call h.Handler()(c) directly bypass.
// Round-tripping through the genuine SDK serializer/deserializer is what
// actually proves a request path and an error response are wire-compatible:
// bare (non-"/databrew/v1/"-prefixed) paths like /tags/{ResourceArn} and
// /recipeVersions, and error bodies missing a "__type"/"code" field, look
// fine to ad-hoc JSON assertions but fail against the real client.
func newRoundTripClient(t *testing.T, h *databrew.Handler) *databrewsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(rtTestRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return databrewsdk.NewFromConfig(cfg, func(o *databrewsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// Test_SDKRoundTrip_ErrorsAreTyped proves that DataBrew error responses carry
// the "__type" field the real restjson1 deserializer requires to produce a
// typed exception. Before the fix, handleError wrote only
// {"Message": "..."} with no "__type"/"code" field and no X-Amzn-ErrorType
// header, so aws-sdk-go-v2's restjson.GetErrorInfo (which identifies the
// exception purely from that header/field, never from HTTP status) fell back
// to a generic smithy.GenericAPIError for every DataBrew error -- callers
// doing errors.As(err, &types.ResourceNotFoundException{}) never matched.
func Test_SDKRoundTrip_ErrorsAreTyped(t *testing.T) {
	t.Parallel()

	tests := []struct {
		trigger func(t *testing.T, client *databrewsdk.Client)
		name    string
	}{
		{
			name: "DescribeDataset not found -> ResourceNotFoundException",
			trigger: func(t *testing.T, client *databrewsdk.Client) {
				t.Helper()
				_, err := client.DescribeDataset(t.Context(), &databrewsdk.DescribeDatasetInput{
					Name: aws.String("no-such-dataset"),
				})
				var nfe *types.ResourceNotFoundException
				require.ErrorAs(t, err, &nfe)
			},
		},
		{
			name: "CreateDataset duplicate -> ConflictException",
			trigger: func(t *testing.T, client *databrewsdk.Client) {
				t.Helper()
				in := &databrewsdk.CreateDatasetInput{
					Name:  aws.String("dup-ds"),
					Input: &types.Input{S3InputDefinition: &types.S3Location{Bucket: aws.String("b")}},
				}
				_, err := client.CreateDataset(t.Context(), in)
				require.NoError(t, err)
				_, err = client.CreateDataset(t.Context(), in)
				var ce *types.ConflictException
				require.ErrorAs(t, err, &ce)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := databrew.NewInMemoryBackend("000000000000", rtTestRegion)
			h := databrew.NewHandler(backend)
			client := newRoundTripClient(t, h)

			tc.trigger(t, client)
		})
	}
}

// Test_SDKRoundTrip_TagOperations_BarePath proves TagResource, UntagResource,
// and ListTagsForResource work over the real AWS wire path
// POST/GET/DELETE /tags/{ResourceArn} (no "/databrew/v1/" prefix). Before the
// fix, the RouteMatcher's ambiguous-segment switch didn't list "tags" at all,
// so these bare requests 404'd before reaching the handler; separately, every
// DataBrew ARN embeds a "/" in its resource part (e.g. "dataset/ds1"), which
// a length-gated call to enrichDataBrewSubOpBody silently skipped for
// short (unprefixed) paths, dropping ResourceArn extraction entirely.
func Test_SDKRoundTrip_TagOperations_BarePath(t *testing.T) {
	t.Parallel()

	backend := databrew.NewInMemoryBackend("000000000000", rtTestRegion)
	h := databrew.NewHandler(backend)
	client := newRoundTripClient(t, h)

	dsOut, err := client.CreateDataset(t.Context(), &databrewsdk.CreateDatasetInput{
		Name:  aws.String("tag-ds"),
		Input: &types.Input{S3InputDefinition: &types.S3Location{Bucket: aws.String("b")}},
	})
	require.NoError(t, err)

	descOut, err := client.DescribeDataset(t.Context(), &databrewsdk.DescribeDatasetInput{Name: dsOut.Name})
	require.NoError(t, err)
	resourceArn := descOut.ResourceArn
	require.NotNil(t, resourceArn)

	_, err = client.TagResource(t.Context(), &databrewsdk.TagResourceInput{
		ResourceArn: resourceArn,
		Tags:        map[string]string{"env": "test"},
	})
	require.NoError(t, err, "TagResource over bare /tags/{arn} path")

	listOut, err := client.ListTagsForResource(t.Context(), &databrewsdk.ListTagsForResourceInput{
		ResourceArn: resourceArn,
	})
	require.NoError(t, err)
	require.Equal(t, "test", listOut.Tags["env"])

	_, err = client.UntagResource(t.Context(), &databrewsdk.UntagResourceInput{
		ResourceArn: resourceArn,
		TagKeys:     []string{"env"},
	})
	require.NoError(t, err, "UntagResource over bare /tags/{arn} path")

	listOut2, err := client.ListTagsForResource(t.Context(), &databrewsdk.ListTagsForResourceInput{
		ResourceArn: resourceArn,
	})
	require.NoError(t, err)
	require.NotContains(t, listOut2.Tags, "env")
}

// Test_SDKRoundTrip_ListRecipeVersions_BarePath proves ListRecipeVersions
// works over its real top-level path GET /recipeVersions?name=... (the
// RecipeName travels as a query param, not a path segment). Before the fix,
// the RouteMatcher didn't claim "recipeVersions" as a top-level segment.
func Test_SDKRoundTrip_ListRecipeVersions_BarePath(t *testing.T) {
	t.Parallel()

	backend := databrew.NewInMemoryBackend("000000000000", rtTestRegion)
	h := databrew.NewHandler(backend)
	client := newRoundTripClient(t, h)

	_, err := client.CreateRecipe(t.Context(), &databrewsdk.CreateRecipeInput{
		Name:  aws.String("rv-recipe"),
		Steps: []types.RecipeStep{{Action: &types.RecipeAction{Operation: aws.String("UPPER_CASE")}}},
	})
	require.NoError(t, err)

	// ListRecipeVersions excludes LATEST_WORKING (see its doc comment:
	// "Lists the versions of a particular DataBrew recipe, except for
	// LATEST_WORKING") -- an unpublished recipe has no published versions.
	out, err := client.ListRecipeVersions(t.Context(), &databrewsdk.ListRecipeVersionsInput{
		Name: aws.String("rv-recipe"),
	})
	require.NoError(t, err, "ListRecipeVersions over bare /recipeVersions path")
	require.Empty(t, out.Recipes)

	_, err = client.PublishRecipe(t.Context(), &databrewsdk.PublishRecipeInput{
		Name: aws.String("rv-recipe"),
	})
	require.NoError(t, err)

	out, err = client.ListRecipeVersions(t.Context(), &databrewsdk.ListRecipeVersionsInput{
		Name: aws.String("rv-recipe"),
	})
	require.NoError(t, err)
	require.Len(t, out.Recipes, 1)
	require.Equal(t, "rv-recipe", aws.ToString(out.Recipes[0].Name))
	require.Equal(t, "1.0", aws.ToString(out.Recipes[0].RecipeVersion))
}

// Test_SDKRoundTrip_BatchDeleteRecipeVersion_RealPath proves
// BatchDeleteRecipeVersion routes correctly over its real wire path
// POST /recipes/{Name}/batchDeleteRecipeVersion. Before the fix,
// parseRecipeSubOp matched the sub-op segment "recipeVersions" (plural, no
// verb) instead of the real "batchDeleteRecipeVersion", so real SDK traffic
// fell through to opUnknown and the client received a non-JSON 404 body it
// could not deserialize.
func Test_SDKRoundTrip_BatchDeleteRecipeVersion_RealPath(t *testing.T) {
	t.Parallel()

	backend := databrew.NewInMemoryBackend("000000000000", rtTestRegion)
	h := databrew.NewHandler(backend)
	client := newRoundTripClient(t, h)

	_, err := client.CreateRecipe(t.Context(), &databrewsdk.CreateRecipeInput{
		Name:  aws.String("bdrv-recipe"),
		Steps: []types.RecipeStep{{Action: &types.RecipeAction{Operation: aws.String("UPPER_CASE")}}},
	})
	require.NoError(t, err)

	out, err := client.BatchDeleteRecipeVersion(t.Context(), &databrewsdk.BatchDeleteRecipeVersionInput{
		Name:           aws.String("bdrv-recipe"),
		RecipeVersions: []string{"1.0"},
	})
	require.NoError(t, err)
	require.Equal(t, "bdrv-recipe", aws.ToString(out.Name))

	_, err = client.BatchDeleteRecipeVersion(t.Context(), &databrewsdk.BatchDeleteRecipeVersionInput{
		Name:           aws.String("no-such-recipe"),
		RecipeVersions: []string{"1.0"},
	})
	var nfe *types.ResourceNotFoundException
	require.ErrorAs(t, err, &nfe)
}

// Test_SDKRoundTrip_ProfileJob_OutputLocation proves CreateProfileJob and
// UpdateProfileJob thread their "OutputLocation" field (a single S3Location)
// through to the Job's Outputs list. Before the fix, both handlers parsed a
// nonexistent "Outputs" list field (that's the CreateRecipeJob/
// UpdateRecipeJob wire shape) instead of "OutputLocation", so a profile job's
// output destination was silently discarded on every create and update.
func Test_SDKRoundTrip_ProfileJob_OutputLocation(t *testing.T) {
	t.Parallel()

	backend := databrew.NewInMemoryBackend("000000000000", rtTestRegion)
	h := databrew.NewHandler(backend)
	client := newRoundTripClient(t, h)

	_, err := client.CreateDataset(t.Context(), &databrewsdk.CreateDatasetInput{
		Name:  aws.String("pj-ds"),
		Input: &types.Input{S3InputDefinition: &types.S3Location{Bucket: aws.String("b")}},
	})
	require.NoError(t, err)

	_, err = client.CreateProfileJob(t.Context(), &databrewsdk.CreateProfileJobInput{
		Name:           aws.String("pj1"),
		DatasetName:    aws.String("pj-ds"),
		RoleArn:        aws.String("arn:aws:iam::000000000000:role/x"),
		OutputLocation: &types.S3Location{Bucket: aws.String("create-bucket"), Key: aws.String("create-key")},
	})
	require.NoError(t, err)

	jobOut, err := client.DescribeJob(t.Context(), &databrewsdk.DescribeJobInput{Name: aws.String("pj1")})
	require.NoError(t, err)
	require.Len(t, jobOut.Outputs, 1, "CreateProfileJob's OutputLocation must round-trip into Outputs")
	require.Equal(t, "create-bucket", aws.ToString(jobOut.Outputs[0].Location.Bucket))
	require.Equal(t, "create-key", aws.ToString(jobOut.Outputs[0].Location.Key))

	_, err = client.UpdateProfileJob(t.Context(), &databrewsdk.UpdateProfileJobInput{
		Name:           aws.String("pj1"),
		RoleArn:        aws.String("arn:aws:iam::000000000000:role/x"),
		OutputLocation: &types.S3Location{Bucket: aws.String("update-bucket"), Key: aws.String("update-key")},
	})
	require.NoError(t, err)

	jobOut2, err := client.DescribeJob(t.Context(), &databrewsdk.DescribeJobInput{Name: aws.String("pj1")})
	require.NoError(t, err)
	require.Len(t, jobOut2.Outputs, 1, "UpdateProfileJob's OutputLocation must round-trip into Outputs")
	require.Equal(t, "update-bucket", aws.ToString(jobOut2.Outputs[0].Location.Bucket))
}

// Test_SDKRoundTrip_ListRulesets_TargetArnFilter proves ListRulesets honors
// the "targetArn" query filter. Before the fix, the backend ignored the
// filter and returned every ruleset in the region regardless of TargetArn.
func Test_SDKRoundTrip_ListRulesets_TargetArnFilter(t *testing.T) {
	t.Parallel()

	backend := databrew.NewInMemoryBackend("000000000000", rtTestRegion)
	h := databrew.NewHandler(backend)
	client := newRoundTripClient(t, h)

	targetArn := "arn:aws:databrew:us-east-1:000000000000:dataset/rs-ds1"
	otherArn := "arn:aws:databrew:us-east-1:000000000000:dataset/rs-ds2"

	for _, rs := range []struct {
		name, target string
	}{
		{"rs1", targetArn},
		{"rs2", otherArn},
	} {
		_, err := client.CreateRuleset(t.Context(), &databrewsdk.CreateRulesetInput{
			Name:      aws.String(rs.name),
			TargetArn: aws.String(rs.target),
			Rules: []types.Rule{{
				Name:            aws.String("rule1"),
				CheckExpression: aws.String(":col1 > 0"),
			}},
		})
		require.NoError(t, err)
	}

	out, err := client.ListRulesets(t.Context(), &databrewsdk.ListRulesetsInput{
		TargetArn: aws.String(targetArn),
	})
	require.NoError(t, err)
	require.Len(t, out.Rulesets, 1)
	require.Equal(t, "rs1", aws.ToString(out.Rulesets[0].Name))

	allOut, err := client.ListRulesets(t.Context(), &databrewsdk.ListRulesetsInput{})
	require.NoError(t, err)
	require.Len(t, allOut.Rulesets, 2)
}

// Test_SDKRoundTrip_Dataset_JSONFormatOptions proves a dataset's JSON format
// options round-trip through DescribeDataset. Before the fix,
// DatasetFormatOptions.JSON was tagged `json:"JSON"` (all caps); the real
// aws-sdk-go-v2 deserializer switches on the exact, case-sensitive wire key
// "Json" (mixed case) and silently drops any key it doesn't recognize, so
// FormatOptions.Json would always decode to nil on the client even though the
// server-side field was populated.
func Test_SDKRoundTrip_Dataset_JSONFormatOptions(t *testing.T) {
	t.Parallel()

	backend := databrew.NewInMemoryBackend("000000000000", rtTestRegion)
	h := databrew.NewHandler(backend)
	client := newRoundTripClient(t, h)

	_, err := client.CreateDataset(t.Context(), &databrewsdk.CreateDatasetInput{
		Name:   aws.String("json-ds"),
		Format: types.InputFormatJson,
		Input:  &types.Input{S3InputDefinition: &types.S3Location{Bucket: aws.String("b")}},
		FormatOptions: &types.FormatOptions{
			Json: &types.JsonOptions{MultiLine: true},
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeDataset(t.Context(), &databrewsdk.DescribeDatasetInput{Name: aws.String("json-ds")})
	require.NoError(t, err)
	require.NotNil(t, out.FormatOptions, "FormatOptions must round-trip")
	require.NotNil(t, out.FormatOptions.Json, "FormatOptions.Json must round-trip under the exact \"Json\" wire key")
	require.True(t, out.FormatOptions.Json.MultiLine)
}

// Test_SDKRoundTrip_ProfileJob_JobSample proves CreateProfileJob's typed
// JobSample (previously map[string]any) round-trips through the real SDK
// client and that an invalid Mode is rejected as ValidationException instead
// of silently stored.
func Test_SDKRoundTrip_ProfileJob_JobSample(t *testing.T) {
	t.Parallel()

	backend := databrew.NewInMemoryBackend("000000000000", rtTestRegion)
	h := databrew.NewHandler(backend)
	client := newRoundTripClient(t, h)

	_, err := client.CreateDataset(t.Context(), &databrewsdk.CreateDatasetInput{
		Name:  aws.String("js-ds"),
		Input: &types.Input{S3InputDefinition: &types.S3Location{Bucket: aws.String("b")}},
	})
	require.NoError(t, err)

	_, err = client.CreateProfileJob(t.Context(), &databrewsdk.CreateProfileJobInput{
		Name:           aws.String("js-job"),
		DatasetName:    aws.String("js-ds"),
		RoleArn:        aws.String("arn:aws:iam::000000000000:role/x"),
		OutputLocation: &types.S3Location{Bucket: aws.String("out")},
		JobSample:      &types.JobSample{Mode: types.SampleModeCustomRows, Size: aws.Int64(500)},
	})
	require.NoError(t, err)

	out, err := client.DescribeJob(t.Context(), &databrewsdk.DescribeJobInput{Name: aws.String("js-job")})
	require.NoError(t, err)
	require.NotNil(t, out.JobSample, "JobSample must round-trip")
	require.Equal(t, types.SampleModeCustomRows, out.JobSample.Mode)
	require.Equal(t, int64(500), aws.ToInt64(out.JobSample.Size))

	_, err = client.CreateProfileJob(t.Context(), &databrewsdk.CreateProfileJobInput{
		Name:           aws.String("js-job-bad"),
		DatasetName:    aws.String("js-ds"),
		RoleArn:        aws.String("arn:aws:iam::000000000000:role/x"),
		OutputLocation: &types.S3Location{Bucket: aws.String("out")},
		JobSample:      &types.JobSample{Mode: "SOME_ROWS"},
	})
	var ve *types.ValidationException
	require.ErrorAs(t, err, &ve, "an invalid JobSample.Mode must be rejected, not silently stored")
}

// Test_SDKRoundTrip_RecipeJob_DataCatalogOutputs proves CreateRecipeJob's
// typed DataCatalogOutputs (previously []map[string]any) round-trips through
// the real SDK client.
func Test_SDKRoundTrip_RecipeJob_DataCatalogOutputs(t *testing.T) {
	t.Parallel()

	backend := databrew.NewInMemoryBackend("000000000000", rtTestRegion)
	h := databrew.NewHandler(backend)
	client := newRoundTripClient(t, h)

	_, err := client.CreateRecipe(t.Context(), &databrewsdk.CreateRecipeInput{
		Name:  aws.String("dco-recipe"),
		Steps: []types.RecipeStep{{Action: &types.RecipeAction{Operation: aws.String("UPPER_CASE")}}},
	})
	require.NoError(t, err)

	_, err = client.CreateRecipeJob(t.Context(), &databrewsdk.CreateRecipeJobInput{
		Name:            aws.String("dco-job"),
		RoleArn:         aws.String("arn:aws:iam::000000000000:role/x"),
		RecipeReference: &types.RecipeReference{Name: aws.String("dco-recipe")},
		DataCatalogOutputs: []types.DataCatalogOutput{{
			DatabaseName: aws.String("db1"),
			TableName:    aws.String("t1"),
			S3Options:    &types.S3TableOutputOptions{Location: &types.S3Location{Bucket: aws.String("b")}},
		}},
	})
	require.NoError(t, err)

	out, err := client.DescribeJob(t.Context(), &databrewsdk.DescribeJobInput{Name: aws.String("dco-job")})
	require.NoError(t, err)
	require.Len(t, out.DataCatalogOutputs, 1, "DataCatalogOutputs must round-trip")
	require.Equal(t, "db1", aws.ToString(out.DataCatalogOutputs[0].DatabaseName))
	require.NotNil(t, out.DataCatalogOutputs[0].S3Options)
	require.Equal(t, "b", aws.ToString(out.DataCatalogOutputs[0].S3Options.Location.Bucket))
}
