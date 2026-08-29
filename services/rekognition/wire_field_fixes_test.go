package rekognition_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	rekognitionsdk "github.com/aws/aws-sdk-go-v2/service/rekognition"
	"github.com/aws/aws-sdk-go-v2/service/rekognition/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/rekognition"
)

const wireFixesRegion = "us-east-1"

func newTestRekognitionClient(t *testing.T, h *rekognition.Handler) *rekognitionsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(wireFixesRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return rekognitionsdk.NewFromConfig(cfg, func(o *rekognitionsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestUpdateDatasetEntries_ChangesWireShape proves UpdateDatasetEntries
// accepts the real client's request shape and that the resulting entries and
// labels round-trip through ListDatasetEntries/ListDatasetLabels. Before the
// fix, the handler's `Changes []byte` field expected the base64 manifest
// bytes directly at the "Changes" key; a real client always sends
// {"Changes":{"GroundTruth":"<base64>"}} (serializers.go's
// awsAwsjson11_serializeDocumentDatasetChanges), which json.Unmarshal into a
// []byte field hard-errors on -- every real UpdateDatasetEntries call failed,
// not just silently dropped data.
func TestUpdateDatasetEntries_ChangesWireShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestRekognitionClient(t, h)

	proj, err := client.CreateProject(t.Context(), &rekognitionsdk.CreateProjectInput{
		ProjectName: aws.String("changes-wire-proj"),
	})
	require.NoError(t, err)

	ds, err := client.CreateDataset(t.Context(), &rekognitionsdk.CreateDatasetInput{
		ProjectArn:  proj.ProjectArn,
		DatasetType: types.DatasetTypeTrain,
	})
	require.NoError(t, err)

	entry := []byte(`{"source-ref":"s3://b/img1.jpg","labels-metadata":{"class-name":"cat"}}`)

	_, err = client.UpdateDatasetEntries(t.Context(), &rekognitionsdk.UpdateDatasetEntriesInput{
		DatasetArn: ds.DatasetArn,
		Changes:    &types.DatasetChanges{GroundTruth: entry},
	})
	require.NoError(t, err)

	entries, err := client.ListDatasetEntries(t.Context(), &rekognitionsdk.ListDatasetEntriesInput{
		DatasetArn: ds.DatasetArn,
	})
	require.NoError(t, err)
	require.Len(t, entries.DatasetEntries, 1)
	assert.JSONEq(t, string(entry), entries.DatasetEntries[0])
}

// TestListDatasetLabels_DatasetLabelDescriptionsWireKey proves
// ListDatasetLabels round-trips through the real SDK client. Before the fix,
// the handler emitted the collection under the fabricated top-level key
// "DatasetLabelStats" with EntryCount flat per item; the real key is
// "DatasetLabelDescriptions" with EntryCount nested one level down under
// LabelStats (deserializers.go's ListDatasetLabelsOutput switch has no
// "DatasetLabelStats" case) -- a real typed client's DatasetLabelDescriptions
// field silently decoded to an empty slice every time.
func TestListDatasetLabels_DatasetLabelDescriptionsWireKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestRekognitionClient(t, h)

	proj, err := client.CreateProject(t.Context(), &rekognitionsdk.CreateProjectInput{
		ProjectName: aws.String("labels-wire-proj"),
	})
	require.NoError(t, err)

	ds, err := client.CreateDataset(t.Context(), &rekognitionsdk.CreateDatasetInput{
		ProjectArn:  proj.ProjectArn,
		DatasetType: types.DatasetTypeTrain,
	})
	require.NoError(t, err)

	entries := [][]byte{
		[]byte(`{"source-ref":"s3://b/img1.jpg","labels-metadata":{"class-name":"cat"}}`),
		[]byte(`{"source-ref":"s3://b/img2.jpg","labels-metadata":{"class-name":"cat"}}`),
		[]byte(`{"source-ref":"s3://b/img3.jpg","labels-metadata":{"class-name":"dog"}}`),
	}
	for _, e := range entries {
		_, err = client.UpdateDatasetEntries(t.Context(), &rekognitionsdk.UpdateDatasetEntriesInput{
			DatasetArn: ds.DatasetArn,
			Changes:    &types.DatasetChanges{GroundTruth: e},
		})
		require.NoError(t, err)
	}

	out, err := client.ListDatasetLabels(t.Context(), &rekognitionsdk.ListDatasetLabelsInput{
		DatasetArn: ds.DatasetArn,
	})
	require.NoError(t, err)
	require.Len(t, out.DatasetLabelDescriptions, 2)

	byName := make(map[string]*types.DatasetLabelStats, len(out.DatasetLabelDescriptions))
	for _, d := range out.DatasetLabelDescriptions {
		require.NotNil(t, d.LabelName)
		byName[*d.LabelName] = d.LabelStats
	}

	require.Contains(t, byName, "cat")
	require.NotNil(t, byName["cat"])
	require.NotNil(t, byName["cat"].EntryCount)
	assert.Equal(t, int32(2), *byName["cat"].EntryCount)

	require.Contains(t, byName, "dog")
	require.NotNil(t, byName["dog"])
	require.NotNil(t, byName["dog"].EntryCount)
	assert.Equal(t, int32(1), *byName["dog"].EntryCount)
}

// TestDescribeCollection_UserCount proves DescribeCollection's UserCount
// reflects users created via CreateUser. Before the fix, the backend tracked
// per-collection users (ListUsers already worked) but DescribeCollection
// never counted them, so a real client's UserCount was always the Go zero
// value (0) regardless of how many users existed.
func TestDescribeCollection_UserCount(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestRekognitionClient(t, h)

	_, err := client.CreateCollection(t.Context(), &rekognitionsdk.CreateCollectionInput{
		CollectionId: aws.String("usercount-coll"),
	})
	require.NoError(t, err)

	for _, userID := range []string{"user-a", "user-b", "user-c"} {
		_, err = client.CreateUser(t.Context(), &rekognitionsdk.CreateUserInput{
			CollectionId: aws.String("usercount-coll"),
			UserId:       aws.String(userID),
		})
		require.NoError(t, err)
	}

	out, err := client.DescribeCollection(t.Context(), &rekognitionsdk.DescribeCollectionInput{
		CollectionId: aws.String("usercount-coll"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.UserCount)
	assert.Equal(t, int64(3), *out.UserCount)
}

// TestDescribeDataset_DatasetStats proves DescribeDataset's DatasetStats
// reflects the dataset's stored manifest entries. Before the fix,
// DatasetDescription never emitted a DatasetStats member at all, so a real
// client's DatasetStats was always nil regardless of how many entries or
// labels the dataset held.
func TestDescribeDataset_DatasetStats(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestRekognitionClient(t, h)

	proj, err := client.CreateProject(t.Context(), &rekognitionsdk.CreateProjectInput{
		ProjectName: aws.String("stats-proj"),
	})
	require.NoError(t, err)

	ds, err := client.CreateDataset(t.Context(), &rekognitionsdk.CreateDatasetInput{
		ProjectArn:  proj.ProjectArn,
		DatasetType: types.DatasetTypeTrain,
	})
	require.NoError(t, err)

	entries := [][]byte{
		[]byte(`{"source-ref":"s3://b/img1.jpg","labels-metadata":{"class-name":"cat"}}`),
		[]byte(`{"source-ref":"s3://b/img2.jpg"}`), // unlabeled
		[]byte(`{"source-ref":"s3://b/img3.jpg","labels-metadata":{"class-name":"dog"}}`),
	}
	for _, e := range entries {
		_, err = client.UpdateDatasetEntries(t.Context(), &rekognitionsdk.UpdateDatasetEntriesInput{
			DatasetArn: ds.DatasetArn,
			Changes:    &types.DatasetChanges{GroundTruth: e},
		})
		require.NoError(t, err)
	}

	out, err := client.DescribeDataset(t.Context(), &rekognitionsdk.DescribeDatasetInput{
		DatasetArn: ds.DatasetArn,
	})
	require.NoError(t, err)
	require.NotNil(t, out.DatasetDescription)
	require.NotNil(t, out.DatasetDescription.DatasetStats)

	stats := out.DatasetDescription.DatasetStats
	require.NotNil(t, stats.TotalEntries)
	assert.Equal(t, int32(3), *stats.TotalEntries)
	require.NotNil(t, stats.LabeledEntries)
	assert.Equal(t, int32(2), *stats.LabeledEntries)
	require.NotNil(t, stats.TotalLabels)
	assert.Equal(t, int32(2), *stats.TotalLabels)
}

// TestCreateProject_AutoUpdateFeatureEcho proves CreateProjectInput's
// AutoUpdate and Feature echo back through DescribeProjects. Before the fix,
// CreateProject's backend method took only a name, discarding both fields
// entirely; a real client's ProjectDescription.AutoUpdate/Feature were
// always empty regardless of what CreateProject was called with.
func TestCreateProject_AutoUpdateFeatureEcho(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestRekognitionClient(t, h)

	explicit, err := client.CreateProject(t.Context(), &rekognitionsdk.CreateProjectInput{
		ProjectName: aws.String("autoupdate-proj"),
		AutoUpdate:  types.ProjectAutoUpdateEnabled,
		Feature:     types.CustomizationFeatureContentModeration,
	})
	require.NoError(t, err)

	defaulted, err := client.CreateProject(t.Context(), &rekognitionsdk.CreateProjectInput{
		ProjectName: aws.String("default-feature-proj"),
	})
	require.NoError(t, err)

	out, err := client.DescribeProjects(t.Context(), &rekognitionsdk.DescribeProjectsInput{
		ProjectNames: []string{"autoupdate-proj", "default-feature-proj"},
		// Both projects must be named explicitly: the Features filter defaults
		// to CUSTOM_LABELS only (see TestDescribeProjects_FeaturesFilter), which
		// would otherwise silently exclude the CONTENT_MODERATION project.
		Features: []types.CustomizationFeature{
			types.CustomizationFeatureCustomLabels,
			types.CustomizationFeatureContentModeration,
		},
	})
	require.NoError(t, err)
	require.Len(t, out.ProjectDescriptions, 2)

	byARN := make(map[string]types.ProjectDescription, len(out.ProjectDescriptions))
	for _, d := range out.ProjectDescriptions {
		byARN[*d.ProjectArn] = d
	}

	explicitDesc := byARN[*explicit.ProjectArn]
	assert.Equal(t, types.ProjectAutoUpdateEnabled, explicitDesc.AutoUpdate)
	assert.Equal(t, types.CustomizationFeatureContentModeration, explicitDesc.Feature)

	defaultDesc := byARN[*defaulted.ProjectArn]
	assert.Equal(t, types.CustomizationFeatureCustomLabels, defaultDesc.Feature)
}

// TestDescribeProjects_ProjectNamesFilter proves DescribeProjectsInput's
// ProjectNames filter actually restricts results. Before the fix, the
// handler read a fabricated "ProjectArns" key instead of the real
// "ProjectNames" (confirmed against serializers.go's
// awsAwsjson11_serializeOpDocumentDescribeProjectsInput, which has no
// ProjectArns member at all) -- a real client's filter was silently
// ignored and every call returned every project.
func TestDescribeProjects_ProjectNamesFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestRekognitionClient(t, h)

	for _, name := range []string{"filter-proj-a", "filter-proj-b", "filter-proj-c"} {
		_, err := client.CreateProject(t.Context(), &rekognitionsdk.CreateProjectInput{
			ProjectName: aws.String(name),
		})
		require.NoError(t, err)
	}

	out, err := client.DescribeProjects(t.Context(), &rekognitionsdk.DescribeProjectsInput{
		ProjectNames: []string{"filter-proj-b"},
	})
	require.NoError(t, err)
	require.Len(t, out.ProjectDescriptions, 1)
	assert.Contains(t, *out.ProjectDescriptions[0].ProjectArn, "filter-proj-b")
}

// TestDescribeProjects_FeaturesFilter proves DescribeProjectsInput.Features
// (api_op_DescribeProjects.go: "Specifies the type of customization to
// filter projects by. If no value is specified, CUSTOM_LABELS is used as a
// default.") is honoured, including its documented default.
func TestDescribeProjects_FeaturesFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestRekognitionClient(t, h)

	_, err := client.CreateProject(t.Context(), &rekognitionsdk.CreateProjectInput{
		ProjectName: aws.String("feat-labels"),
		Feature:     types.CustomizationFeatureCustomLabels,
	})
	require.NoError(t, err)

	_, err = client.CreateProject(t.Context(), &rekognitionsdk.CreateProjectInput{
		ProjectName: aws.String("feat-moderation"),
		Feature:     types.CustomizationFeatureContentModeration,
	})
	require.NoError(t, err)

	byDefault, err := client.DescribeProjects(t.Context(), &rekognitionsdk.DescribeProjectsInput{})
	require.NoError(t, err)
	require.Len(t, byDefault.ProjectDescriptions, 1, "an absent Features filter must default to CUSTOM_LABELS only")
	assert.Contains(t, *byDefault.ProjectDescriptions[0].ProjectArn, "feat-labels")

	moderation, err := client.DescribeProjects(t.Context(), &rekognitionsdk.DescribeProjectsInput{
		Features: []types.CustomizationFeature{types.CustomizationFeatureContentModeration},
	})
	require.NoError(t, err)
	require.Len(t, moderation.ProjectDescriptions, 1)
	assert.Contains(t, *moderation.ProjectDescriptions[0].ProjectArn, "feat-moderation")

	both, err := client.DescribeProjects(t.Context(), &rekognitionsdk.DescribeProjectsInput{
		Features: []types.CustomizationFeature{
			types.CustomizationFeatureCustomLabels,
			types.CustomizationFeatureContentModeration,
		},
	})
	require.NoError(t, err)
	assert.Len(t, both.ProjectDescriptions, 2)
}

// TestListDatasetEntries_Filters proves ListDatasetEntriesInput's ContainsLabels/
// Labeled/SourceRefContains/HasErrors filters (api_op_ListDatasetEntries.go) are
// honoured -- previously none of the four were read by the handler at all.
func TestListDatasetEntries_Filters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestRekognitionClient(t, h)

	proj, err := client.CreateProject(t.Context(), &rekognitionsdk.CreateProjectInput{
		ProjectName: aws.String("dataset-filter-proj"),
	})
	require.NoError(t, err)

	ds, err := client.CreateDataset(t.Context(), &rekognitionsdk.CreateDatasetInput{
		ProjectArn:  proj.ProjectArn,
		DatasetType: types.DatasetTypeTrain,
	})
	require.NoError(t, err)

	entries := [][]byte{
		[]byte(`{"source-ref":"s3://bucket/cats/img1.jpg","labels-metadata":{"class-name":"cat"}}`),
		[]byte(`{"source-ref":"s3://bucket/dogs/img2.jpg","labels-metadata":{"class-name":"dog"}}`),
		[]byte(`{"source-ref":"s3://bucket/unlabeled/img3.jpg"}`),
	}
	for _, e := range entries {
		_, err = client.UpdateDatasetEntries(t.Context(), &rekognitionsdk.UpdateDatasetEntriesInput{
			DatasetArn: ds.DatasetArn,
			Changes:    &types.DatasetChanges{GroundTruth: e},
		})
		require.NoError(t, err)
	}

	byLabel, err := client.ListDatasetEntries(t.Context(), &rekognitionsdk.ListDatasetEntriesInput{
		DatasetArn:     ds.DatasetArn,
		ContainsLabels: []string{"cat"},
	})
	require.NoError(t, err)
	require.Len(t, byLabel.DatasetEntries, 1)
	assert.Contains(t, byLabel.DatasetEntries[0], "img1.jpg")

	labeledOnly, err := client.ListDatasetEntries(t.Context(), &rekognitionsdk.ListDatasetEntriesInput{
		DatasetArn: ds.DatasetArn,
		Labeled:    aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Len(t, labeledOnly.DatasetEntries, 2)

	unlabeledOnly, err := client.ListDatasetEntries(t.Context(), &rekognitionsdk.ListDatasetEntriesInput{
		DatasetArn: ds.DatasetArn,
		Labeled:    aws.Bool(false),
	})
	require.NoError(t, err)
	require.Len(t, unlabeledOnly.DatasetEntries, 1)
	assert.Contains(t, unlabeledOnly.DatasetEntries[0], "img3.jpg")

	bySourceRef, err := client.ListDatasetEntries(t.Context(), &rekognitionsdk.ListDatasetEntriesInput{
		DatasetArn:        ds.DatasetArn,
		SourceRefContains: aws.String("dogs"),
	})
	require.NoError(t, err)
	require.Len(t, bySourceRef.DatasetEntries, 1)
	assert.Contains(t, bySourceRef.DatasetEntries[0], "img2.jpg")

	// This backend has no entry-level error concept -- HasErrors=true must
	// return an honestly empty result, not fabricated error entries.
	withErrors, err := client.ListDatasetEntries(t.Context(), &rekognitionsdk.ListDatasetEntriesInput{
		DatasetArn: ds.DatasetArn,
		HasErrors:  aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Empty(t, withErrors.DatasetEntries)
}

// TestListFaces_Filters proves ListFacesInput's FaceIds and UserId filters
// (api_op_ListFaces.go) are honoured -- previously neither was read by the
// handler at all.
func TestListFaces_Filters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestRekognitionClient(t, h)

	_, err := client.CreateCollection(t.Context(), &rekognitionsdk.CreateCollectionInput{
		CollectionId: aws.String("listfaces-filter-coll"),
	})
	require.NoError(t, err)

	img := &types.Image{Bytes: []byte("fake-image-bytes")}

	faceIDs := make([]string, 0, 3)

	for range 3 {
		out, indexErr := client.IndexFaces(t.Context(), &rekognitionsdk.IndexFacesInput{
			CollectionId: aws.String("listfaces-filter-coll"),
			Image:        img,
		})
		require.NoError(t, indexErr)
		require.Len(t, out.FaceRecords, 1)
		faceIDs = append(faceIDs, aws.ToString(out.FaceRecords[0].Face.FaceId))
	}

	byFaceIDs, err := client.ListFaces(t.Context(), &rekognitionsdk.ListFacesInput{
		CollectionId: aws.String("listfaces-filter-coll"),
		FaceIds:      faceIDs[:2],
	})
	require.NoError(t, err)
	require.Len(t, byFaceIDs.Faces, 2)

	_, err = client.CreateUser(t.Context(), &rekognitionsdk.CreateUserInput{
		CollectionId: aws.String("listfaces-filter-coll"),
		UserId:       aws.String("listfaces-user"),
	})
	require.NoError(t, err)

	_, err = client.AssociateFaces(t.Context(), &rekognitionsdk.AssociateFacesInput{
		CollectionId: aws.String("listfaces-filter-coll"),
		UserId:       aws.String("listfaces-user"),
		FaceIds:      faceIDs[:1],
	})
	require.NoError(t, err)

	byUser, err := client.ListFaces(t.Context(), &rekognitionsdk.ListFacesInput{
		CollectionId: aws.String("listfaces-filter-coll"),
		UserId:       aws.String("listfaces-user"),
	})
	require.NoError(t, err)
	require.Len(t, byUser.Faces, 1)
	assert.Equal(t, faceIDs[0], aws.ToString(byUser.Faces[0].FaceId))
}
