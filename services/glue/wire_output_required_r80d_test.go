package glue_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// TestCreateCatalog_RequiredName proves CreateCatalog reads the real client's
// top-level Name (serializers.go's
// awsAwsjson11_serializeOpDocumentCreateCatalogInput puts Name as a sibling
// of CatalogInput, not a member of it -- types.CatalogInput has no Name field
// at all) and that GetCatalog/GetCatalogs echo it back. Catalog.Name is
// required (types/types.go); before this fix the handler read a nonexistent
// CatalogInput.Name, so every catalog created by a real client was stored
// with an empty name and, combined with the omitempty tag on the wire view,
// the required key vanished from the response entirely.
func TestCreateCatalog_RequiredName(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	_, err := client.CreateCatalog(t.Context(), &gluesdk.CreateCatalogInput{
		Name:         aws.String("child-catalog"),
		CatalogInput: &types.CatalogInput{},
	})
	require.NoError(t, err)

	got, err := client.GetCatalog(t.Context(), &gluesdk.GetCatalogInput{CatalogId: aws.String("child-catalog")})
	require.NoError(t, err)
	require.NotNil(t, got.Catalog.Name, "Catalog.Name must decode non-nil")
	require.Equal(t, "child-catalog", aws.ToString(got.Catalog.Name))

	list, err := client.GetCatalogs(t.Context(), &gluesdk.GetCatalogsInput{})
	require.NoError(t, err)
	require.Len(t, list.CatalogList, 1)
	require.NotNil(t, list.CatalogList[0].Name, "GetCatalogs CatalogList[].Name must decode non-nil")
	require.Equal(t, "child-catalog", aws.ToString(list.CatalogList[0].Name))
}

// TestColumnStatisticsForTable_RequiredColumnType proves GetColumnStatisticsForTable
// keeps the required ColumnType key even when a real client updates statistics
// with an empty (but non-nil) ColumnType -- the real client's own
// validateColumnStatistics only rejects a nil pointer, not an empty string, so
// this state is reachable without bypassing client-side validation.
// types.ColumnStatistics.ColumnType is required (types/types.go).
func TestColumnStatisticsForTable_RequiredColumnType(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	_, err := backend.CreateDatabase(glue.DatabaseInput{Name: "db"}, nil)
	require.NoError(t, err)

	_, err = client.UpdateColumnStatisticsForTable(t.Context(), &gluesdk.UpdateColumnStatisticsForTableInput{
		DatabaseName: aws.String("db"),
		TableName:    aws.String("t"),
		ColumnStatisticsList: []types.ColumnStatistics{
			{
				ColumnName:   aws.String("col1"),
				ColumnType:   aws.String(""),
				AnalyzedTime: aws.Time(time.Now()),
				StatisticsData: &types.ColumnStatisticsData{
					Type: types.ColumnStatisticsTypeString,
				},
			},
		},
	})
	require.NoError(t, err)

	out, err := client.GetColumnStatisticsForTable(t.Context(), &gluesdk.GetColumnStatisticsForTableInput{
		DatabaseName: aws.String("db"),
		TableName:    aws.String("t"),
		ColumnNames:  []string{"col1"},
	})
	require.NoError(t, err)
	require.Len(t, out.ColumnStatisticsList, 1)
	require.NotNil(t, out.ColumnStatisticsList[0].ColumnType, "ColumnStatistics.ColumnType must decode non-nil")
	require.Empty(t, aws.ToString(out.ColumnStatisticsList[0].ColumnType))
}

// TestClassifier_RequiredMembers proves CreateClassifier/GetClassifier keep
// each classifier sub-type's required members present even when a real
// client supplies an empty (but non-nil) value -- the real client's own
// per-request validators (validateCreateGrokClassifierRequest/
// validateCreateXMLClassifierRequest/validateCreateJsonClassifierRequest)
// only reject a nil pointer, never an empty string, so this state is
// reachable without bypassing client-side validation. GrokClassifier.
// Classification/GrokPattern, XMLClassifier.Classification and
// JsonClassifier.JsonPath are all required (types/types.go).
func TestClassifier_RequiredMembers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func() *gluesdk.CreateClassifierInput
		check  func(t *testing.T, c *types.Classifier)
		name   string
	}{
		{
			name: "grok classification and pattern",
			create: func() *gluesdk.CreateClassifierInput {
				return &gluesdk.CreateClassifierInput{
					GrokClassifier: &types.CreateGrokClassifierRequest{
						Name:           aws.String("grok-cl"),
						Classification: aws.String(""),
						GrokPattern:    aws.String(""),
					},
				}
			},
			check: func(t *testing.T, c *types.Classifier) {
				t.Helper()
				require.NotNil(t, c.GrokClassifier)
				require.NotNil(t, c.GrokClassifier.Classification, "GrokClassifier.Classification must decode non-nil")
				require.NotNil(t, c.GrokClassifier.GrokPattern, "GrokClassifier.GrokPattern must decode non-nil")
			},
		},
		{
			name: "xml classification",
			create: func() *gluesdk.CreateClassifierInput {
				return &gluesdk.CreateClassifierInput{
					XMLClassifier: &types.CreateXMLClassifierRequest{
						Name:           aws.String("xml-cl"),
						Classification: aws.String(""),
					},
				}
			},
			check: func(t *testing.T, c *types.Classifier) {
				t.Helper()
				require.NotNil(t, c.XMLClassifier)
				require.NotNil(t, c.XMLClassifier.Classification, "XMLClassifier.Classification must decode non-nil")
			},
		},
		{
			name: "json path",
			create: func() *gluesdk.CreateClassifierInput {
				return &gluesdk.CreateClassifierInput{
					JsonClassifier: &types.CreateJsonClassifierRequest{
						Name:     aws.String("json-cl"),
						JsonPath: aws.String(""),
					},
				}
			},
			check: func(t *testing.T, c *types.Classifier) {
				t.Helper()
				require.NotNil(t, c.JsonClassifier)
				require.NotNil(t, c.JsonClassifier.JsonPath, "JsonClassifier.JsonPath must decode non-nil")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := glue.NewInMemoryBackend(testAccountID, testRegion)
			client := newTestGlueClient(t, glue.NewHandler(backend))

			in := tt.create()
			_, err := client.CreateClassifier(t.Context(), in)
			require.NoError(t, err)

			name := aws.ToString(firstClassifierName(in))
			out, err := client.GetClassifier(t.Context(), &gluesdk.GetClassifierInput{Name: aws.String(name)})
			require.NoError(t, err)
			tt.check(t, out.Classifier)
		})
	}
}

// firstClassifierName extracts the Name from whichever of CreateClassifierInput's
// four mutually-exclusive sub-requests is set, for looking the classifier back up.
func firstClassifierName(in *gluesdk.CreateClassifierInput) *string {
	switch {
	case in.GrokClassifier != nil:
		return in.GrokClassifier.Name
	case in.XMLClassifier != nil:
		return in.XMLClassifier.Name
	case in.JsonClassifier != nil:
		return in.JsonClassifier.Name
	case in.CsvClassifier != nil:
		return in.CsvClassifier.Name
	default:
		return nil
	}
}
