package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetMapping_DerivesFromRealSchema verifies GetMapping reads the source (and
// optional sink) table schema from the catalog rather than returning a
// hardcoded empty mapping.
func TestGetMapping_DerivesFromRealSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
		wantLen  int
	}{
		{
			name:     "missing_source_returns_400",
			input:    map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "unknown_source_table_returns_400",
			input: map[string]any{
				"Source": map[string]any{"DatabaseName": "nodb", "TableName": "notable"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "source_only_maps_identity",
			input: map[string]any{
				"Source": map[string]any{"DatabaseName": "db", "TableName": "src"},
			},
			wantCode: http.StatusOK,
			wantLen:  2,
		},
		{
			name: "source_and_sink_maps_across_tables",
			input: map[string]any{
				"Source": map[string]any{"DatabaseName": "db", "TableName": "src"},
				"Sinks":  []map[string]any{{"DatabaseName": "db", "TableName": "sink"}},
			},
			wantCode: http.StatusOK,
			wantLen:  2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]any{"Name": "db"}})
			doGlueRequest(t, h, "CreateTable", map[string]any{
				"DatabaseName": "db",
				"TableInput": map[string]any{
					"Name": "src",
					"StorageDescriptor": map[string]any{"Columns": []map[string]any{
						{"Name": "id", "Type": "int"},
						{"Name": "name", "Type": "string"},
					}},
				},
			})
			doGlueRequest(t, h, "CreateTable", map[string]any{
				"DatabaseName": "db",
				"TableInput": map[string]any{
					"Name": "sink",
					"StorageDescriptor": map[string]any{"Columns": []map[string]any{
						{"Name": "id", "Type": "bigint"},
					}},
				},
			})

			rec := doGlueRequest(t, h, "GetMapping", tc.input)
			require.Equal(t, tc.wantCode, rec.Code)

			if tc.wantCode != http.StatusOK {
				return
			}

			var out struct {
				Mapping []map[string]any `json:"Mapping"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.Mapping, tc.wantLen)
			assert.Equal(t, "id", out.Mapping[0]["SourcePath"])
		})
	}
}

// TestTestConnection_ValidatesConnection verifies TestConnection checks that
// the named connection actually exists rather than always succeeding.
func TestTestConnection_ValidatesConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
	}{
		{name: "no_name_or_input_returns_400", input: map[string]any{}, wantCode: http.StatusBadRequest},
		{
			name:     "unknown_connection_returns_400",
			input:    map[string]any{"ConnectionName": "does-not-exist"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "known_connection_returns_200",
			input:    map[string]any{"ConnectionName": "my-conn"},
			wantCode: http.StatusOK,
		},
		{
			name: "both_name_and_adhoc_returns_400",
			input: map[string]any{
				"ConnectionName": "my-conn",
				"TestConnectionInput": map[string]any{
					"ConnectionType":       "JDBC",
					"ConnectionProperties": map[string]string{"JDBC_CONNECTION_URL": "jdbc:mysql://h/db"},
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "adhoc_missing_properties_returns_400",
			input: map[string]any{
				"TestConnectionInput": map[string]any{"ConnectionType": "JDBC"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "adhoc_well_formed_returns_200",
			input: map[string]any{
				"TestConnectionInput": map[string]any{
					"ConnectionType":       "JDBC",
					"ConnectionProperties": map[string]string{"JDBC_CONNECTION_URL": "jdbc:mysql://h/db"},
				},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateConnection", map[string]any{
				"ConnectionInput": map[string]any{
					"Name":                 "my-conn",
					"ConnectionType":       "JDBC",
					"ConnectionProperties": map[string]string{"JDBC_CONNECTION_URL": "jdbc:mysql://h/db"},
				},
			})

			rec := doGlueRequest(t, h, "TestConnection", tc.input)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// TestListCrawls_ReturnsRealCrawlHistory verifies ListCrawls surfaces the crawl
// history recorded by StartCrawler rather than always returning an empty list.
func TestListCrawls_ReturnsRealCrawlHistory(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]any{"Name": "db"}})
	doGlueRequest(t, h, "CreateCrawler", map[string]any{
		"Name": "my-crawler",
		"Role": "arn:aws:iam::000000000000:role/GlueCrawlerRole",
		"Targets": map[string]any{
			"S3Targets": []map[string]any{{"Path": "s3://my-bucket/data/"}},
		},
		"DatabaseName": "db",
	})

	rec := doGlueRequest(t, h, "ListCrawls", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "CrawlerName is required")

	rec = doGlueRequest(t, h, "ListCrawls", map[string]any{"CrawlerName": "no-such-crawler"})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "unknown crawler")

	rec = doGlueRequest(t, h, "ListCrawls", map[string]any{"CrawlerName": "my-crawler"})
	require.Equal(t, http.StatusOK, rec.Code)

	var before struct {
		Crawls []map[string]any `json:"Crawls"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &before))
	assert.Empty(t, before.Crawls, "no crawl has started yet")

	rec = doGlueRequest(t, h, "StartCrawler", map[string]any{"Name": "my-crawler"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "ListCrawls", map[string]any{"CrawlerName": "my-crawler"})
	require.Equal(t, http.StatusOK, rec.Code)

	var after struct {
		Crawls []map[string]any `json:"Crawls"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &after))
	require.Len(t, after.Crawls, 1)
	assert.NotEmpty(t, after.Crawls[0]["CrawlId"])
	assert.NotEmpty(t, after.Crawls[0]["StartTime"])
}

// TestGetDataQualityModel_RequiresProfileID verifies GetDataQualityModel parses
// and validates ProfileId rather than silently ignoring it.
func TestGetDataQualityModel_RequiresProfileID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "GetDataQualityModel", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec = doGlueRequest(t, h, "GetDataQualityModel", map[string]any{"ProfileId": "profile-1"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Status string `json:"Status"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "SUCCEEDED", out.Status)
}

// TestGetDataQualityModelResult_RequiresBothIDs verifies both ProfileId and
// StatisticId are required, matching the real (non-optional-StatisticId) SDK
// shape, and that the response uses the real Model/CompletedOn fields instead
// of a fabricated Status string.
func TestGetDataQualityModelResult_RequiresBothIDs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "GetDataQualityModelResult", map[string]any{"ProfileId": "p1"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec = doGlueRequest(
		t, h, "GetDataQualityModelResult", map[string]any{"ProfileId": "p1", "StatisticId": "s1"},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Contains(t, out, "Model")
	assert.NotContains(t, out, "Status", "GetDataQualityModelResult has no Status field in the real API")
}

// TestListTableOptimizerRuns_ReturnsRealHistory verifies ListTableOptimizerRuns
// validates required params and reads real run history seeded by
// CreateTableOptimizer.
func TestListTableOptimizerRuns_ReturnsRealHistory(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]any{"Name": "db"}})
	doGlueRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "db",
		"TableInput":   map[string]any{"Name": "tbl"},
	})

	rec := doGlueRequest(t, h, "ListTableOptimizerRuns", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "missing required params")

	rec = doGlueRequest(t, h, "ListTableOptimizerRuns", map[string]any{
		"DatabaseName": "db", "TableName": "tbl", "Type": "compaction",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "no optimizer created yet")

	rec = doGlueRequest(t, h, "CreateTableOptimizer", map[string]any{
		"CatalogId":    "123456789012",
		"DatabaseName": "db",
		"TableName":    "tbl",
		"Type":         "compaction",
		"TableOptimizerConfiguration": map[string]any{
			"RoleArn": "arn:aws:iam::123456789012:role/GlueRole",
			"Enabled": true,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "ListTableOptimizerRuns", map[string]any{
		"DatabaseName": "db", "TableName": "tbl", "Type": "compaction",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		TableOptimizerRuns []map[string]any `json:"TableOptimizerRuns"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.TableOptimizerRuns, 1)
	assert.Equal(t, "completed", out.TableOptimizerRuns[0]["eventType"])
}

// TestUpdateJobFromSourceControl_ValidatesJob verifies the job must exist and
// that a successful sync records real source-control state on the job.
func TestUpdateJobFromSourceControl_ValidatesJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "UpdateJobFromSourceControl", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "JobName required")

	rec = doGlueRequest(t, h, "UpdateJobFromSourceControl", map[string]any{"JobName": "no-such-job"})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "unknown job")

	doGlueRequest(t, h, "CreateJob", map[string]any{
		"Name":    "my-job",
		"Command": map[string]any{"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
		"Role":    "arn:aws:iam::123456789012:role/GlueRole",
	})

	rec = doGlueRequest(t, h, "UpdateJobFromSourceControl", map[string]any{
		"JobName":         "my-job",
		"Provider":        "GITHUB",
		"RepositoryName":  "my-repo",
		"RepositoryOwner": "my-org",
		"BranchName":      "main",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "GetJob", map[string]any{"JobName": "my-job"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Job struct {
			SourceControlDetails struct {
				Provider   string `json:"Provider"`
				Repository string `json:"Repository"`
				Branch     string `json:"Branch"`
			} `json:"SourceControlDetails"`
		} `json:"Job"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "GITHUB", out.Job.SourceControlDetails.Provider)
	assert.Equal(t, "my-repo", out.Job.SourceControlDetails.Repository)
	assert.Equal(t, "main", out.Job.SourceControlDetails.Branch)
}

// TestUpdateSourceControlFromJob_ValidatesJob mirrors
// TestUpdateJobFromSourceControl_ValidatesJob for the reverse-direction sync op.
func TestUpdateSourceControlFromJob_ValidatesJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "UpdateSourceControlFromJob", map[string]any{"JobName": "no-such-job"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	doGlueRequest(t, h, "CreateJob", map[string]any{
		"Name":    "my-job",
		"Command": map[string]any{"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
		"Role":    "arn:aws:iam::123456789012:role/GlueRole",
	})

	rec = doGlueRequest(t, h, "UpdateSourceControlFromJob", map[string]any{
		"JobName":  "my-job",
		"Provider": "GITHUB",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestGetResourcePolicies_ReturnsStoredPolicies verifies GetResourcePolicies
// aggregates the account-level and per-resource policies set via
// PutResourcePolicy.
func TestGetResourcePolicies_ReturnsStoredPolicies(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "GetResourcePolicies", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var empty struct {
		GetResourcePoliciesResponseList []map[string]any `json:"GetResourcePoliciesResponseList"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &empty))
	assert.Empty(t, empty.GetResourcePoliciesResponseList)

	doGlueRequest(t, h, "PutResourcePolicy", map[string]any{
		"PolicyInJson": `{"Version":"2012-10-17","Statement":[]}`,
	})
	doGlueRequest(t, h, "PutResourcePolicy", map[string]any{
		"PolicyInJson": `{"Version":"2012-10-17","Statement":[{"Sid":"x"}]}`,
		"ResourceArn":  "arn:aws:glue:us-east-1:123456789012:catalog",
	})

	rec = doGlueRequest(t, h, "GetResourcePolicies", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		GetResourcePoliciesResponseList []map[string]any `json:"GetResourcePoliciesResponseList"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.GetResourcePoliciesResponseList, 2)

	for _, p := range out.GetResourcePoliciesResponseList {
		assert.NotEmpty(t, p["PolicyInJson"])
		assert.NotEmpty(t, p["PolicyHash"])
	}
}

// TestListIntegrationResourceProperties_ReturnsStoredEntries verifies the op
// reads real entries created via CreateIntegrationResourceProperty and uses
// the real IntegrationResourcePropertyList wire field.
func TestListIntegrationResourceProperties_ReturnsStoredEntries(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "ListIntegrationResourceProperties", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var empty struct {
		IntegrationResourcePropertyList []map[string]any `json:"IntegrationResourcePropertyList"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &empty))
	assert.Empty(t, empty.IntegrationResourcePropertyList)

	doGlueRequest(t, h, "CreateIntegrationResourceProperty", map[string]any{
		"ResourceArn":      "arn:aws:glue:us-east-1:123456789012:connection/conn1",
		"SourceProperties": map[string]string{"key": "val"},
	})

	rec = doGlueRequest(t, h, "ListIntegrationResourceProperties", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		IntegrationResourcePropertyList []map[string]any `json:"IntegrationResourcePropertyList"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.IntegrationResourcePropertyList, 1)
	assert.Equal(t, "arn:aws:glue:us-east-1:123456789012:connection/conn1",
		out.IntegrationResourcePropertyList[0]["ResourceArn"])
}

// TestUpdateIntegrationResourceProperty_RequiresExistingEntry verifies the op
// mutates a previously created entry instead of no-op'ing.
func TestUpdateIntegrationResourceProperty_RequiresExistingEntry(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	arn := "arn:aws:glue:us-east-1:123456789012:connection/conn1"

	rec := doGlueRequest(t, h, "UpdateIntegrationResourceProperty", map[string]any{
		"ResourceArn":      arn,
		"SourceProperties": map[string]string{"a": "b"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "no entry created yet")

	doGlueRequest(t, h, "CreateIntegrationResourceProperty", map[string]any{
		"ResourceArn":      arn,
		"SourceProperties": map[string]string{"key": "original"},
	})

	rec = doGlueRequest(t, h, "UpdateIntegrationResourceProperty", map[string]any{
		"ResourceArn":      arn,
		"SourceProperties": map[string]string{"key": "updated"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "GetIntegrationResourceProperty", map[string]any{"ResourceArn": arn})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		SourceProperties map[string]string `json:"SourceProperties"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "updated", out.SourceProperties["key"])
}

// TestUpdateIntegrationTableProperties_RequiresExistingEntry mirrors
// TestUpdateIntegrationResourceProperty_RequiresExistingEntry for table props.
func TestUpdateIntegrationTableProperties_RequiresExistingEntry(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	arn := "arn:aws:glue:us-east-1:123456789012:connection/conn1"

	rec := doGlueRequest(t, h, "UpdateIntegrationTableProperties", map[string]any{
		"ResourceArn": arn,
		"TableName":   "tbl",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "no entry created yet")

	doGlueRequest(t, h, "CreateIntegrationTableProperties", map[string]any{
		"ResourceArn":       arn,
		"TableName":         "tbl",
		"SourceTableConfig": map[string]any{"key": "original"},
	})

	rec = doGlueRequest(t, h, "UpdateIntegrationTableProperties", map[string]any{
		"ResourceArn":       arn,
		"TableName":         "tbl",
		"SourceTableConfig": map[string]any{"key": "updated"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "GetIntegrationTableProperties", map[string]any{
		"ResourceArn": arn, "TableName": "tbl",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		SourceTableConfig map[string]string `json:"SourceTableConfig"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "updated", out.SourceTableConfig["key"])
}

// TestColumnStatisticsTaskRunSchedule_RequiresExistingSettings verifies
// Start/StopColumnStatisticsTaskRunSchedule mutate real schedule state tied to
// existing task settings instead of no-op'ing.
func TestColumnStatisticsTaskRunSchedule_RequiresExistingSettings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]any{"Name": "db"}})
	doGlueRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "db",
		"TableInput":   map[string]any{"Name": "tbl"},
	})

	rec := doGlueRequest(t, h, "StartColumnStatisticsTaskRunSchedule", map[string]any{
		"DatabaseName": "db", "TableName": "tbl",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "no task settings created yet")

	doGlueRequest(t, h, "CreateColumnStatisticsTaskSettings", map[string]any{
		"DatabaseName": "db",
		"TableName":    "tbl",
		"RoleArn":      "arn:aws:iam::123456789012:role/GlueRole",
	})

	rec = doGlueRequest(t, h, "StartColumnStatisticsTaskRunSchedule", map[string]any{
		"DatabaseName": "db", "TableName": "tbl",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "GetColumnStatisticsTaskSettings", map[string]any{
		"DatabaseName": "db", "TableName": "tbl",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		ColumnStatisticsTaskSettings struct {
			Schedule struct {
				State string `json:"State"`
			} `json:"Schedule"`
		} `json:"ColumnStatisticsTaskSettings"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "SCHEDULED", out.ColumnStatisticsTaskSettings.Schedule.State)

	rec = doGlueRequest(t, h, "StopColumnStatisticsTaskRunSchedule", map[string]any{
		"DatabaseName": "db", "TableName": "tbl",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "GetColumnStatisticsTaskSettings", map[string]any{
		"DatabaseName": "db", "TableName": "tbl",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "NOT_SCHEDULED", out.ColumnStatisticsTaskSettings.Schedule.State)
}

// TestDataQualityStatisticAnnotations_RealStore verifies
// PutDataQualityProfileAnnotation and BatchPutDataQualityStatisticAnnotation
// write real state that ListDataQualityStatisticAnnotations reads back.
func TestDataQualityStatisticAnnotations_RealStore(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "ListDataQualityStatisticAnnotations", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var empty struct {
		Annotations []map[string]any `json:"Annotations"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &empty))
	assert.Empty(t, empty.Annotations)

	rec = doGlueRequest(t, h, "PutDataQualityProfileAnnotation", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "missing required fields")

	rec = doGlueRequest(t, h, "PutDataQualityProfileAnnotation", map[string]any{
		"ProfileId": "profile-1", "InclusionAnnotation": "INCLUDE",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "BatchPutDataQualityStatisticAnnotation", map[string]any{
		"InclusionAnnotations": []map[string]any{
			{"ProfileId": "profile-1", "StatisticId": "stat-1", "InclusionAnnotation": "EXCLUDE"},
			{"StatisticId": "stat-2", "InclusionAnnotation": "EXCLUDE"}, // missing ProfileId: should fail
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var batchOut struct {
		FailedInclusionAnnotations []map[string]any `json:"FailedInclusionAnnotations"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &batchOut))
	require.Len(t, batchOut.FailedInclusionAnnotations, 1)
	assert.Equal(t, "stat-2", batchOut.FailedInclusionAnnotations[0]["StatisticId"])

	rec = doGlueRequest(t, h, "ListDataQualityStatisticAnnotations", map[string]any{"ProfileId": "profile-1"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Annotations []map[string]any `json:"Annotations"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Annotations, 2, "the profile-wide and the per-statistic annotation")

	rec = doGlueRequest(t, h, "ListDataQualityStatisticAnnotations", map[string]any{
		"ProfileId": "profile-1", "StatisticId": "stat-1",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Annotations, 1)
	assert.Equal(t, "stat-1", out.Annotations[0]["StatisticId"])
}
