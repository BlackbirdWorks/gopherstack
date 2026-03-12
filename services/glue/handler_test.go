package glue_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/glue"
)

const (
	testAccountID = "000000000000"
	testRegion    = "us-east-1"
)

func newTestHandler(t *testing.T) *glue.Handler {
	t.Helper()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)

	return glue.NewHandler(backend)
}

func doGlueRequest(t *testing.T, h *glue.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSGlue."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err = h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// ----- Provider tests -----

func TestGlue_Provider_Name(t *testing.T) {
	t.Parallel()

	p := &glue.Provider{}
	assert.Equal(t, "Glue", p.Name())
}

func TestGlue_Provider_Init(t *testing.T) {
	t.Parallel()

	p := &glue.Provider{}
	svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)
	assert.NotNil(t, svc)
	assert.Equal(t, "Glue", svc.Name())
}

// ----- Handler metadata tests -----

func TestGlue_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "Glue", h.Name())
}

func TestGlue_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateDatabase")
	assert.Contains(t, ops, "GetDatabase")
	assert.Contains(t, ops, "DeleteDatabase")
	assert.Contains(t, ops, "CreateCrawler")
	assert.Contains(t, ops, "GetJob")
}

func TestGlue_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, service.PriorityHeaderExact, h.MatchPriority())
}

func TestGlue_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	matcher := h.RouteMatcher()

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{
			name:   "glue_target",
			target: "AWSGlue.CreateDatabase",
			want:   true,
		},
		{
			name:   "other_target",
			target: "ElasticMapReduce.RunJobFlow",
			want:   false,
		},
		{
			name:   "empty_target",
			target: "",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, matcher(c))
		})
	}
}

// ----- Database CRUD tests -----

func TestGlue_CreateDatabase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      map[string]any
		setup      func(*glue.Handler)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			input: map[string]any{
				"DatabaseInput": map[string]any{
					"Name":        "test-db",
					"Description": "Test database",
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "duplicate",
			input: map[string]any{
				"DatabaseInput": map[string]any{
					"Name": "dup-db",
				},
			},
			wantStatus: http.StatusBadRequest,
			setup: func(h *glue.Handler) {
				rec := doGlueRequest(t, h, "CreateDatabase", map[string]any{
					"DatabaseInput": map[string]any{"Name": "dup-db"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doGlueRequest(t, h, "CreateDatabase", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestGlue_DatabaseLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doGlueRequest(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{
			"Name":        "mydb",
			"Description": "my database",
		},
		"Tags": map[string]string{"env": "test"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get
	rec = doGlueRequest(t, h, "GetDatabase", map[string]any{"Name": "mydb"})
	require.Equal(t, http.StatusOK, rec.Code)

	var getOut struct {
		Database struct {
			Name      string `json:"Name"`
			CatalogID string `json:"CatalogId"`
		} `json:"Database"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getOut))
	assert.Equal(t, "mydb", getOut.Database.Name)
	assert.Equal(t, testAccountID, getOut.Database.CatalogID)

	// GetDatabases
	rec = doGlueRequest(t, h, "GetDatabases", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var listOut struct {
		DatabaseList []struct {
			Name string `json:"Name"`
		} `json:"DatabaseList"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	assert.Len(t, listOut.DatabaseList, 1)

	// Delete
	rec = doGlueRequest(t, h, "DeleteDatabase", map[string]any{"Name": "mydb"})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get after delete - should 400
	rec = doGlueRequest(t, h, "GetDatabase", map[string]any{"Name": "mydb"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ----- Table tests -----

func TestGlue_TableLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create DB first
	rec := doGlueRequest(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "mydb"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Create table
	rec = doGlueRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "mydb",
		"TableInput": map[string]any{
			"Name":      "mytable",
			"TableType": "EXTERNAL_TABLE",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get table
	rec = doGlueRequest(t, h, "GetTable", map[string]any{
		"DatabaseName": "mydb",
		"Name":         "mytable",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var getOut struct {
		Table struct {
			Name string `json:"Name"`
		} `json:"Table"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getOut))
	assert.Equal(t, "mytable", getOut.Table.Name)

	// Update table
	rec = doGlueRequest(t, h, "UpdateTable", map[string]any{
		"DatabaseName": "mydb",
		"TableInput": map[string]any{
			"Name":        "mytable",
			"Description": "updated",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get tables
	rec = doGlueRequest(t, h, "GetTables", map[string]any{"DatabaseName": "mydb"})
	require.Equal(t, http.StatusOK, rec.Code)

	var listOut struct {
		TableList []struct {
			Name string `json:"Name"`
		} `json:"TableList"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	assert.Len(t, listOut.TableList, 1)

	// Delete table
	rec = doGlueRequest(t, h, "DeleteTable", map[string]any{
		"DatabaseName": "mydb",
		"Name":         "mytable",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get after delete
	rec = doGlueRequest(t, h, "GetTable", map[string]any{
		"DatabaseName": "mydb",
		"Name":         "mytable",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ----- Crawler tests -----

func TestGlue_CrawlerLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create DB first
	rec := doGlueRequest(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "crawlerdb"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Create crawler
	rec = doGlueRequest(t, h, "CreateCrawler", map[string]any{
		"Name":         "my-crawler",
		"Role":         "arn:aws:iam::000000000000:role/GlueRole",
		"DatabaseName": "crawlerdb",
		"Targets": map[string]any{
			"S3Targets": []map[string]any{{"Path": "s3://my-bucket"}},
		},
		"Tags": map[string]string{"env": "test"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get crawler
	rec = doGlueRequest(t, h, "GetCrawler", map[string]any{"Name": "my-crawler"})
	require.Equal(t, http.StatusOK, rec.Code)

	var getOut struct {
		Crawler struct {
			Name  string `json:"Name"`
			State string `json:"State"`
		} `json:"Crawler"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getOut))
	assert.Equal(t, "my-crawler", getOut.Crawler.Name)
	assert.Equal(t, "READY", getOut.Crawler.State)

	// GetCrawlers
	rec = doGlueRequest(t, h, "GetCrawlers", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var listOut struct {
		Crawlers []struct {
			Name string `json:"Name"`
		} `json:"Crawlers"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	assert.Len(t, listOut.Crawlers, 1)

	// UpdateCrawler
	rec = doGlueRequest(t, h, "UpdateCrawler", map[string]any{
		"Name":         "my-crawler",
		"Role":         "arn:aws:iam::000000000000:role/GlueRole2",
		"DatabaseName": "crawlerdb",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// DeleteCrawler
	rec = doGlueRequest(t, h, "DeleteCrawler", map[string]any{"Name": "my-crawler"})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get after delete
	rec = doGlueRequest(t, h, "GetCrawler", map[string]any{"Name": "my-crawler"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ----- Job tests -----

func TestGlue_JobLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create job
	rec := doGlueRequest(t, h, "CreateJob", map[string]any{
		"Name": "my-job",
		"Role": "arn:aws:iam::000000000000:role/GlueRole",
		"Command": map[string]any{
			"Name":           "glueetl",
			"ScriptLocation": "s3://my-bucket/script.py",
		},
		"GlueVersion": "4.0",
		"WorkerType":  "G.1X",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut struct {
		Name string `json:"Name"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	assert.Equal(t, "my-job", createOut.Name)

	// GetJob
	rec = doGlueRequest(t, h, "GetJob", map[string]any{"JobName": "my-job"})
	require.Equal(t, http.StatusOK, rec.Code)

	var getOut struct {
		Job struct {
			Name string `json:"Name"`
		} `json:"Job"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getOut))
	assert.Equal(t, "my-job", getOut.Job.Name)

	// GetJobs
	rec = doGlueRequest(t, h, "GetJobs", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var listOut struct {
		Jobs []struct {
			Name string `json:"Name"`
		} `json:"Jobs"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	assert.Len(t, listOut.Jobs, 1)

	// UpdateJob
	rec = doGlueRequest(t, h, "UpdateJob", map[string]any{
		"JobName": "my-job",
		"JobUpdate": map[string]any{
			"Role":        "arn:aws:iam::000000000000:role/GlueRole2",
			"GlueVersion": "4.0",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var updateOut struct {
		JobName string `json:"JobName"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateOut))
	assert.Equal(t, "my-job", updateOut.JobName)

	// DeleteJob
	rec = doGlueRequest(t, h, "DeleteJob", map[string]any{"JobName": "my-job"})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get after delete
	rec = doGlueRequest(t, h, "GetJob", map[string]any{"JobName": "my-job"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ----- Tag tests -----

func TestGlue_Tags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create DB
	rec := doGlueRequest(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "tagdb"},
		"Tags":          map[string]string{"key1": "value1"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// GetDatabase to find ARN
	rec = doGlueRequest(t, h, "GetDatabase", map[string]any{"Name": "tagdb"})
	require.Equal(t, http.StatusOK, rec.Code)

	var dbOut struct {
		Database struct {
			ARN string `json:"Arn"`
		} `json:"Database"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &dbOut))
	assert.NotEmpty(t, dbOut.Database.ARN)

	// GetTags
	rec = doGlueRequest(t, h, "GetTags", map[string]any{"ResourceArn": dbOut.Database.ARN})
	require.Equal(t, http.StatusOK, rec.Code)

	var tagsOut struct {
		Tags map[string]string `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagsOut))
	assert.Equal(t, "value1", tagsOut.Tags["key1"])

	// TagResource
	rec = doGlueRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": dbOut.Database.ARN,
		"TagsToAdd":   map[string]string{"key2": "value2"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// UntagResource
	rec = doGlueRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn":  dbOut.Database.ARN,
		"TagsToRemove": []string{"key1"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify tags
	rec = doGlueRequest(t, h, "GetTags", map[string]any{"ResourceArn": dbOut.Database.ARN})
	require.Equal(t, http.StatusOK, rec.Code)

	var finalTagsOut struct {
		Tags map[string]string `json:"Tags"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &finalTagsOut))
	assert.NotContains(t, finalTagsOut.Tags, "key1")
	assert.Equal(t, "value2", finalTagsOut.Tags["key2"])
}

// ----- Error cases -----

func TestGlue_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doGlueRequest(t, h, "NonExistentOperation", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestGlue_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name   string
		target string
		want   string
	}{
		{
			name:   "valid",
			target: "AWSGlue.CreateDatabase",
			want:   "CreateDatabase",
		},
		{
			name:   "empty",
			target: "",
			want:   "Unknown",
		},
		{
			name:   "other_service",
			target: "ElasticMapReduce.RunJobFlow",
			want:   "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

func TestGlue_ChaosMetadata(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "glue", h.ChaosServiceName())
	assert.Equal(t, []string{testRegion}, h.ChaosRegions())

	ops := h.ChaosOperations()
	assert.Contains(t, ops, "CreateDatabase")
	assert.Contains(t, ops, "GetJob")
}

func TestGlue_BackendMetadata(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend(testAccountID, testRegion)
	assert.Equal(t, testRegion, b.Region())
	assert.Equal(t, testAccountID, b.AccountID())
}

func TestGlue_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name     string
		body     map[string]any
		wantPath string
	}{
		{
			name:     "by_name",
			body:     map[string]any{"Name": "my-db"},
			wantPath: "my-db",
		},
		{
			name:     "by_resource_arn",
			body:     map[string]any{"ResourceArn": "arn:aws:glue:us-east-1:123:database/db1"},
			wantPath: "arn:aws:glue:us-east-1:123:database/db1",
		},
		{
			name:     "by_database_name",
			body:     map[string]any{"DatabaseName": "my-db"},
			wantPath: "my-db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.wantPath, h.ExtractResource(c))
		})
	}
}

func TestGlue_UpdateDatabase(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create DB
	rec := doGlueRequest(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "updatedb"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Update
	rec = doGlueRequest(t, h, "UpdateDatabase", map[string]any{
		"Name":          "updatedb",
		"DatabaseInput": map[string]any{"Name": "updatedb", "Description": "updated"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Update non-existent
	rec = doGlueRequest(t, h, "UpdateDatabase", map[string]any{
		"Name":          "nonexistent",
		"DatabaseInput": map[string]any{"Name": "nonexistent"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestGlue_CrawlerTagsLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create DB and crawler
	rec := doGlueRequest(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "tagcrawlerdb"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "CreateCrawler", map[string]any{
		"Name":         "tagcrawler",
		"Role":         "arn:aws:iam::000000000000:role/GlueRole",
		"DatabaseName": "tagcrawlerdb",
		"Tags":         map[string]string{"env": "test"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get crawler ARN
	rec = doGlueRequest(t, h, "GetCrawler", map[string]any{"Name": "tagcrawler"})
	require.Equal(t, http.StatusOK, rec.Code)

	var crawlerOut struct {
		Crawler struct {
			ARN string `json:"Arn"`
		} `json:"Crawler"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &crawlerOut))

	// Tag crawler
	rec = doGlueRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": crawlerOut.Crawler.ARN,
		"TagsToAdd":   map[string]string{"extra": "tag"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// GetTags for crawler
	rec = doGlueRequest(t, h, "GetTags", map[string]any{"ResourceArn": crawlerOut.Crawler.ARN})
	require.Equal(t, http.StatusOK, rec.Code)

	var tagsOut struct {
		Tags map[string]string `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagsOut))
	assert.Equal(t, "tag", tagsOut.Tags["extra"])
	assert.Equal(t, "test", tagsOut.Tags["env"])

	// Untag crawler
	rec = doGlueRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn":  crawlerOut.Crawler.ARN,
		"TagsToRemove": []string{"env"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestGlue_JobTagsLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create job with tags
	rec := doGlueRequest(t, h, "CreateJob", map[string]any{
		"Name": "tagjob",
		"Role": "arn:aws:iam::000000000000:role/GlueRole",
		"Command": map[string]any{
			"Name":           "glueetl",
			"ScriptLocation": "s3://bucket/script.py",
		},
		"Tags": map[string]string{"env": "prod"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get job ARN
	rec = doGlueRequest(t, h, "GetJob", map[string]any{"JobName": "tagjob"})
	require.Equal(t, http.StatusOK, rec.Code)

	var jobOut struct {
		Job struct {
			ARN string `json:"Arn"`
		} `json:"Job"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &jobOut))

	// Tag job
	rec = doGlueRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": jobOut.Job.ARN,
		"TagsToAdd":   map[string]string{"team": "data"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// GetTags for job
	rec = doGlueRequest(t, h, "GetTags", map[string]any{"ResourceArn": jobOut.Job.ARN})
	require.Equal(t, http.StatusOK, rec.Code)

	var tagsOut struct {
		Tags map[string]string `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagsOut))
	assert.Equal(t, "prod", tagsOut.Tags["env"])
	assert.Equal(t, "data", tagsOut.Tags["team"])

	// Untag job
	rec = doGlueRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn":  jobOut.Job.ARN,
		"TagsToRemove": []string{"env"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestGlue_ErrorCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		action     string
		wantStatus int
	}{
		{
			name:       "get_nonexistent_database",
			action:     "GetDatabase",
			body:       map[string]any{"Name": "no-such-db"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "delete_nonexistent_database",
			action:     "DeleteDatabase",
			body:       map[string]any{"Name": "no-such-db"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:   "get_nonexistent_table",
			action: "GetTable",
			body: map[string]any{
				"DatabaseName": "no-db",
				"Name":         "no-table",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "get_nonexistent_crawler",
			action:     "GetCrawler",
			body:       map[string]any{"Name": "no-crawler"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "delete_nonexistent_crawler",
			action:     "DeleteCrawler",
			body:       map[string]any{"Name": "no-crawler"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "get_nonexistent_job",
			action:     "GetJob",
			body:       map[string]any{"JobName": "no-job"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "delete_nonexistent_job",
			action:     "DeleteJob",
			body:       map[string]any{"JobName": "no-job"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestGlue_Provider_InitWithConfig(t *testing.T) {
	t.Parallel()

	p := &glue.Provider{}
	svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)
	require.NotNil(t, svc)

	h, ok := svc.(*glue.Handler)
	require.True(t, ok)
	assert.Equal(t, "Glue", h.Name())
	// Verify the backend is initialized with non-empty region and account
	assert.NotEmpty(t, h.Backend.Region())
	assert.NotEmpty(t, h.Backend.AccountID())
}
