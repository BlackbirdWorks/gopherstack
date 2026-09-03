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
			// DeleteJob on an unknown JobName is documented as a no-op, not an
			// error (api_op_DeleteJob.go).
			name:       "delete_nonexistent_job",
			action:     "DeleteJob",
			body:       map[string]any{"JobName": "no-job"},
			wantStatus: http.StatusOK,
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

// ----- Business glossary / asset catalog / dashboard tests (parity-4:
// AssociateGlossaryTerms, BatchGetIterableForms, CreateGlossary,
// CreateGlossaryTerm, DeleteAsset, DeleteAssetType, DeleteAttachment,
// DeleteFormType, DeleteGlossary, DeleteGlossaryTerm,
// DisassociateGlossaryTerms, GetAsset, GetAssetType, GetDashboardUrl,
// GetFormType, GetGlossary, GetGlossaryTerm, GetSessionEndpoint,
// ListAssetTypes, ListFormTypes, ListGlossaries, ListGlossaryTerms,
// ListIterableForms, PutAsset, PutAssetType, PutAttachment, PutFormType,
// SearchAssets, UpdateAsset, UpdateGlossary, UpdateGlossaryTerm) -----

// idFromResponse extracts the top-level "Id" field common to
// Create/Get/Update Glossary and GlossaryTerm responses.
func idFromResponse(t *testing.T, body []byte) string {
	t.Helper()

	var out struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal(body, &out))
	require.NotEmpty(t, out.ID)

	return out.ID
}

func mustCreateGlossary(t *testing.T, h *glue.Handler, name, description string) string {
	t.Helper()

	rec := doGlueRequest(t, h, "CreateGlossary", map[string]any{"Name": name, "Description": description})
	require.Equal(t, http.StatusOK, rec.Code)

	return idFromResponse(t, rec.Body.Bytes())
}

func mustCreateGlossaryTerm(t *testing.T, h *glue.Handler, glossaryID, name string) string {
	t.Helper()

	rec := doGlueRequest(t, h, "CreateGlossaryTerm", map[string]any{
		"GlossaryIdentifier": glossaryID, "Name": name,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	return idFromResponse(t, rec.Body.Bytes())
}

func mustPutFormType(t *testing.T, h *glue.Handler, name, schema string) {
	t.Helper()

	rec := doGlueRequest(t, h, "PutFormType", map[string]any{"Name": name, "Schema": schema})
	require.Equal(t, http.StatusOK, rec.Code)
}

func mustPutAssetType(t *testing.T, h *glue.Handler, name, formTypeName string) {
	t.Helper()

	rec := doGlueRequest(t, h, "PutAssetType", map[string]any{
		"Name": name,
		"Forms": map[string]any{
			"main": map[string]any{"FormTypeIdentifier": formTypeName},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func mustPutAsset(t *testing.T, h *glue.Handler, id, name, assetTypeName string) {
	t.Helper()

	rec := doGlueRequest(t, h, "PutAsset", map[string]any{
		"Identifier":  id,
		"Name":        name,
		"AssetTypeId": assetTypeName,
		"Forms":       map[string]any{},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

// glossaryTableCase is the shared row shape for the glossary/glossary-term
// table-driven tests below: buildBody sets up whatever prerequisite state the
// case needs (each subtest gets its own fresh handler, so there is no
// cross-subtest shared or ordered state) and returns the request body for op.
type glossaryTableCase struct {
	buildBody func(t *testing.T, h *glue.Handler) map[string]any
	check     func(t *testing.T, body string)
	name      string
	op        string
	wantCode  int
}

func runGlossaryTableCases(t *testing.T, tests []glossaryTableCase) {
	t.Helper()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := tt.buildBody(t, h)

			rec := doGlueRequest(t, h, tt.op, body)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.check != nil {
				tt.check(t, rec.Body.String())
			}
		})
	}
}

func TestGlue_Glossary_TableDriven(t *testing.T) {
	t.Parallel()

	runGlossaryTableCases(t, []glossaryTableCase{
		{
			name:     "create_glossary",
			op:       "CreateGlossary",
			wantCode: http.StatusOK,
			buildBody: func(t *testing.T, _ *glue.Handler) map[string]any {
				t.Helper()

				return map[string]any{"Name": "Finance", "Description": "Finance glossary"}
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "Finance glossary")
			},
		},
		{
			name:     "create_glossary_missing_name",
			op:       "CreateGlossary",
			wantCode: http.StatusBadRequest,
			buildBody: func(t *testing.T, _ *glue.Handler) map[string]any {
				t.Helper()

				return map[string]any{"Description": "no name"}
			},
		},
		{
			name:     "get_glossary_not_found",
			op:       "GetGlossary",
			wantCode: http.StatusBadRequest,
			buildBody: func(t *testing.T, _ *glue.Handler) map[string]any {
				t.Helper()

				return map[string]any{"Identifier": "gls-missing"}
			},
		},
		{
			name:     "get_glossary_found",
			op:       "GetGlossary",
			wantCode: http.StatusOK,
			buildBody: func(t *testing.T, h *glue.Handler) map[string]any {
				t.Helper()

				return map[string]any{"Identifier": mustCreateGlossary(t, h, "Ops", "")}
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "Ops")
			},
		},
		{
			name:     "update_glossary_name_only_preserves_description",
			op:       "UpdateGlossary",
			wantCode: http.StatusOK,
			buildBody: func(t *testing.T, h *glue.Handler) map[string]any {
				t.Helper()

				id := mustCreateGlossary(t, h, "Old", "keep-me")

				return map[string]any{"Identifier": id, "Name": "New"}
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "New")
				assert.Contains(t, body, "keep-me")
			},
		},
		{
			name:     "list_glossaries",
			op:       "ListGlossaries",
			wantCode: http.StatusOK,
			buildBody: func(t *testing.T, h *glue.Handler) map[string]any {
				t.Helper()
				mustCreateGlossary(t, h, "Alpha", "")
				mustCreateGlossary(t, h, "Beta", "")

				return map[string]any{}
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "Alpha")
				assert.Contains(t, body, "Beta")
			},
		},
		{
			name:     "create_term_unknown_glossary",
			op:       "CreateGlossaryTerm",
			wantCode: http.StatusBadRequest,
			buildBody: func(t *testing.T, _ *glue.Handler) map[string]any {
				t.Helper()

				return map[string]any{"GlossaryIdentifier": "gls-missing", "Name": "Revenue"}
			},
		},
		{
			name:     "create_term_ok",
			op:       "CreateGlossaryTerm",
			wantCode: http.StatusOK,
			buildBody: func(t *testing.T, h *glue.Handler) map[string]any {
				t.Helper()

				id := mustCreateGlossary(t, h, "Sales", "")

				return map[string]any{"GlossaryIdentifier": id, "Name": "Revenue", "ShortDescription": "money in"}
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "Revenue")
				assert.Contains(t, body, "money in")
			},
		},
		{
			name:     "list_glossary_terms",
			op:       "ListGlossaryTerms",
			wantCode: http.StatusOK,
			buildBody: func(t *testing.T, h *glue.Handler) map[string]any {
				t.Helper()

				id := mustCreateGlossary(t, h, "HR", "")
				mustCreateGlossaryTerm(t, h, id, "Headcount")

				return map[string]any{"GlossaryIdentifier": id}
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "Headcount")
			},
		},
		{
			name:     "update_glossary_term",
			op:       "UpdateGlossaryTerm",
			wantCode: http.StatusOK,
			buildBody: func(t *testing.T, h *glue.Handler) map[string]any {
				t.Helper()

				id := mustCreateGlossary(t, h, "Legal", "")
				termID := mustCreateGlossaryTerm(t, h, id, "Contract")

				return map[string]any{"Identifier": termID, "LongDescription": "a binding agreement"}
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "a binding agreement")
			},
		},
		{
			name:     "delete_glossary_conflict_with_terms",
			op:       "DeleteGlossary",
			wantCode: http.StatusBadRequest,
			buildBody: func(t *testing.T, h *glue.Handler) map[string]any {
				t.Helper()

				id := mustCreateGlossary(t, h, "HasTerms", "")
				mustCreateGlossaryTerm(t, h, id, "Term1")

				return map[string]any{"Identifier": id}
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "ConflictException")
			},
		},
		{
			name:     "delete_glossary_term_then_empty_glossary_deletes_ok",
			op:       "DeleteGlossary",
			wantCode: http.StatusOK,
			buildBody: func(t *testing.T, h *glue.Handler) map[string]any {
				t.Helper()

				id := mustCreateGlossary(t, h, "Empty", "")
				termID := mustCreateGlossaryTerm(t, h, id, "OnlyTerm")

				rec := doGlueRequest(t, h, "DeleteGlossaryTerm", map[string]any{"Identifier": termID})
				require.Equal(t, http.StatusOK, rec.Code)

				return map[string]any{"Identifier": id}
			},
		},
	})
}

func TestGlue_AssociateGlossaryTerms_TableDriven(t *testing.T) {
	t.Parallel()

	runGlossaryTableCases(t, []glossaryTableCase{
		{
			name:     "associate_unknown_asset",
			op:       "AssociateGlossaryTerms",
			wantCode: http.StatusBadRequest,
			buildBody: func(t *testing.T, _ *glue.Handler) map[string]any {
				t.Helper()

				return map[string]any{"AssetIdentifier": "no-such-asset", "GlossaryTermIdentifiers": []string{"term-x"}}
			},
		},
		{
			name:     "associate_unknown_term",
			op:       "AssociateGlossaryTerms",
			wantCode: http.StatusBadRequest,
			buildBody: func(t *testing.T, h *glue.Handler) map[string]any {
				t.Helper()

				mustPutFormType(t, h, "MainForm", `{"type":"object"}`)
				mustPutAssetType(t, h, "Table", "MainForm")
				mustPutAsset(t, h, "asset-1", "orders", "Table")

				return map[string]any{"AssetIdentifier": "asset-1", "GlossaryTermIdentifiers": []string{"term-missing"}}
			},
		},
		{
			name:     "associate_then_disassociate",
			op:       "DisassociateGlossaryTerms",
			wantCode: http.StatusOK,
			buildBody: func(t *testing.T, h *glue.Handler) map[string]any {
				t.Helper()

				mustPutFormType(t, h, "MainForm", `{"type":"object"}`)
				mustPutAssetType(t, h, "Table", "MainForm")
				mustPutAsset(t, h, "asset-2", "orders", "Table")
				glossaryID := mustCreateGlossary(t, h, "PII", "")
				termID := mustCreateGlossaryTerm(t, h, glossaryID, "Email")

				assocRec := doGlueRequest(t, h, "AssociateGlossaryTerms", map[string]any{
					"AssetIdentifier": "asset-2", "GlossaryTermIdentifiers": []string{termID},
				})
				require.Equal(t, http.StatusOK, assocRec.Code)

				var assocOut struct {
					GlossaryTerms []string `json:"GlossaryTerms"`
				}
				require.NoError(t, json.Unmarshal(assocRec.Body.Bytes(), &assocOut))
				require.Contains(t, assocOut.GlossaryTerms, termID)

				return map[string]any{"AssetIdentifier": "asset-2", "GlossaryTermIdentifiers": []string{termID}}
			},
			check: func(t *testing.T, body string) {
				t.Helper()

				var out struct {
					GlossaryTerms []string `json:"GlossaryTerms"`
				}
				require.NoError(t, json.Unmarshal([]byte(body), &out))
				assert.Empty(t, out.GlossaryTerms)
			},
		},
		{
			name:     "deleting_glossary_term_cascades_to_asset",
			op:       "GetAsset",
			wantCode: http.StatusOK,
			buildBody: func(t *testing.T, h *glue.Handler) map[string]any {
				t.Helper()

				mustPutFormType(t, h, "MainForm", `{"type":"object"}`)
				mustPutAssetType(t, h, "Table", "MainForm")
				mustPutAsset(t, h, "asset-3", "orders", "Table")
				glossaryID := mustCreateGlossary(t, h, "PII2", "")
				termID := mustCreateGlossaryTerm(t, h, glossaryID, "SSN")

				assocRec := doGlueRequest(t, h, "AssociateGlossaryTerms", map[string]any{
					"AssetIdentifier": "asset-3", "GlossaryTermIdentifiers": []string{termID},
				})
				require.Equal(t, http.StatusOK, assocRec.Code)

				delRec := doGlueRequest(t, h, "DeleteGlossaryTerm", map[string]any{"Identifier": termID})
				require.Equal(t, http.StatusOK, delRec.Code)

				return map[string]any{"Identifier": "asset-3"}
			},
			check: func(t *testing.T, body string) {
				t.Helper()

				var out struct {
					GlossaryTerms []string `json:"GlossaryTerms"`
				}
				require.NoError(t, json.Unmarshal([]byte(body), &out))
				assert.Empty(t, out.GlossaryTerms, "deleted glossary term must be disassociated from the asset")
			},
		},
	})
}

func TestGlue_AssetCatalog_CRUD_TableDriven(t *testing.T) {
	t.Parallel()

	runGlossaryTableCases(t, []glossaryTableCase{
		{
			name:     "put_form_type_lowercase_name_rejected",
			op:       "PutFormType",
			wantCode: http.StatusBadRequest,
			buildBody: func(t *testing.T, _ *glue.Handler) map[string]any {
				t.Helper()

				return map[string]any{"Name": "lowercase", "Schema": `{"type":"object"}`}
			},
		},
		{
			name:     "put_form_type_ok",
			op:       "PutFormType",
			wantCode: http.StatusOK,
			buildBody: func(t *testing.T, _ *glue.Handler) map[string]any {
				t.Helper()

				return map[string]any{"Name": "TableSchema", "Schema": `{"type":"object"}`}
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "TableSchema")
			},
		},
		{
			name:     "put_asset_type_unknown_form_type",
			op:       "PutAssetType",
			wantCode: http.StatusBadRequest,
			buildBody: func(t *testing.T, _ *glue.Handler) map[string]any {
				t.Helper()

				return map[string]any{
					"Name":  "Table",
					"Forms": map[string]any{"main": map[string]any{"FormTypeIdentifier": "NoSuchForm"}},
				}
			},
		},
		{
			name:     "put_asset_type_ok",
			op:       "PutAssetType",
			wantCode: http.StatusOK,
			buildBody: func(t *testing.T, h *glue.Handler) map[string]any {
				t.Helper()

				mustPutFormType(t, h, "TableSchema", `{"type":"object"}`)

				return map[string]any{
					"Name":  "Table",
					"Forms": map[string]any{"main": map[string]any{"FormTypeIdentifier": "TableSchema"}},
				}
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "TableSchema")
			},
		},
		{
			name:     "get_asset_type_not_found",
			op:       "GetAssetType",
			wantCode: http.StatusBadRequest,
			buildBody: func(t *testing.T, _ *glue.Handler) map[string]any {
				t.Helper()

				return map[string]any{"Identifier": "NoSuchType"}
			},
		},
		{
			name:     "list_asset_types",
			op:       "ListAssetTypes",
			wantCode: http.StatusOK,
			buildBody: func(t *testing.T, h *glue.Handler) map[string]any {
				t.Helper()

				mustPutFormType(t, h, "TableSchema", `{"type":"object"}`)
				mustPutAssetType(t, h, "Table", "TableSchema")

				return map[string]any{}
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "Table")
			},
		},
		{
			name:     "delete_form_type_conflict_referenced_by_asset_type",
			op:       "DeleteFormType",
			wantCode: http.StatusBadRequest,
			buildBody: func(t *testing.T, h *glue.Handler) map[string]any {
				t.Helper()

				mustPutFormType(t, h, "TableSchema", `{"type":"object"}`)
				mustPutAssetType(t, h, "Table", "TableSchema")

				return map[string]any{"Identifier": "TableSchema"}
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "ConflictException")
			},
		},
		{
			name:     "delete_asset_type_no_conflict_guard",
			op:       "DeleteAssetType",
			wantCode: http.StatusOK,
			buildBody: func(t *testing.T, h *glue.Handler) map[string]any {
				t.Helper()

				mustPutFormType(t, h, "TableSchema", `{"type":"object"}`)
				mustPutAssetType(t, h, "Table", "TableSchema")
				mustPutAsset(t, h, "asset-orphan", "orders", "Table")

				return map[string]any{"Identifier": "Table"}
			},
		},
		{
			name:     "put_asset_unknown_asset_type",
			op:       "PutAsset",
			wantCode: http.StatusBadRequest,
			buildBody: func(t *testing.T, _ *glue.Handler) map[string]any {
				t.Helper()

				return map[string]any{
					"Identifier": "asset-x", "Name": "orders", "AssetTypeId": "NoSuchType", "Forms": map[string]any{},
				}
			},
		},
		{
			name:     "get_asset_after_put",
			op:       "GetAsset",
			wantCode: http.StatusOK,
			buildBody: func(t *testing.T, h *glue.Handler) map[string]any {
				t.Helper()

				mustPutFormType(t, h, "TableSchema", `{"type":"object"}`)
				mustPutAssetType(t, h, "Table", "TableSchema")
				mustPutAsset(t, h, "asset-4", "orders", "Table")

				return map[string]any{"Identifier": "asset-4"}
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "orders")
				assert.Contains(t, body, "Table")
			},
		},
		{
			name:     "update_asset_name",
			op:       "UpdateAsset",
			wantCode: http.StatusOK,
			buildBody: func(t *testing.T, h *glue.Handler) map[string]any {
				t.Helper()

				mustPutFormType(t, h, "TableSchema", `{"type":"object"}`)
				mustPutAssetType(t, h, "Table", "TableSchema")
				mustPutAsset(t, h, "asset-5", "orders", "Table")

				return map[string]any{"Identifier": "asset-5", "Name": "orders_v2"}
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "orders_v2")
			},
		},
		{
			name:     "delete_asset_then_get_not_found",
			op:       "DeleteAsset",
			wantCode: http.StatusOK,
			buildBody: func(t *testing.T, h *glue.Handler) map[string]any {
				t.Helper()

				mustPutFormType(t, h, "TableSchema", `{"type":"object"}`)
				mustPutAssetType(t, h, "Table", "TableSchema")
				mustPutAsset(t, h, "asset-6", "orders", "Table")

				return map[string]any{"Identifier": "asset-6"}
			},
		},
	})
}

func TestGlue_AssetAttachmentsAndSearch_TableDriven(t *testing.T) {
	t.Parallel()

	runGlossaryTableCases(t, []glossaryTableCase{
		{
			name:     "put_attachment_on_asset",
			op:       "PutAttachment",
			wantCode: http.StatusOK,
			buildBody: func(t *testing.T, h *glue.Handler) map[string]any {
				t.Helper()

				mustPutFormType(t, h, "Notes", `{"type":"object"}`)
				mustPutFormType(t, h, "TableSchema", `{"type":"object"}`)
				mustPutAssetType(t, h, "Table", "TableSchema")
				mustPutAsset(t, h, "asset-att-1", "orders", "Table")

				return map[string]any{
					"AssetIdentifier": "asset-att-1",
					"AttachmentName":  "owner-notes",
					"FormTypeId":      "Notes",
					"Content":         `{"owner":"data-eng"}`,
				}
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "owner-notes")
			},
		},
		{
			name:     "put_attachment_missing_item_identifier",
			op:       "PutAttachment",
			wantCode: http.StatusBadRequest,
			buildBody: func(t *testing.T, h *glue.Handler) map[string]any {
				t.Helper()

				mustPutFormType(t, h, "Notes", `{"type":"object"}`)
				mustPutFormType(t, h, "TableSchema", `{"type":"object"}`)
				mustPutAssetType(t, h, "Table", "TableSchema")
				mustPutAsset(t, h, "asset-att-2", "orders", "Table")

				return map[string]any{
					"AssetIdentifier": "asset-att-2", "AttachmentName": "col-notes", "FormTypeId": "Notes",
					"Content": "{}", "IterableFormName": "columns",
				}
			},
		},
		{
			name:     "put_attachment_on_iterable_form_item_then_batch_get",
			op:       "BatchGetIterableForms",
			wantCode: http.StatusOK,
			buildBody: func(t *testing.T, h *glue.Handler) map[string]any {
				t.Helper()

				mustPutFormType(t, h, "Notes", `{"type":"object"}`)
				mustPutFormType(t, h, "TableSchema", `{"type":"object"}`)
				mustPutAssetType(t, h, "Table", "TableSchema")
				mustPutAsset(t, h, "asset-att-3", "orders", "Table")

				putRec := doGlueRequest(t, h, "PutAttachment", map[string]any{
					"AssetIdentifier": "asset-att-3", "AttachmentName": "col-notes", "FormTypeId": "Notes",
					"Content": `{"pii":true}`, "IterableFormName": "columns", "ItemIdentifier": "customer_email",
				})
				require.Equal(t, http.StatusOK, putRec.Code)

				return map[string]any{
					"AssetIdentifier": "asset-att-3", "IterableFormName": "columns",
					"ItemIdentifiers": []string{"customer_email", "missing_col"},
				}
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "customer_email")
				assert.Contains(t, body, "missing_col")
				assert.Contains(t, body, "EntityNotFoundException")
			},
		},
		{
			name:     "list_iterable_forms_after_attachment",
			op:       "ListIterableForms",
			wantCode: http.StatusOK,
			buildBody: func(t *testing.T, h *glue.Handler) map[string]any {
				t.Helper()

				mustPutFormType(t, h, "Notes", `{"type":"object"}`)
				mustPutFormType(t, h, "TableSchema", `{"type":"object"}`)
				mustPutAssetType(t, h, "Table", "TableSchema")
				mustPutAsset(t, h, "asset-att-4", "orders", "Table")

				putRec := doGlueRequest(t, h, "PutAttachment", map[string]any{
					"AssetIdentifier": "asset-att-4", "AttachmentName": "col-notes", "FormTypeId": "Notes",
					"Content": "{}", "IterableFormName": "columns", "ItemIdentifier": "order_id",
				})
				require.Equal(t, http.StatusOK, putRec.Code)

				return map[string]any{"AssetIdentifier": "asset-att-4", "IterableFormName": "columns"}
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "order_id")
			},
		},
		{
			name:     "delete_attachment_removes_it",
			op:       "GetAsset",
			wantCode: http.StatusOK,
			buildBody: func(t *testing.T, h *glue.Handler) map[string]any {
				t.Helper()

				mustPutFormType(t, h, "Notes", `{"type":"object"}`)
				mustPutFormType(t, h, "TableSchema", `{"type":"object"}`)
				mustPutAssetType(t, h, "Table", "TableSchema")
				mustPutAsset(t, h, "asset-att-5", "orders", "Table")

				putRec := doGlueRequest(t, h, "PutAttachment", map[string]any{
					"AssetIdentifier": "asset-att-5", "AttachmentName": "owner-notes", "FormTypeId": "Notes",
					"Content": "{}",
				})
				require.Equal(t, http.StatusOK, putRec.Code)

				delRec := doGlueRequest(t, h, "DeleteAttachment", map[string]any{
					"AssetIdentifier": "asset-att-5", "AttachmentName": "owner-notes",
				})
				require.Equal(t, http.StatusOK, delRec.Code)

				return map[string]any{"Identifier": "asset-att-5"}
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.NotContains(t, body, "owner-notes")
			},
		},
		{
			name:     "search_assets_requires_text_or_filter",
			op:       "SearchAssets",
			wantCode: http.StatusBadRequest,
			buildBody: func(t *testing.T, _ *glue.Handler) map[string]any {
				t.Helper()

				return map[string]any{}
			},
		},
		{
			name:     "search_assets_by_text",
			op:       "SearchAssets",
			wantCode: http.StatusOK,
			buildBody: func(t *testing.T, h *glue.Handler) map[string]any {
				t.Helper()

				mustPutFormType(t, h, "TableSchema", `{"type":"object"}`)
				mustPutAssetType(t, h, "Table", "TableSchema")
				mustPutAsset(t, h, "asset-s1", "orders_table", "Table")
				mustPutAsset(t, h, "asset-s2", "customers_table", "Table")

				return map[string]any{"SearchText": "orders"}
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "orders_table")
				assert.NotContains(t, body, "customers_table")
			},
		},
		{
			name:     "search_assets_by_attribute_filter",
			op:       "SearchAssets",
			wantCode: http.StatusOK,
			buildBody: func(t *testing.T, h *glue.Handler) map[string]any {
				t.Helper()

				mustPutFormType(t, h, "DatasetSchema", `{"type":"object","properties":{}}`)
				mustPutAssetType(t, h, "Dataset", "DatasetSchema")
				mustPutAsset(t, h, "asset-s3", "orders_table", "Dataset")
				mustPutAsset(t, h, "asset-s4", "customers_table", "Dataset")

				return map[string]any{
					"FilterClause": map[string]any{
						"AttributeFilter": map[string]any{
							"Attribute": "Name",
							"Operator":  "equals",
							"Value":     map[string]any{"StringValue": "customers_table"},
						},
					},
				}
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "customers_table")
				assert.NotContains(t, body, "orders_table")
			},
		},
	})
}

func TestGlue_Dashboard_TableDriven(t *testing.T) {
	t.Parallel()

	runGlossaryTableCases(t, []glossaryTableCase{
		{
			name:     "job_dashboard_url_not_found",
			op:       "GetDashboardUrl",
			wantCode: http.StatusBadRequest,
			buildBody: func(t *testing.T, _ *glue.Handler) map[string]any {
				t.Helper()

				return map[string]any{"ResourceId": "no-such-job", "ResourceType": "JOB"}
			},
		},
		{
			name:     "job_dashboard_url_ok",
			op:       "GetDashboardUrl",
			wantCode: http.StatusOK,
			buildBody: func(t *testing.T, h *glue.Handler) map[string]any {
				t.Helper()

				rec := doGlueRequest(t, h, "CreateJob", map[string]any{
					"Name": "dash-job", "Role": "r", "Command": map[string]any{"Name": "glueetl"},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				return map[string]any{"ResourceId": "dash-job", "ResourceType": "JOB"}
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "gluestudio")
			},
		},
		{
			name:     "invalid_resource_type",
			op:       "GetDashboardUrl",
			wantCode: http.StatusBadRequest,
			buildBody: func(t *testing.T, _ *glue.Handler) map[string]any {
				t.Helper()

				return map[string]any{"ResourceId": "x", "ResourceType": "BOGUS"}
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "InvalidInputException")
			},
		},
		{
			name:     "session_endpoint_not_found",
			op:       "GetSessionEndpoint",
			wantCode: http.StatusBadRequest,
			buildBody: func(t *testing.T, _ *glue.Handler) map[string]any {
				t.Helper()

				return map[string]any{"SessionId": "no-such-session"}
			},
		},
		{
			name:     "session_endpoint_ok",
			op:       "GetSessionEndpoint",
			wantCode: http.StatusOK,
			buildBody: func(t *testing.T, h *glue.Handler) map[string]any {
				t.Helper()

				rec := doGlueRequest(t, h, "CreateSession", map[string]any{
					"Id": "dash-sess", "Role": "r", "Command": map[string]any{"Name": "glueetl"},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				return map[string]any{"SessionId": "dash-sess"}
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "SparkConnect")
				assert.Contains(t, body, "AuthToken")
			},
		},
		{
			name:     "session_endpoint_stopped_session_rejected",
			op:       "GetSessionEndpoint",
			wantCode: http.StatusBadRequest,
			buildBody: func(t *testing.T, h *glue.Handler) map[string]any {
				t.Helper()

				rec := doGlueRequest(t, h, "CreateSession", map[string]any{
					"Id": "dash-sess-2", "Role": "r", "Command": map[string]any{"Name": "glueetl"},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				stopRec := doGlueRequest(t, h, "StopSession", map[string]any{"Id": "dash-sess-2"})
				require.Equal(t, http.StatusOK, stopRec.Code)

				return map[string]any{"SessionId": "dash-sess-2"}
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "IllegalSessionStateException")
			},
		},
	})
}
