package sesv2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateImportJob tests the CreateImportJob operation.
func TestCreateImportJob(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodPost, "/v2/email/import-jobs", map[string]any{
		"ImportDataSource": map[string]any{
			"S3Url":      "s3://bucket/key.csv",
			"DataFormat": "CSV",
		},
		"ImportDestination": map[string]any{
			"SuppressionListDestination": map[string]any{
				"SuppressionListImportAction": "PUT",
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestGetImportJob tests the GetImportJob operation.
func TestGetImportJob(t *testing.T) {
	t.Parallel()

	h := newHandler()

	createRec := doRequest(t, h, http.MethodPost, "/v2/email/import-jobs", map[string]any{
		"ImportDataSource": map[string]any{
			"S3Url":      "s3://bucket/key.csv",
			"DataFormat": "CSV",
		},
		"ImportDestination": map[string]any{
			"SuppressionListDestination": map[string]any{
				"SuppressionListImportAction": "PUT",
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	jobID := createResp["JobId"].(string)

	rec := doRequest(t, h, http.MethodGet, "/v2/email/import-jobs/"+jobID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestCreateImportJobWireShape asserts on the raw HTTP response to
// CreateImportJob for a real client's request shape (ImportDataSource/
// ImportDestination, both nested structs -- not the flat invented "DataSource"
// string this handler used to expect). A body serializing DataSource as an
// empty/absent field parses identically either way, which is how gopherstack
// silently accepted structurally-wrong requests before; asserting the 400 and
// its message is what catches a regression to that shape.
func TestCreateImportJobWireShape(t *testing.T) {
	t.Parallel()

	validDataSource := map[string]any{"S3Url": "s3://bucket/key.csv", "DataFormat": "CSV"}

	tests := []struct {
		body               map[string]any
		name               string
		wantContactList    string
		wantSuppressAction string
		wantStatus         int
	}{
		{
			name: "suppression list destination",
			body: map[string]any{
				"ImportDataSource": validDataSource,
				"ImportDestination": map[string]any{
					"SuppressionListDestination": map[string]any{"SuppressionListImportAction": "PUT"},
				},
			},
			wantStatus:         http.StatusOK,
			wantSuppressAction: "PUT",
		},
		{
			name: "contact list destination",
			body: map[string]any{
				"ImportDataSource": validDataSource,
				"ImportDestination": map[string]any{
					"ContactListDestination": map[string]any{
						"ContactListImportAction": "DELETE",
						"ContactListName":         "newsletter",
					},
				},
			},
			wantStatus:      http.StatusOK,
			wantContactList: "newsletter",
		},
		{
			name:       "old flat invented DataSource field is rejected",
			body:       map[string]any{"DataSource": "s3://bucket/key.csv"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing import data source",
			body: map[string]any{
				"ImportDestination": map[string]any{
					"SuppressionListDestination": map[string]any{"SuppressionListImportAction": "PUT"},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "import data source missing s3 url",
			body: map[string]any{
				"ImportDataSource": map[string]any{"DataFormat": "CSV"},
				"ImportDestination": map[string]any{
					"SuppressionListDestination": map[string]any{"SuppressionListImportAction": "PUT"},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing import destination",
			body:       map[string]any{"ImportDataSource": validDataSource},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "both destination branches set",
			body: map[string]any{
				"ImportDataSource": validDataSource,
				"ImportDestination": map[string]any{
					"ContactListDestination": map[string]any{
						"ContactListImportAction": "PUT",
						"ContactListName":         "x",
					},
					"SuppressionListDestination": map[string]any{"SuppressionListImportAction": "PUT"},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "neither destination branch set",
			body: map[string]any{
				"ImportDataSource":  validDataSource,
				"ImportDestination": map[string]any{},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "contact list destination missing name",
			body: map[string]any{
				"ImportDataSource": validDataSource,
				"ImportDestination": map[string]any{
					"ContactListDestination": map[string]any{"ContactListImportAction": "PUT"},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := doRequest(t, h, http.MethodPost, "/v2/email/import-jobs", tt.body)
			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus != http.StatusOK {
				return
			}

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			jobID, ok := createResp["JobId"].(string)
			require.True(t, ok)

			getRec := doRequest(t, h, http.MethodGet, "/v2/email/import-jobs/"+jobID, nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
			dest, ok := getResp["ImportDestination"].(map[string]any)
			require.True(t, ok)

			if tt.wantSuppressAction != "" {
				sld, sldOK := dest["SuppressionListDestination"].(map[string]any)
				require.True(t, sldOK)
				assert.Equal(t, tt.wantSuppressAction, sld["SuppressionListImportAction"])
			}

			if tt.wantContactList != "" {
				cld, cldOK := dest["ContactListDestination"].(map[string]any)
				require.True(t, cldOK)
				assert.Equal(t, tt.wantContactList, cld["ContactListName"])
			}
		})
	}
}

// TestListImportJobs tests the ListImportJobs operation, served as
// POST /v2/email/import-jobs/list (filter/pagination in the JSON body).
func TestListImportJobs(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodPost, "/v2/email/import-jobs/list", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
}
