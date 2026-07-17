package athena_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHandler_GetDatabase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       `{"CatalogName":"AwsDataCatalog","DatabaseName":"default"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "validation_no_catalog",
			body:       `{"CatalogName":""}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "validation_no_database",
			body:       `{"CatalogName":"x"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			body:       `{"CatalogName":"AwsDataCatalog","DatabaseName":"missing"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetDatabase", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListDatabases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       `{"CatalogName":"AwsDataCatalog"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "validation_no_catalog",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "ListDatabases", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetTableMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       `{"CatalogName":"AwsDataCatalog","DatabaseName":"default","TableName":"sample_table"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "validation_missing_fields",
			body:       `{"CatalogName":"x"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			body:       `{"CatalogName":"AwsDataCatalog","DatabaseName":"default","TableName":"missing"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetTableMetadata", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListTableMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains string
		wantExclude  string
		wantStatus   int
	}{
		{
			name:       "success",
			body:       `{"CatalogName":"AwsDataCatalog","DatabaseName":"default"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "filtered_include",
			body:       `{"CatalogName":"AwsDataCatalog","DatabaseName":"default","Expression":"sample"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:        "filtered_exclude",
			body:        `{"CatalogName":"AwsDataCatalog","DatabaseName":"default","Expression":"nope"}`,
			wantStatus:  http.StatusOK,
			wantExclude: "sample_table",
		},
		{
			name:       "validation_no_catalog",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "ListTableMetadata", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContains)
			}

			if tt.wantExclude != "" {
				assert.NotContains(t, rec.Body.String(), tt.wantExclude)
			}
		})
	}
}

// --- Notebook tests ---
