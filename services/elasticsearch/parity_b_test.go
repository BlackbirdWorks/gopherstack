package elasticsearch_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticsearch"
)

// createPackageAndGetID creates an Elasticsearch TXT-DICTIONARY package and returns its ID.
func createPackageAndGetID(t *testing.T, h *elasticsearch.Handler, pkgName string) string {
	t.Helper()

	resp := doRequest(t, h, http.MethodPost, "/2015-01-01/packages", map[string]any{
		"PackageName": pkgName,
		"PackageType": "TXT-DICTIONARY",
	})
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	details := out["PackageDetails"].(map[string]any)

	return details["PackageID"].(string)
}

func TestParity_AssociatePackage_DuplicateRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		domainName string
		wantCode   int
	}{
		{
			name:       "first_association_succeeds",
			domainName: "parity-dom-first",
			wantCode:   http.StatusOK,
		},
		{
			name:       "second_association_rejected",
			domainName: "parity-dom-dup",
			wantCode:   http.StatusConflict,
		},
		{
			name:       "different_package_allowed",
			domainName: "parity-dom-diff",
			wantCode:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createDomainAndGetARN(t, h, tt.domainName)

			switch tt.name {
			case "first_association_succeeds":
				pkgID := createPackageAndGetID(t, h, "pkg-first")
				resp := doRequest(
					t,
					h,
					http.MethodPost,
					"/2015-01-01/packages/associate/"+pkgID+"/"+tt.domainName,
					nil,
				)
				resp.Body.Close()
				assert.Equal(t, tt.wantCode, resp.StatusCode)

			case "second_association_rejected":
				pkgID := createPackageAndGetID(t, h, "pkg-dup")
				// First association must succeed.
				first := doRequest(
					t,
					h,
					http.MethodPost,
					"/2015-01-01/packages/associate/"+pkgID+"/"+tt.domainName,
					nil,
				)
				first.Body.Close()
				require.Equal(t, http.StatusOK, first.StatusCode)
				// Second association of the same package to the same domain must fail.
				second := doRequest(
					t,
					h,
					http.MethodPost,
					"/2015-01-01/packages/associate/"+pkgID+"/"+tt.domainName,
					nil,
				)
				second.Body.Close()
				assert.Equal(t, tt.wantCode, second.StatusCode)

			case "different_package_allowed":
				pkgA := createPackageAndGetID(t, h, "pkg-a")
				pkgB := createPackageAndGetID(t, h, "pkg-b")
				respA := doRequest(
					t,
					h,
					http.MethodPost,
					"/2015-01-01/packages/associate/"+pkgA+"/"+tt.domainName,
					nil,
				)
				respA.Body.Close()
				require.Equal(t, http.StatusOK, respA.StatusCode)
				respB := doRequest(
					t,
					h,
					http.MethodPost,
					"/2015-01-01/packages/associate/"+pkgB+"/"+tt.domainName,
					nil,
				)
				respB.Body.Close()
				assert.Equal(t, tt.wantCode, respB.StatusCode)
			}
		})
	}
}
