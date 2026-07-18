package ram_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ram"
)

// TestRAMPagination_ListPrincipals covers principal pagination.
func TestListPrincipals_Pagination(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")
	h := ram.NewHandler(b)

	// Create a share with AllowExternalPrincipals so we can associate different external principals.
	createRec := doRAMRequest(t, h, "/createresourceshare", map[string]any{
		"name":                    "principal-share",
		"allowExternalPrincipals": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp struct {
		ResourceShare struct {
			ResourceShareArn string `json:"resourceShareArn"`
		} `json:"resourceShare"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	shareARN := createResp.ResourceShare.ResourceShareArn
	require.NotEmpty(t, shareARN)

	// Associate 4 different external account IDs as principals.
	for i := range 4 {
		principal := fmt.Sprintf("%012d", 111111111111+i)
		body := map[string]any{
			"resourceShareArn": shareARN,
			"principals":       []string{principal},
		}
		rec := doRAMRequest(t, h, "/associateresourceshare", body)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		maxResults *int32
		name       string
		wantTotal  int
		wantPages  int
		wantError  bool
	}{
		{
			name:       "maxResults=2 paginates 4 principals into 2 pages",
			maxResults: ptr32(2),
			wantTotal:  4,
			wantPages:  2,
		},
		{
			name:       "maxResults=4 returns all in one page",
			maxResults: ptr32(4),
			wantTotal:  4,
			wantPages:  1,
		},
		{
			name:       "maxResults=-1 returns error",
			maxResults: ptr32(-1),
			wantError:  true,
		},
	}

	type reqBody struct {
		ResourceOwner string `json:"resourceOwner"`
		MaxResults    *int32 `json:"maxResults,omitempty"`
		NextToken     string `json:"nextToken,omitempty"`
	}

	type respBody struct {
		NextToken  string `json:"nextToken"`
		Principals []any  `json:"principals"`
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			nextToken := ""
			totalSeen := 0
			pages := 0

			for {
				req := reqBody{
					ResourceOwner: "SELF",
					MaxResults:    tc.maxResults,
					NextToken:     nextToken,
				}

				rec := doRAMRequest(t, h, "/listprincipals", req)

				if tc.wantError {
					assert.Equal(t, http.StatusBadRequest, rec.Code)

					var errResp map[string]string
					require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
					assert.Equal(t, "InvalidParameterException", errResp["__type"])

					return
				}

				require.Equal(t, http.StatusOK, rec.Code)

				var resp respBody
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				pages++
				totalSeen += len(resp.Principals)
				nextToken = resp.NextToken

				if nextToken == "" {
					break
				}
			}

			assert.Equal(t, tc.wantTotal, totalSeen)
			assert.Equal(t, tc.wantPages, pages)
		})
	}
}

func TestListPrincipals_ResourceOwnerFilter(t *testing.T) {
	t.Parallel()

	const (
		selfAccount = "000000000000"
		// Use same-account format so AllowExternalPrincipals=false doesn't block.
		principal = "111111111111"
	)

	tests := []struct {
		name        string
		filterOwner string
		wantCount   int
	}{
		{
			name:        "SELF returns principals on own shares",
			filterOwner: "SELF",
			wantCount:   1,
		},
		{
			name:        "OTHER-ACCOUNTS returns empty for own share",
			filterOwner: "OTHER-ACCOUNTS",
			wantCount:   0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ram.NewInMemoryBackend(selfAccount, "us-east-1")
			h := ram.NewHandler(b)

			shareARN := "arn:aws:ram:us-east-1:000000000000:resource-share/principal-test"
			rs := ram.NewTestResourceShare(shareARN, "principal-share")
			rs.AllowExternalPrincipals = true
			ram.AddResourceShareInternal(b, rs)

			_, err := b.AssociateResourceShare(shareARN, []string{principal}, nil)
			require.NoError(t, err)

			rec := doRAMRequest(t, h, "/listprincipals", map[string]any{
				"resourceOwner": tc.filterOwner,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			principals := resp["principals"].([]any)
			assert.Len(t, principals, tc.wantCount)
		})
	}
}
