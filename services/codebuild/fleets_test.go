package codebuild_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateFleet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			body: map[string]any{
				"name":            "my-fleet",
				"baseCapacity":    2,
				"computeType":     "BUILD_GENERAL1_SMALL",
				"environmentType": "LINUX_CONTAINER",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_name",
			body:       map[string]any{"baseCapacity": 2},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "duplicate_fails",
			body: map[string]any{
				"name":         "dup-fleet",
				"baseCapacity": 1,
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "duplicate_fails" {
				rec := doRequest(t, h, "CreateFleet", tt.body)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "CreateFleet", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_BatchGetFleets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setupFleets  []string
		queryNames   []string
		wantFound    int
		wantNotFound int
		wantStatus   int
	}{
		{
			name:         "returns_fleet",
			setupFleets:  []string{"fleet-a"},
			queryNames:   []string{"fleet-a"},
			wantFound:    1,
			wantNotFound: 0,
			wantStatus:   http.StatusOK,
		},
		{
			name:         "not_found_in_fleetsNotFound",
			setupFleets:  []string{},
			queryNames:   []string{"ghost-fleet"},
			wantFound:    0,
			wantNotFound: 1,
			wantStatus:   http.StatusOK,
		},
		{
			name:         "mixed_found_and_not_found",
			setupFleets:  []string{"fleet-x"},
			queryNames:   []string{"fleet-x", "ghost"},
			wantFound:    1,
			wantNotFound: 1,
			wantStatus:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, fn := range tt.setupFleets {
				rec := doRequest(t, h, "CreateFleet", map[string]any{
					"name":         fn,
					"baseCapacity": 1,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "BatchGetFleets", map[string]any{"names": tt.queryNames})
			require.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

			fleets, _ := out["fleets"].([]any)
			assert.Len(t, fleets, tt.wantFound)

			notFound, _ := out["fleetsNotFound"].([]any)
			assert.Len(t, notFound, tt.wantNotFound)
		})
	}
}

// TestCodeBuild_Fleet covers UpdateFleet.
func TestCodeBuild_Fleet(t *testing.T) {
	t.Parallel()

	t.Run("update_fleet", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createRec := doRequest(t, h, "CreateFleet", map[string]any{
			"name":         "upd-fleet",
			"baseCapacity": 2,
		})
		require.Equal(t, http.StatusOK, createRec.Code)

		var createOut struct {
			Fleet struct {
				Arn          string `json:"arn"`
				BaseCapacity int    `json:"baseCapacity"`
			} `json:"fleet"`
		}
		require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))
		fleetArn := createOut.Fleet.Arn

		updRec := doRequest(t, h, "UpdateFleet", map[string]any{
			"arn":          fleetArn,
			"baseCapacity": 5,
		})
		require.Equal(t, http.StatusOK, updRec.Code)

		var updOut struct {
			Fleet struct {
				BaseCapacity int `json:"baseCapacity"`
			} `json:"fleet"`
		}
		require.NoError(t, json.NewDecoder(updRec.Body).Decode(&updOut))
		assert.Equal(t, 5, updOut.Fleet.BaseCapacity)
	})

	t.Run("update_fleet_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "UpdateFleet", map[string]any{
			"arn":          "arn:aws:codebuild:us-east-1:000000000000:fleet/ghost",
			"baseCapacity": 1,
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

// TestHandler_CreateFleet_StatusSchema tests expanded fleet schema fields.
func TestHandler_CreateFleet_StatusSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fleetBody        map[string]any
		name             string
		wantStatusCode   string
		wantBaseCapacity float64
	}{
		{
			name: "fleet_has_status_struct",
			fleetBody: map[string]any{
				"name":            "status-fleet",
				"baseCapacity":    2,
				"computeType":     "BUILD_GENERAL1_MEDIUM",
				"environmentType": "LINUX_CONTAINER",
			},
			wantBaseCapacity: 2,
			wantStatusCode:   "ACTIVE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateFleet", tt.fleetBody)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Fleet struct {
					Status struct {
						StatusCode string `json:"statusCode"`
					} `json:"status"`
					BaseCapacity float64 `json:"baseCapacity"`
				} `json:"fleet"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.InDelta(t, tt.wantBaseCapacity, out.Fleet.BaseCapacity, 0)
			assert.Equal(t, tt.wantStatusCode, out.Fleet.Status.StatusCode)
		})
	}
}

// TestHandler_DeleteFleet_RemovesFleet verifies DeleteFleet actually removes the fleet.
func TestHandler_DeleteFleet_RemovesFleet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createName string
		deleteArn  string
		wantDelete int
		wantList   int
	}{
		{
			name:       "delete_removes_fleet",
			createName: "fleet-to-delete",
			wantDelete: http.StatusOK,
			wantList:   0,
		},
		{
			name:       "delete_missing_fleet_returns_404",
			deleteArn:  "arn:aws:codebuild:us-east-1:000000000000:fleet/ghost-fleet",
			wantDelete: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var fleetArn string
			if tt.createName != "" {
				createRec := doRequest(t, h, "CreateFleet", map[string]any{
					"name":            tt.createName,
					"baseCapacity":    1,
					"computeType":     "BUILD_GENERAL1_SMALL",
					"environmentType": "LINUX_CONTAINER",
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				var createOut struct {
					Fleet struct {
						Arn string `json:"arn"`
					} `json:"fleet"`
				}
				require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))
				fleetArn = createOut.Fleet.Arn
			} else {
				fleetArn = tt.deleteArn
			}

			deleteRec := doRequest(t, h, "DeleteFleet", map[string]any{"arn": fleetArn})
			assert.Equal(t, tt.wantDelete, deleteRec.Code)

			if tt.wantList == 0 && tt.createName != "" {
				listRec := doRequest(t, h, "ListFleets", nil)
				require.Equal(t, http.StatusOK, listRec.Code)

				var listOut struct {
					Fleets []string `json:"fleets"`
				}
				require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
				assert.Empty(t, listOut.Fleets, "fleet should be removed from list after delete")
			}
		})
	}
}
