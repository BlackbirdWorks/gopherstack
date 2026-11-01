package azurearm_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

type (
	putFn   func(rg, name string)
	countFn func(path string) int
	listRun func(t *testing.T, h *azurearm.Handler, put, del putFn, count countFn)
)

func vnetPath(rg, name string) string {
	return "/subscriptions/s1/resourceGroups/" + rg + "/providers/Microsoft.Network/applicationGateways/" + name +
		"?api-version=2023-01-01"
}

func TestGenericList_IndexFollowsWrites(t *testing.T) {
	t.Parallel()

	const (
		list  = "/subscriptions/s1/providers/Microsoft.Network/applicationGateways?api-version=2023-01-01"
		listG = "/subscriptions/s1/resourceGroups/RG1/providers/microsoft.network/ApplicationGateways?api-version=2023-01-01"
	)

	tests := []struct {
		run  listRun
		name string
	}{
		{
			name: "put_delete",
			run: func(t *testing.T, _ *azurearm.Handler, put putFn, del putFn, count countFn) {
				t.Helper()

				assert.Equal(t, 0, count(list))
				put("rg1", "a")
				put("rg2", "b")
				assert.Equal(t, 2, count(list))
				assert.Equal(t, 1, count(listG))
				del("rg1", "a")
				assert.Equal(t, 1, count(list))
				assert.Equal(t, 0, count(listG))
			},
		},
		{
			name: "group_cascade",
			run: func(t *testing.T, h *azurearm.Handler, put putFn, _ putFn, count countFn) {
				t.Helper()

				status, _ := doRequest(
					t,
					h,
					http.MethodPut,
					"/subscriptions/s1/resourceGroups/rg1?api-version=2021-04-01",
					[]byte(`{"location":"eastus"}`),
				)
				require.Contains(t, []int{http.StatusOK, http.StatusCreated}, status)
				put("rg1", "a")
				put("rg2", "b")
				assert.Equal(t, 2, count(list))
				assert.Equal(t, 1, count(listG))

				status, _ = doRequest(
					t,
					h,
					http.MethodDelete,
					"/subscriptions/s1/resourceGroups/rg1?api-version=2021-04-01",
					nil,
				)
				require.Contains(t, []int{http.StatusOK, http.StatusNoContent}, status)
				assert.Equal(t, 1, count(list))
			},
		},
		{
			name: "update_in_place",
			run: func(t *testing.T, _ *azurearm.Handler, put putFn, _ putFn, count countFn) {
				t.Helper()

				put("rg1", "a")
				put("rg1", "a")
				assert.Equal(t, 1, count(list))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()

			t.Parallel()

			h := newTestHandler(t)
			body := []byte(`{"location":"eastus"}`)

			put := func(rg, name string) {
				status, _ := doRequest(
					t,
					h,
					http.MethodPut,
					vnetPath(rg, name),
					body,
				)
				require.Equal(t, http.StatusOK, status)
			}
			del := func(rg, name string) {
				status, _ := doRequest(
					t,
					h,
					http.MethodDelete,
					vnetPath(rg, name),
					nil,
				)
				require.Equal(t, http.StatusOK, status)
			}
			count := func(path string) int {
				status, resp := doRequest(t, h, http.MethodGet, path, nil)
				require.Equal(t, http.StatusOK, status)

				vals, _ := resp["value"].([]any)

				return len(vals)
			}

			tt.run(t, h, put, del, count)
		})
	}
}
