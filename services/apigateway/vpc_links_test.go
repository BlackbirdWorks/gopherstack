package apigateway_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

func TestAPIGateway_VpcLink_RESTLifecycle(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)

	createRec := restCall(
		t,
		h,
		http.MethodPost,
		"/vpclinks",
		"application/json",
		`{"name":"my-link","targetArns":["arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/net/nlb/abc"]}`,
	)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	id, _ := created["id"].(string)
	require.NotEmpty(t, id)
	assert.Equal(t, "my-link", created["name"])

	getRec := restCall(t, h, http.MethodGet, "/vpclinks/"+id, "", "")
	require.Equal(t, http.StatusOK, getRec.Code)

	listRec := restCall(t, h, http.MethodGet, "/vpclinks", "", "")
	require.Equal(t, http.StatusOK, listRec.Code)

	var list map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &list))
	items, _ := list["item"].([]any)
	assert.Len(t, items, 1)

	patchRec := restCall(t, h, http.MethodPatch, "/vpclinks/"+id, "application/json",
		`[{"op":"replace","path":"/name","value":"renamed-link"}]`)
	require.Equal(t, http.StatusOK, patchRec.Code)

	var updated map[string]any
	require.NoError(t, json.Unmarshal(patchRec.Body.Bytes(), &updated))
	assert.Equal(t, "renamed-link", updated["name"])

	deleteRec := restCall(t, h, http.MethodDelete, "/vpclinks/"+id, "", "")
	require.Equal(t, http.StatusNoContent, deleteRec.Code)

	getAfterDeleteRec := restCall(t, h, http.MethodGet, "/vpclinks/"+id, "", "")
	assert.Equal(t, http.StatusNotFound, getAfterDeleteRec.Code)
}

// TestVpcLinks tests CreateVpcLink, GetVpcLink, GetVpcLinks, DeleteVpcLink, UpdateVpcLink.
func TestVpcLinks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		linkName        string
		updateName      string
		wantCreateCode  int
		wantGetCode     int
		wantDeleteCode  int
		wantGetAfterDel int
		doDelete        bool
		useInvalidID    bool
	}{
		{
			name:            "full_lifecycle",
			linkName:        "my-vpc-link",
			updateName:      "updated-link",
			wantCreateCode:  http.StatusCreated,
			wantGetCode:     http.StatusOK,
			doDelete:        true,
			wantDeleteCode:  http.StatusNoContent,
			wantGetAfterDel: http.StatusNotFound,
		},
		{
			name:           "get_not_found",
			linkName:       "other-link",
			wantCreateCode: http.StatusCreated,
			useInvalidID:   true,
			wantGetCode:    http.StatusNotFound,
		},
		{
			name:           "missing_name_fails",
			linkName:       "",
			wantCreateCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()

			var linkID string

			// Create
			body := fmt.Sprintf(
				`{"name":%q,"targetArns":["arn:aws:elasticloadbalancing:us-east-1:123:loadbalancer/net/my-nlb/abc"]}`,
				tt.linkName,
			)
			createRec := postWithHandler(t, handler, e, "CreateVpcLink", body)
			assert.Equal(t, tt.wantCreateCode, createRec.Code)

			if tt.wantCreateCode == http.StatusCreated {
				var createResp map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
				linkID = createResp["id"].(string)

				// Get
				lookupID := linkID
				if tt.useInvalidID {
					lookupID = "notexist"
				}
				getRec := postWithHandler(t, handler, e, "GetVpcLink",
					fmt.Sprintf(`{"vpcLinkId":%q}`, lookupID))
				assert.Equal(t, tt.wantGetCode, getRec.Code)

				// List
				listRec := postWithHandler(t, handler, e, "GetVpcLinks", `{}`)
				assert.Equal(t, http.StatusOK, listRec.Code)
				var listResp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
				assert.GreaterOrEqual(t, len(listResp["item"].([]any)), 1)

				// Update
				if tt.updateName != "" {
					updateRec := postWithHandler(t, handler, e, "UpdateVpcLink",
						fmt.Sprintf(`{"vpcLinkId":%q,"name":%q}`, linkID, tt.updateName))
					assert.Equal(t, http.StatusOK, updateRec.Code)
					var updateResp map[string]any
					require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updateResp))
					assert.Equal(t, tt.updateName, updateResp["name"])
				}

				// Delete
				if tt.doDelete {
					delRec := postWithHandler(t, handler, e, "DeleteVpcLink",
						fmt.Sprintf(`{"vpcLinkId":%q}`, linkID))
					assert.Equal(t, tt.wantDeleteCode, delRec.Code)

					getRec2 := postWithHandler(t, handler, e, "GetVpcLink",
						fmt.Sprintf(`{"vpcLinkId":%q}`, linkID))
					assert.Equal(t, tt.wantGetAfterDel, getRec2.Code)
				}
			}
		})
	}
}
