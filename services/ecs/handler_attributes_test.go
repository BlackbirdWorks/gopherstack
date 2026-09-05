package ecs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecssdk "github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

// TestECS_ListAttributes_StableOrder guards against Go map iteration leaking
// into the wire response: gopherstack keys attributes in a
// map[string]*Attribute per cluster (services/ecs/store.go), and ranging that
// map directly -- as ListAttributes did -- produces an order that can differ
// between two calls with no mutation in between. AWS documents no order for
// ListAttributes (ecs@v1.90.0 api_op_ListAttributes.go), so any deterministic
// order is correct; this pins name-ascending.
func TestECS_ListAttributes_StableOrder(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)

	_, err := client.CreateCluster(t.Context(), &ecssdk.CreateClusterInput{
		ClusterName: aws.String("attr-order-cluster"),
	})
	require.NoError(t, err)

	_, err = client.PutAttributes(t.Context(), &ecssdk.PutAttributesInput{
		Cluster: aws.String("attr-order-cluster"),
		Attributes: []ecstypes.Attribute{
			{Name: aws.String("zeta"), TargetId: aws.String("i-1"), TargetType: ecstypes.TargetTypeContainerInstance},
			{Name: aws.String("alpha"), TargetId: aws.String("i-1"), TargetType: ecstypes.TargetTypeContainerInstance},
			{Name: aws.String("mid"), TargetId: aws.String("i-1"), TargetType: ecstypes.TargetTypeContainerInstance},
		},
	})
	require.NoError(t, err)

	want := []string{"alpha", "mid", "zeta"}

	for range 5 {
		out, listErr := client.ListAttributes(t.Context(), &ecssdk.ListAttributesInput{
			Cluster:    aws.String("attr-order-cluster"),
			TargetType: ecstypes.TargetTypeContainerInstance,
		})
		require.NoError(t, listErr)

		got := make([]string, len(out.Attributes))
		for i, a := range out.Attributes {
			got[i] = aws.ToString(a.Name)
		}

		assert.Equal(t, want, got)
	}
}

func TestAttributes_PutListDelete_Roundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "attr-cluster"})
	ciResp := doECSRequest(t, h, "RegisterContainerInstance", map[string]any{
		"cluster":                  "attr-cluster",
		"instanceIdentityDocument": fakeInstanceIdentityDocument("i-attr-1234"),
	})
	require.Equal(t, http.StatusOK, ciResp.Code)
	var ciOut map[string]any
	require.NoError(t, json.Unmarshal(ciResp.Body.Bytes(), &ciOut))
	ciArn := ciOut["containerInstance"].(map[string]any)["containerInstanceArn"].(string)

	// Put attributes
	putResp := doECSRequest(t, h, "PutAttributes", map[string]any{
		"cluster": "attr-cluster",
		"attributes": []any{
			map[string]any{
				"name":       "com.example.gpu",
				"value":      "nvidia",
				"targetType": "container-instance",
				"targetId":   ciArn,
			},
			map[string]any{
				"name":       "com.example.zone",
				"value":      "us-east-1a",
				"targetType": "container-instance",
				"targetId":   ciArn,
			},
		},
	})
	require.Equal(t, http.StatusOK, putResp.Code)

	// List attributes
	listResp := doECSRequest(t, h, "ListAttributes", map[string]any{
		"cluster":    "attr-cluster",
		"targetType": "container-instance",
	})
	require.Equal(t, http.StatusOK, listResp.Code)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	attrs := listOut["attributes"].([]any)
	assert.Len(t, attrs, 2)

	// Delete one attribute
	delResp := doECSRequest(t, h, "DeleteAttributes", map[string]any{
		"cluster": "attr-cluster",
		"attributes": []any{
			map[string]any{
				"name":       "com.example.gpu",
				"targetType": "container-instance",
				"targetId":   ciArn,
			},
		},
	})
	require.Equal(t, http.StatusOK, delResp.Code)

	// Verify one remains
	listResp2 := doECSRequest(t, h, "ListAttributes", map[string]any{
		"cluster":    "attr-cluster",
		"targetType": "container-instance",
	})
	require.Equal(t, http.StatusOK, listResp2.Code)
	var listOut2 map[string]any
	require.NoError(t, json.Unmarshal(listResp2.Body.Bytes(), &listOut2))
	attrs2 := listOut2["attributes"].([]any)
	assert.Len(t, attrs2, 1)
	assert.Equal(t, "com.example.zone", attrs2[0].(map[string]any)["name"])
}

func TestAttributes_FilterByName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "attr-filter-cluster"})
	ciResp := doECSRequest(t, h, "RegisterContainerInstance", map[string]any{
		"cluster":                  "attr-filter-cluster",
		"instanceIdentityDocument": fakeInstanceIdentityDocument("i-filter-5678"),
	})
	require.Equal(t, http.StatusOK, ciResp.Code)
	var ciOut map[string]any
	require.NoError(t, json.Unmarshal(ciResp.Body.Bytes(), &ciOut))
	ciArn := ciOut["containerInstance"].(map[string]any)["containerInstanceArn"].(string)

	doECSRequest(t, h, "PutAttributes", map[string]any{
		"cluster": "attr-filter-cluster",
		"attributes": []any{
			map[string]any{
				"name":       "ecs.gpu",
				"value":      "1",
				"targetType": "container-instance",
				"targetId":   ciArn,
			},
			map[string]any{
				"name":       "ecs.cpu",
				"value":      "16",
				"targetType": "container-instance",
				"targetId":   ciArn,
			},
		},
	})

	// Filter by attributeName
	listResp := doECSRequest(t, h, "ListAttributes", map[string]any{
		"cluster":       "attr-filter-cluster",
		"targetType":    "container-instance",
		"attributeName": "ecs.gpu",
	})
	require.Equal(t, http.StatusOK, listResp.Code)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	attrs := listOut["attributes"].([]any)
	assert.Len(t, attrs, 1)
	assert.Equal(t, "ecs.gpu", attrs[0].(map[string]any)["name"])
}

func TestListAttributes_TargetID_Filter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "attr-tid-cluster"})

	ci1Resp := doECSRequest(t, h, "RegisterContainerInstance", map[string]any{
		"cluster":                  "attr-tid-cluster",
		"instanceIdentityDocument": fakeInstanceIdentityDocument("i-instance-1"),
	})
	require.Equal(t, http.StatusOK, ci1Resp.Code)
	var ci1Out map[string]any
	require.NoError(t, json.Unmarshal(ci1Resp.Body.Bytes(), &ci1Out))
	ci1Arn := ci1Out["containerInstance"].(map[string]any)["containerInstanceArn"].(string)

	ci2Resp := doECSRequest(t, h, "RegisterContainerInstance", map[string]any{
		"cluster":                  "attr-tid-cluster",
		"instanceIdentityDocument": fakeInstanceIdentityDocument("i-instance-2"),
	})
	require.Equal(t, http.StatusOK, ci2Resp.Code)
	var ci2Out map[string]any
	require.NoError(t, json.Unmarshal(ci2Resp.Body.Bytes(), &ci2Out))
	ci2Arn := ci2Out["containerInstance"].(map[string]any)["containerInstanceArn"].(string)

	doECSRequest(t, h, "PutAttributes", map[string]any{
		"cluster": "attr-tid-cluster",
		"attributes": []any{
			map[string]any{
				"name":       "zone",
				"value":      "us-east-1a",
				"targetType": "container-instance",
				"targetId":   ci1Arn,
			},
			map[string]any{
				"name":       "zone",
				"value":      "us-east-1b",
				"targetType": "container-instance",
				"targetId":   ci2Arn,
			},
		},
	})

	// Filter by targetId — should return only ci1's attribute
	listResp := doECSRequest(t, h, "ListAttributes", map[string]any{
		"cluster":       "attr-tid-cluster",
		"targetType":    "container-instance",
		"attributeName": "zone",
		"targetId":      ci1Arn,
	})
	require.Equal(t, http.StatusOK, listResp.Code)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	attrs := listOut["attributes"].([]any)
	require.Len(t, attrs, 1)
	assert.Equal(t, "us-east-1a", attrs[0].(map[string]any)["value"])
}

func TestListAttributes_TargetID_Filter_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "attr-nf-cluster"})

	doECSRequest(t, h, "PutAttributes", map[string]any{
		"cluster": "attr-nf-cluster",
		"attributes": []any{
			map[string]any{
				"name":       "ecs.instance-type",
				"value":      "m5.large",
				"targetType": "container-instance",
				"targetId":   "ci-arn-1",
			},
		},
	})

	listResp := doECSRequest(t, h, "ListAttributes", map[string]any{
		"cluster":  "attr-nf-cluster",
		"targetId": "ci-arn-that-does-not-exist",
	})
	require.Equal(t, http.StatusOK, listResp.Code)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	attrs := listOut["attributes"].([]any)
	assert.Empty(t, attrs)
}

func TestListAttributes_TargetID_NoFilter_ReturnsAll(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "attr-all-cluster"})

	doECSRequest(t, h, "PutAttributes", map[string]any{
		"cluster": "attr-all-cluster",
		"attributes": []any{
			map[string]any{
				"name":       "ecs.availability-zone",
				"value":      "a",
				"targetType": "container-instance",
				"targetId":   "ci-1",
			},
			map[string]any{
				"name":       "ecs.availability-zone",
				"value":      "b",
				"targetType": "container-instance",
				"targetId":   "ci-2",
			},
			map[string]any{
				"name":       "ecs.availability-zone",
				"value":      "c",
				"targetType": "container-instance",
				"targetId":   "ci-3",
			},
		},
	})

	listResp := doECSRequest(t, h, "ListAttributes", map[string]any{
		"cluster":    "attr-all-cluster",
		"targetType": "container-instance",
	})
	require.Equal(t, http.StatusOK, listResp.Code)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	attrs := listOut["attributes"].([]any)
	assert.Len(t, attrs, 3)
}

func TestECS_DeleteAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input     map[string]any
		name      string
		seedAttrs []ecs.Attribute
		wantCount int
		wantCode  int
	}{
		{
			name: "delete existing attributes",
			seedAttrs: []ecs.Attribute{
				{Name: "my-attr", Value: "my-value", TargetID: "container-1"},
			},
			input: map[string]any{
				"attributes": []map[string]any{
					{"name": "my-attr", "targetId": "container-1"},
				},
			},
			wantCode:  http.StatusOK,
			wantCount: 1,
		},
		{
			name: "delete non-existent attribute returns empty list",
			input: map[string]any{
				"attributes": []map[string]any{
					{"name": "nonexistent", "targetId": "x"},
				},
			},
			wantCode:  http.StatusOK,
			wantCount: 0,
		},
		{
			name: "empty attributes list",
			input: map[string]any{
				"attributes": []map[string]any{},
			},
			wantCode:  http.StatusOK,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

			for i := range tt.seedAttrs {
				backend.AddAttributeInternal("default", &tt.seedAttrs[i])
			}

			h := ecs.NewHandler(backend)
			rec := doECSRequest(t, h, "DeleteAttributes", tt.input)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			attrs, ok := resp["attributes"].([]any)
			require.True(t, ok)
			assert.Len(t, attrs, tt.wantCount)
		})
	}
}

func TestECS_PutAndListAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cluster  string
		attrs    []map[string]any
		wantCode int
		wantLen  int
	}{
		{
			name:    "puts single attribute",
			cluster: "test-attrs-cluster",
			attrs: []map[string]any{
				{
					"name":       "ecs.capability.docker-remote-api.1.21",
					"targetId":   "container-instance/abc",
					"targetType": "container-instance",
				},
			},
			wantCode: http.StatusOK,
			wantLen:  1,
		},
		{
			name:    "puts multiple attributes",
			cluster: "test-attrs-cluster2",
			attrs: []map[string]any{
				{"name": "attr1", "targetId": "ci/1"},
				{"name": "attr2", "targetId": "ci/2"},
			},
			wantCode: http.StatusOK,
			wantLen:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": tt.cluster})

			// Put attributes
			rec := doECSRequest(t, h, "PutAttributes", map[string]any{
				"cluster":    tt.cluster,
				"attributes": tt.attrs,
			})
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var putResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))

			attrs, ok := putResp["attributes"].([]any)
			require.True(t, ok)
			assert.Len(t, attrs, tt.wantLen)

			// List attributes
			listRec := doECSRequest(t, h, "ListAttributes", map[string]any{"cluster": tt.cluster})
			require.Equal(t, http.StatusOK, listRec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))

			listedAttrs, ok := listResp["attributes"].([]any)
			require.True(t, ok)
			assert.Len(t, listedAttrs, tt.wantLen)
		})
	}
}
