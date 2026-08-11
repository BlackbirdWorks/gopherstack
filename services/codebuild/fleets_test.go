package codebuild_test

import (
	"encoding/json"
	"maps"
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

// fleetExtendedFields decodes the subset of Fleet fields added by this pass's
// fix for CreateFleet/UpdateFleet silently dropping id/overflowBehavior/
// imageId/fleetServiceRole (see PARITY.md).
type fleetExtendedFields struct {
	Fleet struct {
		Arn              string `json:"arn"`
		ID               string `json:"id"`
		OverflowBehavior string `json:"overflowBehavior"`
		ImageID          string `json:"imageId"`
		FleetServiceRole string `json:"fleetServiceRole"`
	} `json:"fleet"`
}

// TestHandler_CreateFleet_ExtendedFields is a regression test for a
// previously-unflagged gap: CreateFleet accepted (or should have accepted)
// overflowBehavior/imageId/fleetServiceRole but silently dropped them, and
// Fleet had no "id" field at all despite the real Fleet shape having one
// (verified against aws-sdk-go-v2/service/codebuild@v1.72.4's
// awsAwsjson11_deserializeDocumentFleet, which has cases for "id",
// "overflowBehavior", "imageId", and "fleetServiceRole").
func TestHandler_CreateFleet_ExtendedFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateFleet", map[string]any{
		"name":             "extended-fleet",
		"baseCapacity":     1,
		"computeType":      "BUILD_GENERAL1_SMALL",
		"environmentType":  "LINUX_CONTAINER",
		"overflowBehavior": "ON_DEMAND",
		"imageId":          "aws/codebuild/standard:7.0",
		"fleetServiceRole": "arn:aws:iam::000000000000:role/fleet-role",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out fleetExtendedFields
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.NotEmpty(t, out.Fleet.ID)
	assert.Equal(t, "ON_DEMAND", out.Fleet.OverflowBehavior)
	assert.Equal(t, "aws/codebuild/standard:7.0", out.Fleet.ImageID)
	assert.Equal(t, "arn:aws:iam::000000000000:role/fleet-role", out.Fleet.FleetServiceRole)
}

// TestHandler_UpdateFleet_ExtendedFields verifies UpdateFleet actually
// applies overflowBehavior/imageId/fleetServiceRole/computeType/
// environmentType changes instead of silently ignoring everything but
// baseCapacity (the previous behavior).
func TestHandler_UpdateFleet_ExtendedFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, "CreateFleet", map[string]any{
		"name":            "upd-extended-fleet",
		"baseCapacity":    1,
		"computeType":     "BUILD_GENERAL1_SMALL",
		"environmentType": "LINUX_CONTAINER",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut fleetExtendedFields
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))

	updRec := doRequest(t, h, "UpdateFleet", map[string]any{
		"arn":              createOut.Fleet.Arn,
		"baseCapacity":     3,
		"overflowBehavior": "QUEUE",
		"imageId":          "aws/codebuild/standard:6.0",
		"fleetServiceRole": "arn:aws:iam::000000000000:role/updated-role",
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	var out fleetExtendedFields
	require.NoError(t, json.NewDecoder(updRec.Body).Decode(&out))
	assert.Equal(t, createOut.Fleet.ID, out.Fleet.ID, "id must not change across an update")
	assert.Equal(t, "QUEUE", out.Fleet.OverflowBehavior)
	assert.Equal(t, "aws/codebuild/standard:6.0", out.Fleet.ImageID)
	assert.Equal(t, "arn:aws:iam::000000000000:role/updated-role", out.Fleet.FleetServiceRole)
}

// fleetNestedConfig decodes the nested Fleet configuration objects
// (ComputeConfiguration/ProxyConfiguration/VpcConfig/ScalingConfiguration)
// added by this pass's field-diff against
// aws-sdk-go-v2/service/codebuild@v1.72.4's types.Fleet -- see PARITY.md gaps.
type fleetNestedConfig struct {
	Fleet struct {
		ComputeConfiguration *struct {
			MachineType  string `json:"machineType"`
			InstanceType string `json:"instanceType"`
			Disk         int64  `json:"disk"`
			Memory       int64  `json:"memory"`
			VCPU         int64  `json:"vCpu"`
		} `json:"computeConfiguration"`
		ProxyConfiguration *struct {
			DefaultBehavior   string `json:"defaultBehavior"`
			OrderedProxyRules []struct {
				Effect   string   `json:"effect"`
				Type     string   `json:"type"`
				Entities []string `json:"entities"`
			} `json:"orderedProxyRules"`
		} `json:"proxyConfiguration"`
		VpcConfig *struct {
			VpcID            string   `json:"vpcId"`
			SecurityGroupIDs []string `json:"securityGroupIds"`
			Subnets          []string `json:"subnets"`
		} `json:"vpcConfig"`
		ScalingConfiguration *struct {
			ScalingType                  string `json:"scalingType"`
			TargetTrackingScalingConfigs []struct {
				MetricType  string  `json:"metricType"`
				TargetValue float64 `json:"targetValue"`
			} `json:"targetTrackingScalingConfigs"`
			MaxCapacity     int32 `json:"maxCapacity"`
			DesiredCapacity int32 `json:"desiredCapacity"`
		} `json:"scalingConfiguration"`
		Arn string `json:"arn"`
	} `json:"fleet"`
}

// TestHandler_Fleet_NestedConfiguration is a table-driven regression test for
// this pass's field-diff of Fleet's nested config objects (ComputeConfiguration/
// ProxyConfiguration/VpcConfig/ScalingConfiguration) against real
// aws-sdk-go-v2/service/codebuild@v1.72.4 types.Fleet/CreateFleetInput/
// UpdateFleetInput -- previously entirely unmodeled (PARITY.md gap), now wired
// end to end through CreateFleet, UpdateFleet, and the response shape.
func TestHandler_Fleet_NestedConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createBody map[string]any
		updateBody map[string]any // nil means no UpdateFleet call
		check      func(t *testing.T, out fleetNestedConfig)
		name       string
	}{
		{
			name: "create_computeConfiguration",
			createBody: map[string]any{
				"name":         "cfg-compute-fleet",
				"baseCapacity": 1,
				"computeType":  "ATTRIBUTE_BASED_COMPUTE",
				"computeConfiguration": map[string]any{
					"machineType":  "GENERAL",
					"instanceType": "m5.large",
					"disk":         100,
					"memory":       8192,
					"vCpu":         4,
				},
			},
			check: func(t *testing.T, out fleetNestedConfig) {
				t.Helper()
				require.NotNil(t, out.Fleet.ComputeConfiguration)
				assert.Equal(t, "GENERAL", out.Fleet.ComputeConfiguration.MachineType)
				assert.Equal(t, "m5.large", out.Fleet.ComputeConfiguration.InstanceType)
				assert.Equal(t, int64(100), out.Fleet.ComputeConfiguration.Disk)
				assert.Equal(t, int64(8192), out.Fleet.ComputeConfiguration.Memory)
				assert.Equal(t, int64(4), out.Fleet.ComputeConfiguration.VCPU)
			},
		},
		{
			name: "create_proxyConfiguration",
			createBody: map[string]any{
				"name":         "cfg-proxy-fleet",
				"baseCapacity": 1,
				"proxyConfiguration": map[string]any{
					"defaultBehavior": "DENY_ALL",
					"orderedProxyRules": []map[string]any{
						{"effect": "ALLOW", "type": "DOMAIN", "entities": []string{"example.com"}},
					},
				},
			},
			check: func(t *testing.T, out fleetNestedConfig) {
				t.Helper()
				require.NotNil(t, out.Fleet.ProxyConfiguration)
				assert.Equal(t, "DENY_ALL", out.Fleet.ProxyConfiguration.DefaultBehavior)
				require.Len(t, out.Fleet.ProxyConfiguration.OrderedProxyRules, 1)
				assert.Equal(t, "ALLOW", out.Fleet.ProxyConfiguration.OrderedProxyRules[0].Effect)
				assert.Equal(t, "DOMAIN", out.Fleet.ProxyConfiguration.OrderedProxyRules[0].Type)
				assert.Equal(t, []string{"example.com"}, out.Fleet.ProxyConfiguration.OrderedProxyRules[0].Entities)
			},
		},
		{
			name: "create_vpcConfig",
			createBody: map[string]any{
				"name":         "cfg-vpc-fleet",
				"baseCapacity": 1,
				"vpcConfig": map[string]any{
					"vpcId":            "vpc-12345",
					"subnets":          []string{"subnet-1", "subnet-2"},
					"securityGroupIds": []string{"sg-1"},
				},
			},
			check: func(t *testing.T, out fleetNestedConfig) {
				t.Helper()
				require.NotNil(t, out.Fleet.VpcConfig)
				assert.Equal(t, "vpc-12345", out.Fleet.VpcConfig.VpcID)
				assert.Equal(t, []string{"subnet-1", "subnet-2"}, out.Fleet.VpcConfig.Subnets)
				assert.Equal(t, []string{"sg-1"}, out.Fleet.VpcConfig.SecurityGroupIDs)
			},
		},
		{
			name: "create_scalingConfiguration_desiredCapacityMatchesBase",
			createBody: map[string]any{
				"name":         "cfg-scaling-fleet",
				"baseCapacity": 3,
				"scalingConfiguration": map[string]any{
					"scalingType": "TARGET_TRACKING_SCALING",
					"maxCapacity": 10,
					"targetTrackingScalingConfigs": []map[string]any{
						{"metricType": "FLEET_UTILIZATION_RATE", "targetValue": 80},
					},
				},
			},
			check: func(t *testing.T, out fleetNestedConfig) {
				t.Helper()
				require.NotNil(t, out.Fleet.ScalingConfiguration)
				assert.Equal(t, "TARGET_TRACKING_SCALING", out.Fleet.ScalingConfiguration.ScalingType)
				assert.Equal(t, int32(10), out.Fleet.ScalingConfiguration.MaxCapacity)
				assert.Equal(t, int32(3), out.Fleet.ScalingConfiguration.DesiredCapacity,
					"desiredCapacity should reflect baseCapacity absent any live scaling event")
				require.Len(t, out.Fleet.ScalingConfiguration.TargetTrackingScalingConfigs, 1)
				assert.Equal(t, "FLEET_UTILIZATION_RATE",
					out.Fleet.ScalingConfiguration.TargetTrackingScalingConfigs[0].MetricType)
				assert.InDelta(t, 80.0, out.Fleet.ScalingConfiguration.TargetTrackingScalingConfigs[0].TargetValue, 0)
			},
		},
		{
			name: "update_overwrites_nested_configuration",
			createBody: map[string]any{
				"name":         "cfg-update-fleet",
				"baseCapacity": 1,
				"vpcConfig": map[string]any{
					"vpcId": "vpc-old",
				},
			},
			updateBody: map[string]any{
				"baseCapacity": 2,
				"vpcConfig": map[string]any{
					"vpcId":   "vpc-new",
					"subnets": []string{"subnet-new"},
				},
			},
			check: func(t *testing.T, out fleetNestedConfig) {
				t.Helper()
				require.NotNil(t, out.Fleet.VpcConfig)
				assert.Equal(t, "vpc-new", out.Fleet.VpcConfig.VpcID)
				assert.Equal(t, []string{"subnet-new"}, out.Fleet.VpcConfig.Subnets)
			},
		},
		{
			name: "update_without_nested_configuration_leaves_it_unchanged",
			createBody: map[string]any{
				"name":         "cfg-preserve-fleet",
				"baseCapacity": 1,
				"vpcConfig": map[string]any{
					"vpcId": "vpc-preserved",
				},
			},
			updateBody: map[string]any{
				"baseCapacity": 4,
			},
			check: func(t *testing.T, out fleetNestedConfig) {
				t.Helper()
				require.NotNil(t, out.Fleet.VpcConfig, "vpcConfig must survive an update that doesn't touch it")
				assert.Equal(t, "vpc-preserved", out.Fleet.VpcConfig.VpcID)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createRec := doRequest(t, h, "CreateFleet", tt.createBody)
			require.Equal(t, http.StatusOK, createRec.Code)

			var createOut fleetNestedConfig
			require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))

			out := createOut
			if tt.updateBody != nil {
				body := map[string]any{"arn": createOut.Fleet.Arn}
				maps.Copy(body, tt.updateBody)

				updRec := doRequest(t, h, "UpdateFleet", body)
				require.Equal(t, http.StatusOK, updRec.Code)

				var updOut fleetNestedConfig
				require.NoError(t, json.NewDecoder(updRec.Body).Decode(&updOut))
				out = updOut
			}

			tt.check(t, out)
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
		// DeleteFleet declares no ResourceNotFoundException in its real error set
		// (botocore codebuild/2016-10-06/service-2.json operations.DeleteFleet.errors:
		// only InvalidInputException), so it is idempotent.
		{
			name:       "delete_missing_fleet_is_idempotent",
			deleteArn:  "arn:aws:codebuild:us-east-1:000000000000:fleet/ghost-fleet",
			wantDelete: http.StatusOK,
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
