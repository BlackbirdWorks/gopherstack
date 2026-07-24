package emr_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWireShape_ListClusters_IncludesTimeline verifies ListClusters' per-cluster
// Status.Timeline.CreationDateTime is present on the wire (it was previously
// dropped entirely by gatherClusterSummaries, which also broke the
// most-recently-created-first sort since it read the same, always-empty
// field) and is an epoch-seconds JSON number, matching the real EMR
// awsjson1.1 wire format.
func TestWireShape_ListClusters_IncludesTimeline(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "timeline-wire-cluster"})
	require.Equal(t, http.StatusOK, rec.Code)

	listRec := doEMRRequest(t, h, "ListClusters", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var out struct {
		Clusters []struct {
			Status struct {
				Timeline struct {
					CreationDateTime float64 `json:"CreationDateTime"`
					ReadyDateTime    float64 `json:"ReadyDateTime"`
				} `json:"Timeline"`
			} `json:"Status"`
		} `json:"Clusters"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
	require.Len(t, out.Clusters, 1)
	assert.NotZero(t, out.Clusters[0].Status.Timeline.CreationDateTime)
	assert.NotZero(t, out.Clusters[0].Status.Timeline.ReadyDateTime)
}

// TestWireShape_ModifyInstanceFleet_AppliesCapacities verifies
// ModifyInstanceFleet actually mutates the fleet's target/provisioned
// capacities -- it previously looked up the fleet, matched its ID, and
// returned success without changing anything (a disguised no-op).
func TestWireShape_ModifyInstanceFleet_AppliesCapacities(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "modify-fleet-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	addRec := doEMRRequest(t, h, "AddInstanceFleet", map[string]any{
		"ClusterId": create.JobFlowID,
		"InstanceFleet": map[string]any{
			"Name":                   "task-fleet",
			"InstanceFleetType":      "TASK",
			"TargetOnDemandCapacity": 1,
			"TargetSpotCapacity":     1,
		},
	})
	require.Equal(t, http.StatusOK, addRec.Code)

	var added struct {
		InstanceFleetID string `json:"InstanceFleetId"`
	}
	require.NoError(t, json.Unmarshal(addRec.Body.Bytes(), &added))

	modRec := doEMRRequest(t, h, "ModifyInstanceFleet", map[string]any{
		"ClusterId": create.JobFlowID,
		"InstanceFleet": map[string]any{
			"InstanceFleetId":        added.InstanceFleetID,
			"TargetOnDemandCapacity": 8,
			"TargetSpotCapacity":     3,
		},
	})
	require.Equal(t, http.StatusOK, modRec.Code)

	listRec := doEMRRequest(t, h, "ListInstanceFleets", map[string]any{"ClusterId": create.JobFlowID})
	require.Equal(t, http.StatusOK, listRec.Code)

	var out struct {
		InstanceFleets []struct {
			ID                          string `json:"Id"`
			TargetOnDemandCapacity      int    `json:"TargetOnDemandCapacity"`
			TargetSpotCapacity          int    `json:"TargetSpotCapacity"`
			ProvisionedOnDemandCapacity int    `json:"ProvisionedOnDemandCapacity"`
			ProvisionedSpotCapacity     int    `json:"ProvisionedSpotCapacity"`
		} `json:"InstanceFleets"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
	require.Len(t, out.InstanceFleets, 1)
	f := out.InstanceFleets[0]
	assert.Equal(t, 8, f.TargetOnDemandCapacity)
	assert.Equal(t, 3, f.TargetSpotCapacity)
	assert.Equal(t, 8, f.ProvisionedOnDemandCapacity)
	assert.Equal(t, 3, f.ProvisionedSpotCapacity)
}

// TestWireShape_CancelSteps_RealEnumValues verifies CancelSteps reports the
// real CancelStepsRequestStatus enum values (SUBMITTED for a step that was
// still pending, FAILED for one that no longer is), not the fabricated
// "SUCCESS"/"QUEUED" strings this backend used to return -- a real client
// type-asserting on the enum would never recognize the old values.
func TestWireShape_CancelSteps_RealEnumValues(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{
		"Name": "cancel-enum-cluster",
		"Steps": []map[string]any{
			{
				"Name":            "step1",
				"ActionOnFailure": "CONTINUE",
				"HadoopJarStep":   map[string]any{"Jar": "command-runner.jar"},
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	listRec := doEMRRequest(t, h, "ListSteps", map[string]any{"ClusterId": create.JobFlowID})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut struct {
		Steps []struct {
			ID string `json:"Id"`
		} `json:"Steps"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	require.NotEmpty(t, listOut.Steps)

	stepID := listOut.Steps[0].ID

	// First cancellation: the step is still PENDING, so it is accepted.
	firstRec := doEMRRequest(t, h, "CancelSteps", map[string]any{
		"ClusterId": create.JobFlowID,
		"StepIds":   []string{stepID},
	})
	require.Equal(t, http.StatusOK, firstRec.Code)

	var firstOut struct {
		CancelStepsInfoList []struct {
			StepID string `json:"StepId"`
			Status string `json:"Status"`
		} `json:"CancelStepsInfoList"`
	}
	require.NoError(t, json.Unmarshal(firstRec.Body.Bytes(), &firstOut))
	require.Len(t, firstOut.CancelStepsInfoList, 1)
	assert.Equal(t, "SUBMITTED", firstOut.CancelStepsInfoList[0].Status)

	// Second cancellation of the now-CANCELLED step must fail.
	secondRec := doEMRRequest(t, h, "CancelSteps", map[string]any{
		"ClusterId": create.JobFlowID,
		"StepIds":   []string{stepID},
	})
	require.Equal(t, http.StatusOK, secondRec.Code)

	var secondOut struct {
		CancelStepsInfoList []struct {
			StepID string `json:"StepId"`
			Status string `json:"Status"`
			Reason string `json:"Reason"`
		} `json:"CancelStepsInfoList"`
	}
	require.NoError(t, json.Unmarshal(secondRec.Body.Bytes(), &secondOut))
	require.Len(t, secondOut.CancelStepsInfoList, 1)
	assert.Equal(t, "FAILED", secondOut.CancelStepsInfoList[0].Status)
	assert.NotEmpty(t, secondOut.CancelStepsInfoList[0].Reason)
}

// TestWireShape_NotebookExecution_EpochTimestamps verifies
// StartNotebookExecution/DescribeNotebookExecution emit StartTime as an
// epoch-seconds JSON number, matching the real EMR awsjson1.1 wire format --
// it previously marshalled the embedded time.Time with Go's default RFC3339
// string encoding, which a real SDK client's smithytime.ParseEpochSeconds
// deserializer rejects outright.
func TestWireShape_NotebookExecution_EpochTimestamps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	startRec := doEMRRequest(t, h, "StartNotebookExecution", map[string]any{
		"EditorId":              "e-123",
		"NotebookExecutionName": "nb-exec",
		"ExecutionEngineConfig": map[string]any{"Id": "j-CLUSTER"},
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	var started struct {
		NotebookExecutionID string `json:"NotebookExecutionId"`
	}
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &started))

	descRec := doEMRRequest(t, h, "DescribeNotebookExecution", map[string]any{
		"NotebookExecutionId": started.NotebookExecutionID,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var out struct {
		NotebookExecution struct {
			StartTime float64 `json:"StartTime"`
		} `json:"NotebookExecution"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &out))
	assert.NotZero(t, out.NotebookExecution.StartTime)
}

// TestWireShape_GetOnClusterAppUIPresignedURL_FieldName verifies the response
// carries the real "PresignedURL" wire field (plus "PresignedURLReady") --
// this backend previously sent "URL", a field name
// GetOnClusterAppUIPresignedURLOutput does not define, so a real client's
// output.PresignedURL would always deserialize as nil.
func TestWireShape_GetOnClusterAppUIPresignedURL_FieldName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "presigned-url-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	rec := doEMRRequest(t, h, "GetOnClusterAppUIPresignedURL", map[string]any{"ClusterId": create.JobFlowID})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		PresignedURL      string `json:"PresignedURL"`
		PresignedURLReady bool   `json:"PresignedURLReady"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out.PresignedURL)
	assert.True(t, out.PresignedURLReady)

	// The old, wrong field name must not be the only thing populated.
	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
	assert.NotContains(t, raw, "URL")
}

// TestWireShape_GetPersistentAppUIPresignedURL_ReadyFlag verifies
// PresignedURLReady is present and true (gopherstack provisions synchronously).
func TestWireShape_GetPersistentAppUIPresignedURL_ReadyFlag(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doEMRRequest(t, h, "GetPersistentAppUIPresignedURL", map[string]any{
		"PersistentAppUIId": "pau-123",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		PresignedURL      string `json:"PresignedURL"`
		PresignedURLReady bool   `json:"PresignedURLReady"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out.PresignedURL)
	assert.True(t, out.PresignedURLReady)
}

// TestWireShape_RunJobFlow_JobFlowRole verifies the top-level JobFlowRole
// input field (real RunJobFlowInput.JobFlowRole) is echoed back as
// Cluster.Ec2InstanceAttributes.IamInstanceProfile -- there is no
// IamInstanceProfile member on the Instances block itself in the real API.
func TestWireShape_RunJobFlow_JobFlowRole(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doEMRRequest(t, h, "RunJobFlow", map[string]any{
		"Name":        "job-flow-role-cluster",
		"JobFlowRole": "EMR_EC2_DefaultRole",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &create))

	descRec := doEMRRequest(t, h, "DescribeCluster", map[string]any{"ClusterId": create.JobFlowID})
	require.Equal(t, http.StatusOK, descRec.Code)

	var out struct {
		Cluster struct {
			Ec2InstanceAttributes struct {
				IamInstanceProfile string `json:"IamInstanceProfile"`
			} `json:"Ec2InstanceAttributes"`
		} `json:"Cluster"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &out))
	assert.Equal(t, "EMR_EC2_DefaultRole", out.Cluster.Ec2InstanceAttributes.IamInstanceProfile)
}

// TestWireShape_RunJobFlow_InlinePolicies verifies RunJobFlow accepts
// ManagedScalingPolicy and AutoTerminationPolicy inline (real
// RunJobFlowInput supports both), not only via the separate
// Put{ManagedScaling,AutoTermination}Policy operations.
func TestWireShape_RunJobFlow_InlinePolicies(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doEMRRequest(t, h, "RunJobFlow", map[string]any{
		"Name": "inline-policy-cluster",
		"ManagedScalingPolicy": map[string]any{
			"ComputeLimits": map[string]any{
				"UnitType":             "InstanceFleetUnits",
				"MinimumCapacityUnits": 1,
				"MaximumCapacityUnits": 10,
			},
		},
		"AutoTerminationPolicy": map[string]any{"IdleTimeout": 3600},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &create))

	msRec := doEMRRequest(t, h, "GetManagedScalingPolicy", map[string]any{"ClusterId": create.JobFlowID})
	require.Equal(t, http.StatusOK, msRec.Code)

	var msOut struct {
		ManagedScalingPolicy struct {
			ComputeLimits struct {
				MaximumCapacityUnits int `json:"MaximumCapacityUnits"`
			} `json:"ComputeLimits"`
		} `json:"ManagedScalingPolicy"`
	}
	require.NoError(t, json.Unmarshal(msRec.Body.Bytes(), &msOut))
	assert.Equal(t, 10, msOut.ManagedScalingPolicy.ComputeLimits.MaximumCapacityUnits)

	atRec := doEMRRequest(t, h, "GetAutoTerminationPolicy", map[string]any{"ClusterId": create.JobFlowID})
	require.Equal(t, http.StatusOK, atRec.Code)

	var atOut struct {
		AutoTerminationPolicy struct {
			IdleTimeout int64 `json:"IdleTimeout"`
		} `json:"AutoTerminationPolicy"`
	}
	require.NoError(t, json.Unmarshal(atRec.Body.Bytes(), &atOut))
	assert.Equal(t, int64(3600), atOut.AutoTerminationPolicy.IdleTimeout)
}

// TestWireShape_GetManagedScalingPolicy_OmitsFieldWhenUnset verifies the
// ManagedScalingPolicy field is entirely absent from the response when no
// policy is attached -- real GetManagedScalingPolicyOutput.ManagedScalingPolicy
// is a pointer that AWS omits from the wire (rather than a zero-valued
// object), so a real client's output.ManagedScalingPolicy must deserialize
// as nil, not as a non-nil struct with zeroed fields.
func TestWireShape_GetManagedScalingPolicy_OmitsFieldWhenUnset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "no-managed-scaling-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	rec := doEMRRequest(t, h, "GetManagedScalingPolicy", map[string]any{"ClusterId": create.JobFlowID})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
	assert.NotContains(t, raw, "ManagedScalingPolicy")
}

// TestWireShape_GetAutoTerminationPolicy_OmitsFieldWhenUnset is
// TestWireShape_GetManagedScalingPolicy_OmitsFieldWhenUnset's counterpart for
// GetAutoTerminationPolicy.
func TestWireShape_GetAutoTerminationPolicy_OmitsFieldWhenUnset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "no-auto-termination-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	rec := doEMRRequest(t, h, "GetAutoTerminationPolicy", map[string]any{"ClusterId": create.JobFlowID})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
	assert.NotContains(t, raw, "AutoTerminationPolicy")
}

// TestWireShape_AddTags_StudioResource verifies AddTags/ListTagsForResource
// work against a Studio ID, not only a cluster ID/ARN -- real
// AddTagsInput.ResourceId accepts "a cluster identifier or an Amazon EMR
// Studio ID" per the SDK doc, but this backend previously only ever looked
// up clusters, so tagging a studio incorrectly 400'd as resource-not-found.
func TestWireShape_AddTags_StudioResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doEMRRequest(t, h, "CreateStudio", map[string]any{
		"Name":     "tag-studio",
		"AuthMode": "SSO",
		"VpcId":    "vpc-123",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created struct {
		StudioID string `json:"StudioId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

	addRec := doEMRRequest(t, h, "AddTags", map[string]any{
		"ResourceId": created.StudioID,
		"Tags":       []map[string]any{{"Key": "env", "Value": "prod"}},
	})
	require.Equal(t, http.StatusOK, addRec.Code, addRec.Body.String())

	listRec := doEMRRequest(t, h, "ListTagsForResource", map[string]any{"ResourceId": created.StudioID})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	require.Len(t, listOut.Tags, 1)
	assert.Equal(t, "env", listOut.Tags[0].Key)
	assert.Equal(t, "prod", listOut.Tags[0].Value)

	removeRec := doEMRRequest(t, h, "RemoveTags", map[string]any{
		"ResourceId": created.StudioID,
		"TagKeys":    []string{"env"},
	})
	require.Equal(t, http.StatusOK, removeRec.Code)

	listRec2 := doEMRRequest(t, h, "ListTagsForResource", map[string]any{"ResourceId": created.StudioID})
	require.Equal(t, http.StatusOK, listRec2.Code)

	var listOut2 struct {
		Tags []struct{} `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(listRec2.Body.Bytes(), &listOut2))
	assert.Empty(t, listOut2.Tags)
}

// TestWireShape_RunJobFlow_InstanceFleets verifies RunJobFlow supports
// creating a cluster with Instances.InstanceFleets (an alternative to
// InstanceGroups the real JobFlowInstancesConfig supports) -- this backend
// previously only ever accepted InstanceGroups at creation time; fleets
// could only be attached after the fact via AddInstanceFleet.
func TestWireShape_RunJobFlow_InstanceFleets(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doEMRRequest(t, h, "RunJobFlow", map[string]any{
		"Name": "fleet-cluster",
		"Instances": map[string]any{
			"InstanceFleets": []map[string]any{
				{
					"Name":                   "primary",
					"InstanceFleetType":      "MASTER",
					"TargetOnDemandCapacity": 1,
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &create))

	descRec := doEMRRequest(t, h, "DescribeCluster", map[string]any{"ClusterId": create.JobFlowID})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut struct {
		Cluster struct {
			InstanceCollectionType string `json:"InstanceCollectionType"`
		} `json:"Cluster"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	assert.Equal(t, "INSTANCE_FLEET", descOut.Cluster.InstanceCollectionType)

	listRec := doEMRRequest(t, h, "ListInstanceFleets", map[string]any{"ClusterId": create.JobFlowID})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut struct {
		InstanceFleets []struct {
			Name                   string `json:"Name"`
			TargetOnDemandCapacity int    `json:"TargetOnDemandCapacity"`
		} `json:"InstanceFleets"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	require.Len(t, listOut.InstanceFleets, 1)
	assert.Equal(t, "primary", listOut.InstanceFleets[0].Name)
	assert.Equal(t, 1, listOut.InstanceFleets[0].TargetOnDemandCapacity)
}

// TestWireShape_RunJobFlow_KerberosAttributesAndPlacementGroups verifies
// both fields round-trip from RunJobFlow input to the DescribeCluster
// response -- real Cluster carries KerberosAttributes and PlacementGroups,
// but this backend previously dropped both entirely.
func TestWireShape_RunJobFlow_KerberosAttributesAndPlacementGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doEMRRequest(t, h, "RunJobFlow", map[string]any{
		"Name": "kerberos-placement-cluster",
		"KerberosAttributes": map[string]any{
			"Realm":            "EC2.INTERNAL",
			"KdcAdminPassword": "s3cret",
		},
		"PlacementGroupConfigs": []map[string]any{
			{"InstanceRole": "MASTER", "PlacementStrategy": "SPREAD"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &create))

	descRec := doEMRRequest(t, h, "DescribeCluster", map[string]any{"ClusterId": create.JobFlowID})
	require.Equal(t, http.StatusOK, descRec.Code)

	var out struct {
		Cluster struct {
			KerberosAttributes struct {
				Realm string `json:"Realm"`
			} `json:"KerberosAttributes"`
			PlacementGroups []struct {
				InstanceRole      string `json:"InstanceRole"`
				PlacementStrategy string `json:"PlacementStrategy"`
			} `json:"PlacementGroups"`
		} `json:"Cluster"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &out))
	assert.Equal(t, "EC2.INTERNAL", out.Cluster.KerberosAttributes.Realm)
	require.Len(t, out.Cluster.PlacementGroups, 1)
	assert.Equal(t, "MASTER", out.Cluster.PlacementGroups[0].InstanceRole)
	assert.Equal(t, "SPREAD", out.Cluster.PlacementGroups[0].PlacementStrategy)
}

// TestWireShape_RunJobFlow_AutoTerminate verifies Cluster.AutoTerminate is
// the real API's inverse of Instances.KeepJobFlowAliveWhenNoSteps.
func TestWireShape_RunJobFlow_AutoTerminate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doEMRRequest(t, h, "RunJobFlow", map[string]any{
		"Name":      "keep-alive-cluster",
		"Instances": map[string]any{"KeepJobFlowAliveWhenNoSteps": true},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &create))

	descRec := doEMRRequest(t, h, "DescribeCluster", map[string]any{"ClusterId": create.JobFlowID})
	require.Equal(t, http.StatusOK, descRec.Code)

	var out struct {
		Cluster struct {
			AutoTerminate               bool `json:"AutoTerminate"`
			KeepJobFlowAliveWhenNoSteps bool `json:"KeepJobFlowAliveWhenNoSteps"`
		} `json:"Cluster"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &out))
	assert.True(t, out.Cluster.KeepJobFlowAliveWhenNoSteps)
	assert.False(t, out.Cluster.AutoTerminate)
}
