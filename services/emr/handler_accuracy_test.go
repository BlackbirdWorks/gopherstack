package emr_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/emr"
)

// --- #3: Release label validation ---

func TestAccuracy_RunJobFlow_InvalidReleaseLabel(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doEMRRequest(t, h, "RunJobFlow", map[string]any{
		"Name":         "bad-label-cluster",
		"ReleaseLabel": "emr-0.0.0-invalid",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccuracy_RunJobFlow_DefaultReleaseLabel(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "default-label-cluster"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	descRec := doEMRRequest(t, h, "DescribeCluster", map[string]any{"ClusterId": out.JobFlowID})
	require.Equal(t, http.StatusOK, descRec.Code)

	var desc struct {
		Cluster struct {
			ReleaseLabel string `json:"ReleaseLabel"`
		} `json:"Cluster"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &desc))
	assert.Equal(t, emr.DefaultReleaseLabel, desc.Cluster.ReleaseLabel)
}

// --- #4: Applications captured and returned ---

func TestAccuracy_RunJobFlow_ApplicationsDefaulted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doEMRRequest(t, h, "RunJobFlow", map[string]any{
		"Name":         "apps-cluster",
		"ReleaseLabel": "emr-7.3.0",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	descRec := doEMRRequest(t, h, "DescribeCluster", map[string]any{"ClusterId": out.JobFlowID})
	require.Equal(t, http.StatusOK, descRec.Code)

	var desc struct {
		Cluster struct {
			Applications []struct {
				Name string `json:"Name"`
			} `json:"Applications"`
		} `json:"Cluster"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &desc))
	assert.NotEmpty(t, desc.Cluster.Applications)
}

// --- #2 + #27: AddJobFlowSteps / ListSteps / DescribeStep ---

func TestAccuracy_Steps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "steps-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))
	clusterID := create.JobFlowID

	addRec := doEMRRequest(t, h, "AddJobFlowSteps", map[string]any{
		"JobFlowId": clusterID,
		"Steps": []any{
			map[string]any{
				"Name":            "step-one",
				"ActionOnFailure": "CONTINUE",
				"HadoopJarStep":   map[string]any{"Jar": "command-runner.jar"},
			},
			map[string]any{
				"Name":            "step-two",
				"ActionOnFailure": "TERMINATE_CLUSTER",
				"HadoopJarStep":   map[string]any{"Jar": "command-runner.jar"},
			},
		},
	})
	require.Equal(t, http.StatusOK, addRec.Code)

	var addOut struct {
		StepIDs []string `json:"StepIds"`
	}
	require.NoError(t, json.Unmarshal(addRec.Body.Bytes(), &addOut))
	require.Len(t, addOut.StepIDs, 2)
	assert.NotEmpty(t, addOut.StepIDs[0])

	listRec := doEMRRequest(t, h, "ListSteps", map[string]any{"ClusterId": clusterID})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut struct {
		Steps []struct {
			ID string `json:"Id"`
		} `json:"Steps"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	assert.Len(t, listOut.Steps, 2)

	descRec := doEMRRequest(t, h, "DescribeStep", map[string]any{
		"ClusterId": clusterID,
		"StepId":    addOut.StepIDs[0],
	})
	require.Equal(t, http.StatusOK, descRec.Code)
}

// --- #11: ManagedScalingPolicy persistence ---

func TestAccuracy_ManagedScalingPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "msp-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))
	clusterID := create.JobFlowID

	putRec := doEMRRequest(t, h, "PutManagedScalingPolicy", map[string]any{
		"ClusterId": clusterID,
		"ManagedScalingPolicy": map[string]any{
			"ComputeLimits": map[string]any{
				"UnitType":             "InstanceFleetUnits",
				"MinimumCapacityUnits": 1,
				"MaximumCapacityUnits": 10,
			},
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doEMRRequest(t, h, "GetManagedScalingPolicy", map[string]any{"ClusterId": clusterID})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut struct {
		ManagedScalingPolicy struct {
			ComputeLimits struct {
				UnitType             string `json:"UnitType"`
				MaximumCapacityUnits int    `json:"MaximumCapacityUnits"`
			} `json:"ComputeLimits"`
		} `json:"ManagedScalingPolicy"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Equal(t, "InstanceFleetUnits", getOut.ManagedScalingPolicy.ComputeLimits.UnitType)
	assert.Equal(t, 10, getOut.ManagedScalingPolicy.ComputeLimits.MaximumCapacityUnits)

	removeRec := doEMRRequest(t, h, "RemoveManagedScalingPolicy", map[string]any{"ClusterId": clusterID})
	assert.Equal(t, http.StatusOK, removeRec.Code)
}

// --- #13: AutoTerminationPolicy persistence ---

func TestAccuracy_AutoTerminationPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "atp-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))
	clusterID := create.JobFlowID

	putRec := doEMRRequest(t, h, "PutAutoTerminationPolicy", map[string]any{
		"ClusterId":             clusterID,
		"AutoTerminationPolicy": map[string]any{"IdleTimeout": 3600},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doEMRRequest(t, h, "GetAutoTerminationPolicy", map[string]any{"ClusterId": clusterID})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut struct {
		AutoTerminationPolicy struct {
			IdleTimeout int64 `json:"IdleTimeout"`
		} `json:"AutoTerminationPolicy"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Equal(t, int64(3600), getOut.AutoTerminationPolicy.IdleTimeout)
}

func TestAccuracy_AutoTerminationPolicy_InvalidTimeout(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "atp-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	putRec := doEMRRequest(t, h, "PutAutoTerminationPolicy", map[string]any{
		"ClusterId":             create.JobFlowID,
		"AutoTerminationPolicy": map[string]any{"IdleTimeout": 10},
	})
	assert.Equal(t, http.StatusBadRequest, putRec.Code)
}

// --- #14: SetTerminationProtection blocks TerminateJobFlows ---

func TestAccuracy_TerminationProtection(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "protected-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))
	clusterID := create.JobFlowID

	protectRec := doEMRRequest(t, h, "SetTerminationProtection", map[string]any{
		"JobFlowIds":           []string{clusterID},
		"TerminationProtected": true,
	})
	require.Equal(t, http.StatusOK, protectRec.Code)

	termRec := doEMRRequest(t, h, "TerminateJobFlows", map[string]any{
		"JobFlowIds": []string{clusterID},
	})
	assert.Equal(t, http.StatusBadRequest, termRec.Code)

	unprotectRec := doEMRRequest(t, h, "SetTerminationProtection", map[string]any{
		"JobFlowIds":           []string{clusterID},
		"TerminationProtected": false,
	})
	require.Equal(t, http.StatusOK, unprotectRec.Code)

	termRec2 := doEMRRequest(t, h, "TerminateJobFlows", map[string]any{
		"JobFlowIds": []string{clusterID},
	})
	assert.Equal(t, http.StatusOK, termRec2.Code)
}

// --- #15: ModifyCluster StepConcurrencyLevel ---

func TestAccuracy_ModifyCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "modify-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	modRec := doEMRRequest(t, h, "ModifyCluster", map[string]any{
		"ClusterId":            create.JobFlowID,
		"StepConcurrencyLevel": 5,
	})
	require.Equal(t, http.StatusOK, modRec.Code)

	var modOut struct {
		StepConcurrencyLevel int `json:"StepConcurrencyLevel"`
	}
	require.NoError(t, json.Unmarshal(modRec.Body.Bytes(), &modOut))
	assert.Equal(t, 5, modOut.StepConcurrencyLevel)
}

func TestAccuracy_ModifyCluster_InvalidRange(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "modify-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	modRec := doEMRRequest(t, h, "ModifyCluster", map[string]any{
		"ClusterId":            create.JobFlowID,
		"StepConcurrencyLevel": 999,
	})
	assert.Equal(t, http.StatusBadRequest, modRec.Code)
}

// --- #17: SecurityConfiguration JSON validation ---

func TestAccuracy_SecurityConfig_InvalidJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doEMRRequest(t, h, "CreateSecurityConfiguration", map[string]any{
		"Name":                  "bad-config",
		"SecurityConfiguration": "not valid json {",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccuracy_SecurityConfig_ValidJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doEMRRequest(t, h, "CreateSecurityConfiguration", map[string]any{
		"Name":                  "good-config",
		"SecurityConfiguration": `{"EncryptionConfiguration":{"EnableInTransitEncryption":false}}`,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- #18: ListSecurityConfigurations ---

func TestAccuracy_ListSecurityConfigurations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"alpha", "beta", "gamma"} {
		rec := doEMRRequest(t, h, "CreateSecurityConfiguration", map[string]any{
			"Name":                  name,
			"SecurityConfiguration": `{}`,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	listRec := doEMRRequest(t, h, "ListSecurityConfigurations", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var out struct {
		SecurityConfigurations []struct {
			Name string `json:"Name"`
		} `json:"SecurityConfigurations"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
	assert.Len(t, out.SecurityConfigurations, 3)
	assert.Equal(t, "alpha", out.SecurityConfigurations[0].Name)
}

// --- #19 + #20: Studio lifecycle + SessionMapping wiring ---

func TestAccuracy_Studio_DescribeAndList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "CreateStudio", map[string]any{
		"Name":                     "my-studio",
		"AuthMode":                 "SSO",
		"DefaultS3Location":        "s3://bucket",
		"EngineSecurityGroupId":    "sg-1",
		"ServiceRole":              "arn:role",
		"VpcId":                    "vpc-1",
		"WorkspaceSecurityGroupId": "sg-2",
		"SubnetIds":                []string{"subnet-1", "subnet-2"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		StudioID string `json:"StudioId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))
	studioID := create.StudioID

	descRec := doEMRRequest(t, h, "DescribeStudio", map[string]any{"StudioId": studioID})
	require.Equal(t, http.StatusOK, descRec.Code)

	var desc struct {
		Studio struct {
			Name string `json:"Name"`
		} `json:"Studio"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &desc))
	assert.Equal(t, "my-studio", desc.Studio.Name)

	listRec := doEMRRequest(t, h, "ListStudios", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut struct {
		Studios []struct {
			Name string `json:"Name"`
		} `json:"Studios"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	assert.Len(t, listOut.Studios, 1)
}

func TestAccuracy_StudioSessionMapping_GetUpdateList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "CreateStudio", map[string]any{
		"Name":                     "mapping-studio",
		"AuthMode":                 "SSO",
		"DefaultS3Location":        "s3://b",
		"EngineSecurityGroupId":    "sg-1",
		"ServiceRole":              "arn:r",
		"VpcId":                    "vpc-1",
		"WorkspaceSecurityGroupId": "sg-2",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		StudioID string `json:"StudioId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))
	studioID := create.StudioID

	doEMRRequest(t, h, "CreateStudioSessionMapping", map[string]any{
		"StudioId":         studioID,
		"IdentityType":     "USER",
		"IdentityId":       "user-123",
		"SessionPolicyArn": "arn:policy:old",
	})

	listRec := doEMRRequest(t, h, "ListStudioSessionMappings", map[string]any{"StudioId": studioID})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut struct {
		SessionMappings []struct {
			SessionPolicyArn string `json:"SessionPolicyArn"`
		} `json:"SessionMappings"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	require.Len(t, listOut.SessionMappings, 1)

	updateRec := doEMRRequest(t, h, "UpdateStudioSessionMapping", map[string]any{
		"StudioId":         studioID,
		"IdentityType":     "USER",
		"IdentityId":       "user-123",
		"SessionPolicyArn": "arn:policy:new",
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	getRec := doEMRRequest(t, h, "GetStudioSessionMapping", map[string]any{
		"StudioId":     studioID,
		"IdentityType": "USER",
		"IdentityId":   "user-123",
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut struct {
		SessionMapping struct {
			SessionPolicyArn string `json:"SessionPolicyArn"`
		} `json:"SessionMapping"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Equal(t, "arn:policy:new", getOut.SessionMapping.SessionPolicyArn)
}

// --- #22: BlockPublicAccessConfiguration ---

func TestAccuracy_BlockPublicAccessConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	getRec := doEMRRequest(t, h, "GetBlockPublicAccessConfiguration", map[string]any{})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut struct {
		BlockPublicAccessConfiguration struct {
			BlockPublicSecurityGroupRules bool `json:"BlockPublicSecurityGroupRules"`
		} `json:"BlockPublicAccessConfiguration"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.True(t, getOut.BlockPublicAccessConfiguration.BlockPublicSecurityGroupRules)

	putRec := doEMRRequest(t, h, "PutBlockPublicAccessConfiguration", map[string]any{
		"BlockPublicAccessConfiguration": map[string]any{
			"BlockPublicSecurityGroupRules":          false,
			"PermittedPublicSecurityGroupRuleRanges": []map[string]any{},
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec2 := doEMRRequest(t, h, "GetBlockPublicAccessConfiguration", map[string]any{})
	require.NoError(t, json.Unmarshal(getRec2.Body.Bytes(), &getOut))
	assert.False(t, getOut.BlockPublicAccessConfiguration.BlockPublicSecurityGroupRules)
}

// --- #23: ListReleaseLabels / DescribeReleaseLabel ---

func TestAccuracy_ListReleaseLabels(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	listRec := doEMRRequest(t, h, "ListReleaseLabels", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var out struct {
		ReleaseLabels []string `json:"ReleaseLabels"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
	assert.NotEmpty(t, out.ReleaseLabels)
	assert.Contains(t, out.ReleaseLabels, "emr-7.3.0")
}

func TestAccuracy_DescribeReleaseLabel(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doEMRRequest(t, h, "DescribeReleaseLabel", map[string]any{"ReleaseLabel": "emr-7.3.0"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		ReleaseLabel string `json:"ReleaseLabel"`
		Applications []struct {
			Name string `json:"Name"`
		} `json:"Applications"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "emr-7.3.0", out.ReleaseLabel)
	assert.NotEmpty(t, out.Applications)
}

// --- #24: ListSupportedInstanceTypes ---

func TestAccuracy_ListSupportedInstanceTypes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doEMRRequest(t, h, "ListSupportedInstanceTypes", map[string]any{"ReleaseLabel": "emr-7.3.0"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		SupportedInstanceTypes []struct {
			Type string `json:"Type"`
			VCPU int    `json:"VCPU"`
		} `json:"SupportedInstanceTypes"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out.SupportedInstanceTypes)
}

// --- #25: ListClusters filtering ---

func TestAccuracy_ListClusters_StateFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "waiting-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	doEMRRequest(t, h, "TerminateJobFlows", map[string]any{
		"JobFlowIds": []string{create.JobFlowID},
	})

	listActive := doEMRRequest(t, h, "ListClusters", map[string]any{})
	require.Equal(t, http.StatusOK, listActive.Code)

	var activeOut struct {
		Clusters []struct {
			ID string `json:"Id"`
		} `json:"Clusters"`
	}
	require.NoError(t, json.Unmarshal(listActive.Body.Bytes(), &activeOut))
	assert.Empty(t, activeOut.Clusters)

	listTerminated := doEMRRequest(t, h, "ListClusters", map[string]any{
		"ClusterStates": []string{"TERMINATED"},
	})
	require.Equal(t, http.StatusOK, listTerminated.Code)

	var termOut struct {
		Clusters []struct {
			ID string `json:"Id"`
		} `json:"Clusters"`
	}
	require.NoError(t, json.Unmarshal(listTerminated.Body.Bytes(), &termOut))
	assert.Len(t, termOut.Clusters, 1)
}

func TestAccuracy_ListClusters_DateFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "date-cluster"})

	past := float64(time.Now().Add(-time.Hour).UnixMilli())
	future := float64(time.Now().Add(time.Hour).UnixMilli())

	listInRange := doEMRRequest(t, h, "ListClusters", map[string]any{
		"CreatedAfter":  past,
		"CreatedBefore": future,
	})
	require.Equal(t, http.StatusOK, listInRange.Code)

	var inRange struct {
		Clusters []struct {
			ID string `json:"Id"`
		} `json:"Clusters"`
	}
	require.NoError(t, json.Unmarshal(listInRange.Body.Bytes(), &inRange))
	assert.Len(t, inRange.Clusters, 1)

	listOld := doEMRRequest(t, h, "ListClusters", map[string]any{
		"CreatedBefore": past,
	})
	require.Equal(t, http.StatusOK, listOld.Code)

	var old struct {
		Clusters []struct {
			ID string `json:"Id"`
		} `json:"Clusters"`
	}
	require.NoError(t, json.Unmarshal(listOld.Body.Bytes(), &old))
	assert.Empty(t, old.Clusters)
}

// --- #26: DescribeJobFlows ---

func TestAccuracy_DescribeJobFlows(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "jf-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	rec := doEMRRequest(t, h, "DescribeJobFlows", map[string]any{
		"JobFlowIds": []string{create.JobFlowID},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		JobFlows []struct {
			JobFlowID string `json:"JobFlowId"`
			Name      string `json:"Name"`
		} `json:"JobFlows"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.JobFlows, 1)
	assert.Equal(t, create.JobFlowID, out.JobFlows[0].JobFlowID)
	assert.Equal(t, "jf-cluster", out.JobFlows[0].Name)
}

// --- #29: GetClusterSessionCredentials ---

func TestAccuracy_GetClusterSessionCredentials(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "creds-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	rec := doEMRRequest(t, h, "GetClusterSessionCredentials", map[string]any{
		"ClusterId":        create.JobFlowID,
		"ExecutionRoleArn": "arn:aws:iam::000000000000:role/test-role",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Credentials map[string]any `json:"Credentials"`
		ExpiresAt   string         `json:"ExpiresAt"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out.Credentials)
	assert.NotEmpty(t, out.ExpiresAt)
}

func TestAccuracy_GetClusterSessionCredentials_MissingRole(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "creds-cluster2"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	rec := doEMRRequest(t, h, "GetClusterSessionCredentials", map[string]any{
		"ClusterId": create.JobFlowID,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- #30: Persistence includes instance groups ---

func TestAccuracy_Persistence_InstanceGroups(t *testing.T) {
	t.Parallel()

	src := emr.NewInMemoryBackend(testAccountID, testRegion)
	_, err := src.RunJobFlow(emr.RunJobFlowParams{
		Name:         "persist-ig-cluster",
		ReleaseLabel: "emr-7.3.0",
		Instances: emr.RunJobFlowInstances{
			InstanceGroups: []emr.InstanceGroupSpec{
				{Name: "master", InstanceRole: "MASTER", InstanceType: "m5.xlarge", InstanceCount: 1},
				{Name: "core", InstanceRole: "CORE", InstanceType: "m5.2xlarge", InstanceCount: 2},
			},
		},
	})
	require.NoError(t, err)

	snap := src.Snapshot()
	require.NotNil(t, snap)

	dst := emr.NewInMemoryBackend(testAccountID, testRegion)
	require.NoError(t, dst.Restore(snap))

	clusters, _ := dst.ListClusters(emr.ListClustersParams{})
	require.Len(t, clusters, 1)

	groups, err := dst.ListInstanceGroups(clusters[0].ID)
	require.NoError(t, err)
	assert.Len(t, groups, 2)
}

// --- #10: ListInstances synthetic ---

func TestAccuracy_ListInstances(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{
		"Name": "instances-cluster",
		"Instances": map[string]any{
			"InstanceGroups": []map[string]any{
				{"Name": "master", "InstanceRole": "MASTER", "InstanceType": "m5.xlarge", "InstanceCount": 1},
				{"Name": "core", "InstanceRole": "CORE", "InstanceType": "m5.2xlarge", "InstanceCount": 2},
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	listRec := doEMRRequest(t, h, "ListInstances", map[string]any{"ClusterId": create.JobFlowID})
	require.Equal(t, http.StatusOK, listRec.Code)

	var out struct {
		Instances []struct {
			ID            string `json:"Id"`
			Ec2InstanceID string `json:"Ec2InstanceId"`
		} `json:"Instances"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
	assert.Len(t, out.Instances, 3)
}

// --- #16: ModifyInstanceGroups ---

func TestAccuracy_ModifyInstanceGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{
		"Name": "modify-ig-cluster",
		"Instances": map[string]any{
			"InstanceGroups": []map[string]any{
				{"Name": "core", "InstanceRole": "CORE", "InstanceType": "m5.xlarge", "InstanceCount": 2},
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	listRec := doEMRRequest(t, h, "ListInstanceGroups", map[string]any{"ClusterId": create.JobFlowID})
	var listOut struct {
		InstanceGroups []struct {
			ID string `json:"Id"`
		} `json:"InstanceGroups"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	require.NotEmpty(t, listOut.InstanceGroups)

	modRec := doEMRRequest(t, h, "ModifyInstanceGroups", map[string]any{
		"ClusterId": create.JobFlowID,
		"InstanceGroups": []map[string]any{
			{"InstanceGroupId": listOut.InstanceGroups[0].ID, "InstanceCount": 5},
		},
	})
	assert.Equal(t, http.StatusOK, modRec.Code)
}

// --- #12: AutoScalingPolicy on instance group ---

func TestAccuracy_AutoScalingPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{
		"Name": "asg-cluster",
		"Instances": map[string]any{
			"InstanceGroups": []map[string]any{
				{"Name": "core", "InstanceRole": "CORE", "InstanceType": "m5.xlarge", "InstanceCount": 2},
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	listRec := doEMRRequest(t, h, "ListInstanceGroups", map[string]any{"ClusterId": create.JobFlowID})
	var listOut struct {
		InstanceGroups []struct {
			ID string `json:"Id"`
		} `json:"InstanceGroups"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	require.NotEmpty(t, listOut.InstanceGroups)
	groupID := listOut.InstanceGroups[0].ID

	putRec := doEMRRequest(t, h, "PutAutoScalingPolicy", map[string]any{
		"ClusterId":       create.JobFlowID,
		"InstanceGroupId": groupID,
		"AutoScalingPolicy": map[string]any{
			"Constraints": map[string]any{"MinCapacity": 1, "MaxCapacity": 10},
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	var putOut struct {
		AutoScalingPolicy *struct {
			Constraints struct {
				MaxCapacity int `json:"MaxCapacity"`
			} `json:"Constraints"`
		} `json:"AutoScalingPolicy"`
	}
	require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &putOut))
	require.NotNil(t, putOut.AutoScalingPolicy)
	assert.Equal(t, 10, putOut.AutoScalingPolicy.Constraints.MaxCapacity)

	removeRec := doEMRRequest(t, h, "RemoveAutoScalingPolicy", map[string]any{
		"ClusterId":       create.JobFlowID,
		"InstanceGroupId": groupID,
	})
	assert.Equal(t, http.StatusOK, removeRec.Code)
}

// --- #5: Configurations/Classification tree ---

func TestAccuracy_Configurations_RunJobFlow(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doEMRRequest(t, h, "RunJobFlow", map[string]any{
		"Name": "config-cluster",
		"Configurations": []map[string]any{
			{
				"Classification": "spark-defaults",
				"Properties":     map[string]string{"spark.executor.memory": "4g"},
				"Configurations": []map[string]any{
					{"Classification": "nested", "Properties": map[string]string{"k": "v"}},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	descRec := doEMRRequest(t, h, "DescribeCluster", map[string]any{"ClusterId": out.JobFlowID})
	require.Equal(t, http.StatusOK, descRec.Code)

	var desc struct {
		Cluster struct {
			Configurations []struct {
				Classification string            `json:"Classification"`
				Properties     map[string]string `json:"Properties"`
				Configurations []struct {
					Classification string `json:"Classification"`
				} `json:"Configurations"`
			} `json:"Configurations"`
		} `json:"Cluster"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &desc))
	require.Len(t, desc.Cluster.Configurations, 1)
	assert.Equal(t, "spark-defaults", desc.Cluster.Configurations[0].Classification)
	assert.Equal(t, "4g", desc.Cluster.Configurations[0].Properties["spark.executor.memory"])
	require.Len(t, desc.Cluster.Configurations[0].Configurations, 1)
	assert.Equal(t, "nested", desc.Cluster.Configurations[0].Configurations[0].Classification)
}

// --- #6: Remaining cluster fields (EbsRootVolumeSize, OSReleaseLabel, CustomAmiId) ---

func TestAccuracy_ClusterFields_EbsAndOS(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doEMRRequest(t, h, "RunJobFlow", map[string]any{
		"Name":              "fields-cluster",
		"EbsRootVolumeSize": 64,
		"OSReleaseLabel":    "2.0.20230718",
		"CustomAmiId":       "ami-0123456789abcdef0",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	descRec := doEMRRequest(t, h, "DescribeCluster", map[string]any{"ClusterId": out.JobFlowID})
	require.Equal(t, http.StatusOK, descRec.Code)

	var desc struct {
		Cluster struct {
			OSReleaseLabel    string `json:"OSReleaseLabel"`
			CustomAmiID       string `json:"CustomAmiId"`
			EbsRootVolumeSize int    `json:"EbsRootVolumeSize"`
		} `json:"Cluster"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &desc))
	assert.Equal(t, 64, desc.Cluster.EbsRootVolumeSize)
	assert.Equal(t, "2.0.20230718", desc.Cluster.OSReleaseLabel)
	assert.Equal(t, "ami-0123456789abcdef0", desc.Cluster.CustomAmiID)
}

// --- #8: InstanceFleet capacity targets ---

func TestAccuracy_InstanceFleet_CapacityTargets(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "fleet-cap-cluster"})
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
			"TargetOnDemandCapacity": 5,
			"TargetSpotCapacity":     10,
		},
	})
	require.Equal(t, http.StatusOK, addRec.Code)

	listRec := doEMRRequest(t, h, "ListInstanceFleets", map[string]any{"ClusterId": create.JobFlowID})
	require.Equal(t, http.StatusOK, listRec.Code)

	var out struct {
		InstanceFleets []struct {
			Status struct {
				State string `json:"State"`
			} `json:"Status"`
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
	assert.Equal(t, 5, f.TargetOnDemandCapacity)
	assert.Equal(t, 10, f.TargetSpotCapacity)
	assert.Equal(t, 5, f.ProvisionedOnDemandCapacity)
	assert.Equal(t, 10, f.ProvisionedSpotCapacity)
	assert.Equal(t, "RUNNING", f.Status.State)
	assert.NotEmpty(t, f.ID)
}

// --- #9: InstanceGroup BidPrice ---

func TestAccuracy_InstanceGroup_BidPrice(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{
		"Name": "bidprice-cluster",
		"Instances": map[string]any{
			"InstanceGroups": []map[string]any{
				{
					"Name":          "task",
					"InstanceRole":  "TASK",
					"InstanceType":  "m5.xlarge",
					"InstanceCount": 2,
					"Market":        "SPOT",
					"BidPrice":      "0.05",
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	listRec := doEMRRequest(t, h, "ListInstanceGroups", map[string]any{"ClusterId": create.JobFlowID})
	require.Equal(t, http.StatusOK, listRec.Code)

	var out struct {
		InstanceGroups []struct {
			Market   string `json:"Market"`
			BidPrice string `json:"BidPrice"`
		} `json:"InstanceGroups"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
	require.Len(t, out.InstanceGroups, 1)
	assert.Equal(t, "SPOT", out.InstanceGroups[0].Market)
	assert.Equal(t, "0.05", out.InstanceGroups[0].BidPrice)
}

// --- #21: NotebookExecution lifecycle ---

func TestAccuracy_NotebookExecution(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Start a notebook execution.
	startRec := doEMRRequest(t, h, "StartNotebookExecution", map[string]any{
		"EditorId":              "e-EXAMPLEEDITORID",
		"NotebookExecutionName": "test-run",
		"NotebookParams":        `{"key":"value"}`,
		"ExecutionEngineConfig": map[string]any{
			"Id": "j-EXAMPLECLUSTERID",
		},
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startOut struct {
		NotebookExecutionID string `json:"NotebookExecutionId"`
	}
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
	assert.NotEmpty(t, startOut.NotebookExecutionID)

	// Describe it.
	descRec := doEMRRequest(t, h, "DescribeNotebookExecution", map[string]any{
		"NotebookExecutionId": startOut.NotebookExecutionID,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut struct {
		NotebookExecution struct {
			NotebookExecutionID   string `json:"NotebookExecutionId"`
			Status                string `json:"Status"`
			NotebookExecutionName string `json:"NotebookExecutionName"`
		} `json:"NotebookExecution"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	assert.Equal(t, startOut.NotebookExecutionID, descOut.NotebookExecution.NotebookExecutionID)
	assert.Equal(t, "RUNNING", descOut.NotebookExecution.Status)
	assert.Equal(t, "test-run", descOut.NotebookExecution.NotebookExecutionName)

	// List it.
	listRec := doEMRRequest(t, h, "ListNotebookExecutions", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut struct {
		NotebookExecutions []struct {
			NotebookExecutionID string `json:"NotebookExecutionId"`
			Status              string `json:"Status"`
		} `json:"NotebookExecutions"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	require.Len(t, listOut.NotebookExecutions, 1)
	assert.Equal(t, startOut.NotebookExecutionID, listOut.NotebookExecutions[0].NotebookExecutionID)

	// Stop it.
	stopRec := doEMRRequest(t, h, "StopNotebookExecution", map[string]any{
		"NotebookExecutionId": startOut.NotebookExecutionID,
	})
	assert.Equal(t, http.StatusOK, stopRec.Code)

	// Describe again - should be STOPPED.
	descRec2 := doEMRRequest(t, h, "DescribeNotebookExecution", map[string]any{
		"NotebookExecutionId": startOut.NotebookExecutionID,
	})
	require.Equal(t, http.StatusOK, descRec2.Code)

	var descOut2 struct {
		NotebookExecution struct {
			Status string `json:"Status"`
		} `json:"NotebookExecution"`
	}
	require.NoError(t, json.Unmarshal(descRec2.Body.Bytes(), &descOut2))
	assert.Equal(t, "STOPPED", descOut2.NotebookExecution.Status)
}

func TestAccuracy_NotebookExecution_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doEMRRequest(t, h, "DescribeNotebookExecution", map[string]any{
		"NotebookExecutionId": "ex-doesnotexist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccuracy_NotebookExecution_ListFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Start two executions with different editor IDs.
	for range 3 {
		doEMRRequest(t, h, "StartNotebookExecution", map[string]any{
			"EditorId":              "e-EDITOR1",
			"NotebookExecutionName": "run1",
			"ExecutionEngineConfig": map[string]any{"Id": "j-1"},
		})
	}
	doEMRRequest(t, h, "StartNotebookExecution", map[string]any{
		"EditorId":              "e-EDITOR2",
		"NotebookExecutionName": "run2",
		"ExecutionEngineConfig": map[string]any{"Id": "j-2"},
	})

	// Filter by EditorId.
	rec := doEMRRequest(t, h, "ListNotebookExecutions", map[string]any{
		"EditorId": "e-EDITOR1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		NotebookExecutions []struct {
			NotebookExecutionID string `json:"NotebookExecutionId"`
		} `json:"NotebookExecutions"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Len(t, out.NotebookExecutions, 3)
}

// --- Persistence: notebookExecutions round-trip ---

func TestAccuracy_NotebookExecution_Persistence(t *testing.T) {
	t.Parallel()

	src := emr.NewInMemoryBackend(testAccountID, testRegion)
	ne, err := src.StartNotebookExecution("e-ED1", "persist-run", "{}", "j-1", nil)
	require.NoError(t, err)

	snap := src.Snapshot()
	require.NotNil(t, snap)

	dst := emr.NewInMemoryBackend("", "")
	require.NoError(t, dst.Restore(snap))

	restored, err := dst.DescribeNotebookExecution(ne.NotebookExecutionID)
	require.NoError(t, err)
	assert.Equal(t, "persist-run", restored.NotebookExecutionName)
	assert.Equal(t, "RUNNING", restored.Status)
}
