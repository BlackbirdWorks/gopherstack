package ssoadmin_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssoadmin"
)

func TestUpdateInstance(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceARN := createInstance(t, h, "test-instance")

	// UpdateInstance
	rec := doRequest(t, h, "UpdateInstance", map[string]any{
		"InstanceArn": instanceARN,
		"Name":        "updated-instance",
	})
	require.Equal(t, http.StatusOK, rec.Code, "UpdateInstance: %s", rec.Body.String())

	descRec := doRequest(t, h, "DescribeInstance", map[string]any{"InstanceArn": instanceARN})
	require.Equal(t, http.StatusOK, descRec.Code)
	descResp := parseResponse(t, descRec)
	assert.Equal(t, "updated-instance", descResp["Name"], "UpdateInstance must persist the new Name")
}

// TestInstanceLazyActivation verifies instances transition CREATE_IN_PROGRESS → ACTIVE lazily.
func TestInstanceLazyActivation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus string
		describe   bool
	}{
		{
			name:       "describe triggers activation",
			describe:   true,
			wantStatus: "ACTIVE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequest(t, h, "CreateInstance", map[string]any{"Name": "lazy-inst"})
			require.Equal(t, http.StatusOK, rec.Code)
			resp := parseResponse(t, rec)
			arn := resp["InstanceArn"].(string)

			rec2 := doRequest(t, h, "DescribeInstance", map[string]any{"InstanceArn": arn})
			require.Equal(t, http.StatusOK, rec2.Code)
			resp2 := parseResponse(t, rec2)
			assert.Equal(t, tt.wantStatus, resp2["Status"])
		})
	}
}

// TestInstanceCount verifies that InstanceCount export helper works.
func TestInstanceCount(t *testing.T) {
	t.Parallel()

	b := ssoadmin.NewInMemoryBackend("000000000000", "us-east-1")
	// Default pre-seeded instance exists.
	assert.Equal(t, 1, ssoadmin.InstanceCount(b))

	b.AddInstanceInternal("extra")
	assert.Equal(t, 2, ssoadmin.InstanceCount(b))
}

// TestDeleteInstanceCascades verifies that deleting an instance also removes
// dependent permission sets, applications, and trusted token issuers.
func TestDeleteInstanceCascades(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		createPS     bool
		createApp    bool
		createTTI    bool
		wantPSCount  int
		wantAppCount int
		wantTTICount int
	}{
		{
			name:         "cascade deletes permission sets and apps",
			createPS:     true,
			createApp:    true,
			createTTI:    true,
			wantPSCount:  0,
			wantAppCount: 0,
			wantTTICount: 0,
		},
		{
			name:         "cascade on empty instance",
			createPS:     false,
			createApp:    false,
			createTTI:    false,
			wantPSCount:  0,
			wantAppCount: 0,
			wantTTICount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssoadmin.NewInMemoryBackend("000000000000", "us-east-1")
			h := ssoadmin.NewHandler(b)

			inst := b.AddInstanceInternal("cascade-inst")
			instanceArn := inst.InstanceArn

			if tt.createPS {
				b.AddPermissionSetInternal(instanceArn, "cascade-ps")
			}
			if tt.createApp {
				b.AddApplicationInternal(instanceArn, "cascade-app")
			}
			if tt.createTTI {
				rec := doRequest(t, h, "CreateTrustedTokenIssuer", map[string]any{
					"InstanceArn":            instanceArn,
					"Name":                   "cascade-tti",
					"TrustedTokenIssuerType": "OIDC_JWT",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "DeleteInstance", map[string]any{"InstanceArn": instanceArn})
			require.Equal(t, http.StatusOK, rec.Code)

			assert.Equal(t, tt.wantPSCount, ssoadmin.PermissionSetCount(b))
			assert.Equal(t, tt.wantAppCount, ssoadmin.ApplicationCount(b))
			assert.Equal(t, tt.wantTTICount, ssoadmin.TrustedTokenIssuerCount(b))
		})
	}
}

// TestAddInstanceInternal verifies the seed helper.
func TestAddInstanceInternal(t *testing.T) {
	t.Parallel()

	b := ssoadmin.NewInMemoryBackend("000000000000", "us-east-1")
	initial := ssoadmin.InstanceCount(b)
	inst := b.AddInstanceInternal("seeded")
	require.NotNil(t, inst)
	assert.NotEmpty(t, inst.InstanceArn)
	assert.Equal(t, "seeded", inst.Name)
	assert.Equal(t, initial+1, ssoadmin.InstanceCount(b))
}

// TestDescribeInstanceCreatedDate verifies that DescribeInstance returns CreatedDate.
func TestDescribeInstanceCreatedDate(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "date-inst")

	rec := doRequest(t, h, "DescribeInstance", map[string]any{"InstanceArn": instanceArn})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	assert.NotNil(t, resp["CreatedDate"], "CreatedDate must be present in DescribeInstance response")
	cd, ok := resp["CreatedDate"].(float64)
	assert.True(t, ok, "CreatedDate should be a numeric timestamp")
	assert.Greater(t, cd, float64(0), "CreatedDate should be > 0")
}

// TestListInstancesIncludesCreatedDate verifies CreatedDate in ListInstances response.
func TestListInstancesIncludesCreatedDate(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doRequest(t, h, "ListInstances", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResponse(t, rec)
	instances, ok := resp["Instances"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, instances)

	inst := instances[0].(map[string]any)
	createdDate, hasDate := inst["CreatedDate"]
	assert.True(t, hasDate, "ListInstances should include CreatedDate")
	assert.NotNil(t, createdDate)
	assert.Greater(t, createdDate.(float64), float64(0), "CreatedDate should be a positive unix timestamp")
}

// TestDescribeInstanceWireShape locks in the real DescribeInstanceOutput wire
// shape: it has no Tags member (gopherstack previously invented one here).
// Tags for an instance are fetched separately via ListTagsForResource,
// matching every other taggable ssoadmin resource.
func TestDescribeInstanceWireShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "r3-tags-inst")

	// Tag the instance.
	tagRec := doRequest(t, h, "TagResource", map[string]any{
		"InstanceArn": instanceArn,
		"ResourceArn": instanceArn,
		"Tags":        []map[string]any{{"Key": "env", "Value": "test"}},
	})
	require.Equal(t, http.StatusOK, tagRec.Code)

	rec := doRequest(t, h, "DescribeInstance", map[string]any{"InstanceArn": instanceArn})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResponse(t, rec)
	assert.NotContains(t, resp, "Tags", "DescribeInstanceOutput has no Tags member")
	assert.Equal(t, instanceArn, resp["InstanceArn"])

	// Tags are only reachable via ListTagsForResource, matching real AWS.
	tagsRec := doRequest(t, h, "ListTagsForResource", map[string]any{
		"InstanceArn": instanceArn,
		"ResourceArn": instanceArn,
	})
	require.Equal(t, http.StatusOK, tagsRec.Code)
	tagsResp := parseResponse(t, tagsRec)
	tagSlice, ok := tagsResp["Tags"].([]any)
	assert.True(t, ok)
	assert.Len(t, tagSlice, 1)
	tagEntry := tagSlice[0].(map[string]any)
	assert.Equal(t, "env", tagEntry["Key"])
	assert.Equal(t, "test", tagEntry["Value"])
}

// TestDescribeInstance_EncryptionConfigurationDetails verifies the real
// DescribeInstanceOutput.EncryptionConfigurationDetails member is populated
// with the one true default state for an instance whose encryption
// configuration was never (and, via this SDK version's op set, can never be)
// switched to a customer-managed KMS key.
func TestDescribeInstance_EncryptionConfigurationDetails(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "encryption-details-inst")

	rec := doRequest(t, h, "DescribeInstance", map[string]any{"InstanceArn": instanceArn})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResponse(t, rec)
	details, ok := resp["EncryptionConfigurationDetails"].(map[string]any)
	require.True(t, ok, "EncryptionConfigurationDetails must be present")
	assert.Equal(t, "ENABLED", details["EncryptionStatus"])
	assert.Equal(t, "AWS_OWNED_KMS_KEY", details["KeyType"])
}

// TestListInstancesSortedByBackend verifies that ListInstances (via handler)
// returns instances sorted by ARN, relying on backend sort rather than handler
// sort (the previous implementation sorted in the handler layer on each call).
func TestListInstancesSortedByBackend(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	for _, name := range []string{"zzz-last", "aaa-first", "mmm-middle"} {
		createInstance(t, h, name)
	}

	rec := doRequest(t, h, "ListInstances", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResponse(t, rec)
	insts, ok := resp["Instances"].([]any)
	require.True(t, ok)
	require.GreaterOrEqual(t, len(insts), 3)

	arns := make([]string, 0, len(insts))
	for _, inst := range insts {
		m, mok := inst.(map[string]any)
		require.True(t, mok)
		arns = append(arns, m["InstanceArn"].(string))
	}

	for i := 1; i < len(arns); i++ {
		assert.LessOrEqual(t, arns[i-1], arns[i],
			"ListInstances result not sorted at index %d: %s > %s", i, arns[i-1], arns[i])
	}
}

func TestInstanceCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createName string
		wantName   string
		wantStatus int
	}{
		{
			name:       "create and describe instance",
			createName: "test-instance",
			wantStatus: http.StatusOK,
			wantName:   "test-instance",
		},
		{
			name:       "create instance empty name",
			createName: "",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			rec := doRequest(t, h, "CreateInstance", map[string]any{"Name": tt.createName})
			require.Equal(t, tt.wantStatus, rec.Code)

			resp := parseResponse(t, rec)
			instanceArn, ok := resp["InstanceArn"].(string)
			require.True(t, ok)
			require.NotEmpty(t, instanceArn)

			if tt.wantName != "" {
				descRec := doRequest(t, h, "DescribeInstance", map[string]any{"InstanceArn": instanceArn})
				require.Equal(t, http.StatusOK, descRec.Code)
				descResp := parseResponse(t, descRec)
				assert.Equal(t, tt.wantName, descResp["Name"])
				assert.Equal(t, instanceArn, descResp["InstanceArn"])
				assert.Equal(t, "ACTIVE", descResp["Status"])
			}
		})
	}
}

func TestListInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		createNames  []string
		wantMinCount int
	}{
		{
			name:         "empty list",
			createNames:  nil,
			wantMinCount: 0,
		},
		{
			name:         "lists all instances",
			createNames:  []string{"inst-a", "inst-b"},
			wantMinCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			for _, n := range tt.createNames {
				createInstance(t, h, n)
			}

			rec := doRequest(t, h, "ListInstances", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)
			resp := parseResponse(t, rec)
			instances, ok := resp["Instances"].([]any)
			require.True(t, ok)
			assert.GreaterOrEqual(t, len(instances), tt.wantMinCount)
		})
	}
}

func TestDeleteInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		instanceArn string
		wantStatus  int
	}{
		{
			name:        "delete existing instance",
			instanceArn: "",
			wantStatus:  http.StatusOK,
		},
		{
			name:        "delete non-existing instance",
			instanceArn: "arn:aws:sso:::instance/ssoins-nonexistent",
			wantStatus:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			instanceArn := tt.instanceArn
			if instanceArn == "" {
				instanceArn = createInstance(t, h, "delete-me")
			}

			rec := doRequest(t, h, "DeleteInstance", map[string]any{"InstanceArn": instanceArn})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestDeleteInstance_MissingArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "missing_arn_returns_400",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequest(t, h, "DeleteInstance", map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
