package ec2_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// ---- Instance ---- //nolint:godot // existing issue.
func TestGetConsoleOutput(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	insts, _ := b.RunInstances("ami-test", "t3.micro", "", 1)
	inst := insts[0]

	t.Run("returns base64 output for known instance", func(t *testing.T) { //nolint:paralleltest // existing issue.
		output, ts, err := b.GetConsoleOutput(inst.ID)
		require.NoError(t, err)
		assert.NotEmpty(t, output)
		assert.False(t, ts.IsZero())
	})

	t.Run("unknown instance returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, _, err := b.GetConsoleOutput("i-doesnotexist")
		require.Error(t, err)
	})
}

func TestModifyInstanceMetadataOptions(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	insts, _ := b.RunInstances("ami-test", "t3.micro", "", 1)
	inst := insts[0]

	t.Run("sets required tokens", func(t *testing.T) { //nolint:paralleltest // existing issue.
		opts, err := b.ModifyInstanceMetadataOptions(inst.ID, "required", "enabled", "", 2)
		require.NoError(t, err)
		assert.Equal(t, "required", opts.HTTPTokens)
		assert.Equal(t, "applied", opts.State)
	})

	t.Run("unknown instance returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.ModifyInstanceMetadataOptions("i-missing", "required", "", "", 0)
		require.Error(t, err)
	})
}

func TestInstanceMetadataDefaults(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("get returns default values", func(t *testing.T) { //nolint:paralleltest // existing issue.
		d := b.GetInstanceMetadataDefaults()
		assert.NotNil(t, d)
	})

	t.Run("modify updates values", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifyInstanceMetadataDefaults("required", "enabled", "", 2))
		d := b.GetInstanceMetadataDefaults()
		assert.Equal(t, "required", d.HTTPTokens)
		assert.Equal(t, 2, d.HTTPPutResponseHopLimit)
	})
}

func TestInstanceCreditSpecifications(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	insts, _ := b.RunInstances("ami-test", "t3.micro", "", 1)
	inst := insts[0]

	t.Run("default returns standard", func(t *testing.T) { //nolint:paralleltest // existing issue.
		specs := b.DescribeInstanceCreditSpecifications([]string{inst.ID})
		require.Len(t, specs, 1)
		assert.Equal(t, "standard", specs[0].CPUCredits)
	})

	t.Run("modify changes to unlimited", func(t *testing.T) { //nolint:paralleltest // existing issue.
		successful, unsuccessful := b.ModifyInstanceCreditSpecification(
			[]ec2.InstanceCreditSpec{{InstanceID: inst.ID, CPUCredits: "unlimited"}},
		)
		require.Len(t, successful, 1)
		require.Empty(t, unsuccessful)
		specs := b.DescribeInstanceCreditSpecifications([]string{inst.ID})
		require.Len(t, specs, 1)
		assert.Equal(t, "unlimited", specs[0].CPUCredits)
	})
}

func TestInstanceTopology(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	insts, _ := b.RunInstances("ami-test", "t3.micro", "", 1)
	inst := insts[0]

	t.Run("returns topology for instance", func(t *testing.T) {
		items := b.DescribeInstanceTopology([]string{inst.ID})
		require.Len(t, items, 1)
		assert.Equal(t, inst.ID, items[0].InstanceID)
		assert.NotEmpty(t, items[0].AvailabilityZone)
	})
}

func TestMonitorUnmonitorInstances(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	insts, _ := b.RunInstances("ami-test", "t3.micro", "", 1)
	id := insts[0].ID

	t.Run("monitor sets enabled", func(t *testing.T) { //nolint:paralleltest // existing issue.
		states, err := b.MonitorInstances([]string{id})
		require.NoError(t, err)
		require.Len(t, states, 1)
		assert.Equal(t, "enabled", states[0].State)
	})

	t.Run("unmonitor sets disabled", func(t *testing.T) { //nolint:paralleltest // existing issue.
		states, err := b.UnmonitorInstances([]string{id})
		require.NoError(t, err)
		require.Len(t, states, 1)
		assert.Equal(t, "disabled", states[0].State)
	})

	t.Run("empty list returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.MonitorInstances(nil)
		require.Error(t, err)
	})
}

// ---- Network interface ---- //nolint:godot // existing issue.

func TestHTTP_GetConsoleOutput(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)

	insts, _ := b.RunInstances("ami-test", "t3.micro", "", 1)

	_, err := ec2.ExportDispatch(h, url.Values{
		"Action":     []string{"GetConsoleOutput"},
		"InstanceId": []string{insts[0].ID},
	})
	require.NoError(t, err)
}

func TestHTTP_MonitorInstances(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)

	insts, _ := b.RunInstances("ami-test", "t3.micro", "", 1)

	_, err := ec2.ExportDispatch(h, url.Values{
		"Action":       []string{"MonitorInstances"},
		"InstanceId.1": []string{insts[0].ID},
	})
	require.NoError(t, err)
}

// ---- Serial console ---- //nolint:godot // existing issue.
func TestSerialConsole(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("default is false", func(t *testing.T) { //nolint:paralleltest // existing issue.
		assert.False(t, b.GetSerialConsoleAccessStatus())
	})

	t.Run("enable sets true", func(t *testing.T) { //nolint:paralleltest // existing issue.
		b.EnableSerialConsoleAccess()
		assert.True(t, b.GetSerialConsoleAccessStatus())
	})

	t.Run("disable sets false", func(t *testing.T) { //nolint:paralleltest // existing issue.
		b.DisableSerialConsoleAccess()
		assert.False(t, b.GetSerialConsoleAccessStatus())
	})
}

// ---- VGW route propagation ---- //nolint:godot // existing issue.

// ---- Default credit specification ---- //nolint:godot // existing issue.
func TestDefaultCreditSpecification(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("default is standard", func(t *testing.T) { //nolint:paralleltest // existing issue.
		assert.Equal(t, "standard", b.GetDefaultCreditSpecification())
	})

	t.Run("modify changes setting", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifyDefaultCreditSpecification("unlimited-cpu-credits"))
		assert.Equal(t, "unlimited-cpu-credits", b.GetDefaultCreditSpecification())
	})

	t.Run("empty value returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.ModifyDefaultCreditSpecification(""))
	})
}

// ---- Replace root volume tasks ---- //nolint:godot // existing issue.

func TestHTTP_GetSerialConsoleAccessStatus(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	_, err := dispatchHandler(h, url.Values{
		"Action": []string{"GetSerialConsoleAccessStatus"},
	})
	require.NoError(t, err)
}

func TestHTTP_GetDefaultCreditSpecification(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	_, err := dispatchHandler(h, url.Values{
		"Action": []string{"GetDefaultCreditSpecification"},
	})
	require.NoError(t, err)
}

// ---- Instance Connect Endpoint ---- //nolint:godot // existing issue.
func TestInstanceConnectEndpoint(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var epID string

	t.Run("create endpoint", func(t *testing.T) { //nolint:paralleltest // existing issue.
		ep, err := b.CreateInstanceConnectEndpoint("subnet-default", nil, false)
		require.NoError(t, err)
		assert.NotEmpty(t, ep.InstanceConnectEndpointID)
		assert.Equal(t, "active", ep.State)
		epID = ep.InstanceConnectEndpointID
	})

	t.Run("describe returns created", func(t *testing.T) { //nolint:paralleltest // existing issue.
		eps := b.DescribeInstanceConnectEndpoints([]string{epID})
		require.Len(t, eps, 1)
		assert.Equal(t, "subnet-default", eps[0].SubnetID)
	})

	t.Run("modify preserveClientIP", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifyInstanceConnectEndpoint(epID, true))
	})

	t.Run("delete endpoint", func(t *testing.T) { //nolint:paralleltest // existing issue.
		deleted, err := b.DeleteInstanceConnectEndpoint(epID)
		require.NoError(t, err)
		assert.Equal(t, epID, deleted.InstanceConnectEndpointID)
		eps := b.DescribeInstanceConnectEndpoints(nil)
		assert.Empty(t, eps)
	})

	t.Run("unknown subnet returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CreateInstanceConnectEndpoint("subnet-missing", nil, false)
		require.Error(t, err)
	})
}

// ---- Instance Event Window ---- //nolint:godot // existing issue.

// ---- Instance Event Window ---- //nolint:godot // existing issue.
func TestInstanceEventWindow(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var ewID string

	t.Run("create window", func(t *testing.T) { //nolint:paralleltest // existing issue.
		ew, err := b.CreateInstanceEventWindow("test-window", "0 4 * * 6,7")
		require.NoError(t, err)
		assert.NotEmpty(t, ew.InstanceEventWindowID)
		ewID = ew.InstanceEventWindowID
	})

	t.Run("describe returns window", func(t *testing.T) { //nolint:paralleltest // existing issue.
		ews := b.DescribeInstanceEventWindows([]string{ewID})
		require.Len(t, ews, 1)
		assert.Equal(t, "0 4 * * 6,7", ews[0].CronExpression)
	})

	t.Run("modify window", func(t *testing.T) { //nolint:paralleltest // existing issue.
		modified, err := b.ModifyInstanceEventWindow(ewID, "updated", "0 3 * * *")
		require.NoError(t, err)
		assert.Equal(t, "updated", modified.Name)
		assert.Equal(t, "0 3 * * *", modified.CronExpression)
	})

	t.Run("delete window", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DeleteInstanceEventWindow(ewID))
	})
}

// ---- Spot Datafeed ---- //nolint:godot // existing issue.

// ---- Instance utilities ---- //nolint:godot // existing issue.
func TestGetPasswordData(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	insts, _ := b.RunInstances("ami-test", "t3.micro", "", 1)

	t.Run("returns empty for instance", func(t *testing.T) { //nolint:paralleltest // existing issue.
		data, _, err := b.GetPasswordData(insts[0].ID)
		require.NoError(t, err)
		assert.Empty(t, data)
	})

	t.Run("unknown instance returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, _, err := b.GetPasswordData("i-missing")
		require.Error(t, err)
	})
}

func TestGetInstanceTypesFromInstanceRequirements( //nolint:paralleltest // existing issue.
	t *testing.T,
) {
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("returns static list", func(t *testing.T) {
		types := b.GetInstanceTypesFromInstanceRequirements()
		assert.NotEmpty(t, types)
	})
}

// ---- VPN ---- //nolint:godot // existing issue.

func TestHTTP_GetInstanceTypesFromInstanceRequirements( //nolint:paralleltest // existing issue.
	t *testing.T,
) {
	h := newTestHandler()

	_, err := dispatchHandler(h, url.Values{
		"Action": []string{"GetInstanceTypesFromInstanceRequirements"},
	})
	require.NoError(t, err)
}

// TestGetDefaultCreditSpecification_EchoesInstanceFamily verifies that
// GetDefaultCreditSpecification and ModifyDefaultCreditSpecification echo the requested
// InstanceFamily back in the response, matching AWS EC2 behaviour.
// Previously both handlers hardcoded "t3" regardless of the request parameter.
func TestGetDefaultCreditSpecification_EchoesInstanceFamily(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		action     string
		family     string
		cpuCredits string
		wantFamily string
	}{
		{
			name:       "get_t2_family_echoed",
			action:     "GetDefaultCreditSpecification",
			family:     "t2",
			wantFamily: "t2",
		},
		{
			name:       "get_t3a_family_echoed",
			action:     "GetDefaultCreditSpecification",
			family:     "t3a",
			wantFamily: "t3a",
		},
		{
			name:       "modify_t4g_family_echoed",
			action:     "ModifyDefaultCreditSpecification",
			family:     "t4g",
			cpuCredits: "unlimited",
			wantFamily: "t4g",
		},
		{
			name:       "modify_t2_family_echoed",
			action:     "ModifyDefaultCreditSpecification",
			family:     "t2",
			cpuCredits: "standard",
			wantFamily: "t2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vals := url.Values{
				"Action":         {tt.action},
				"InstanceFamily": {tt.family},
			}
			if tt.cpuCredits != "" {
				vals.Set("CpuCredits", tt.cpuCredits)
			}

			resp, err := ec2.ExportDispatch(h, vals)
			require.NoError(t, err)
			assert.Contains(t, resp, "<instanceFamily>"+tt.wantFamily+"</instanceFamily>",
				"response must echo the requested InstanceFamily")
		})
	}
}

func TestSendDiagnosticInterruptHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	instances, err := h.Backend.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":     {"SendDiagnosticInterrupt"},
		"InstanceId": {instances[0].ID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<SendDiagnosticInterruptResponse")

	rec := postForm(t, h, "Action=SendDiagnosticInterrupt&InstanceId=i-missing")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidInstanceID.NotFound")
}

func TestDescribeElasticGpusHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp, err := ec2.ExportDispatch(h, url.Values{"Action": {"DescribeElasticGpus"}})
	require.NoError(t, err)
	assert.Contains(t, resp, "<DescribeElasticGpusResponse>")
	assert.Contains(t, resp, "<elasticGpuSet>")
}

// TestAllOpsReturn200HTTP is a broad smoke test verifying every
// de-stubbed op returns 200 OK through the full HTTP handler for a minimal
// (frequently zero-value/empty) request, mirroring the coverage the retired
// registerStubOps registration used to get for free.

// TestHandlerDescribeInstanceTypeOfferings covers handleDescribeInstanceTypeOfferings.
func TestHandlerDescribeInstanceTypeOfferings(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Action=DescribeInstanceTypeOfferings&Version=2016-11-15")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeInstanceTypeOfferingsResponse")
}

// TestHandlerVpcPeeringConnectionHandlers covers handleCreateVpcPeeringConnection,
// handleDeleteVpcPeeringConnection, and handleRejectVpcPeeringConnection.

func TestRunInstances_SecurityGroupIds_Stored(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	vpc, err := b.CreateVpc("10.0.0.0/16")
	require.NoError(t, err)

	sg, err := b.CreateSecurityGroup("my-sg", "My SG", vpc.ID)
	require.NoError(t, err)

	vals := url.Values{
		"Action":            {"RunInstances"},
		"Version":           {"2016-11-15"},
		"ImageId":           {"ami-123"},
		"InstanceType":      {"t3.micro"},
		"MinCount":          {"1"},
		"MaxCount":          {"1"},
		"SecurityGroupId.1": {sg.ID},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, sg.ID, "security group ID should appear in response")
}

func TestRunInstances_SecurityGroupIds_InDescribeInstances(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	vpc, err := b.CreateVpc("10.0.0.0/16")
	require.NoError(t, err)

	sg, err := b.CreateSecurityGroup("web-sg", "Web SG", vpc.ID)
	require.NoError(t, err)

	runVals := url.Values{
		"Action":            {"RunInstances"},
		"Version":           {"2016-11-15"},
		"ImageId":           {"ami-123"},
		"InstanceType":      {"t3.micro"},
		"MinCount":          {"1"},
		"MaxCount":          {"1"},
		"SecurityGroupId.1": {sg.ID},
	}

	runResp, err := ec2.ExportDispatch(h, runVals)
	require.NoError(t, err)
	instanceID := accuracyExtractXMLValue(runResp, "instanceId")
	require.NotEmpty(t, instanceID)

	descVals := url.Values{
		"Action":       {"DescribeInstances"},
		"Version":      {"2016-11-15"},
		"InstanceId.1": {instanceID},
	}

	descResp, err := ec2.ExportDispatch(h, descVals)
	require.NoError(t, err)
	assert.Contains(t, descResp, sg.ID, "security group ID should be in DescribeInstances groupSet")
}

func TestRunInstances_SecurityGroupIds_MultipleGroups(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	vpc, err := b.CreateVpc("10.0.0.0/16")
	require.NoError(t, err)

	sg1, err := b.CreateSecurityGroup("web", "Web", vpc.ID)
	require.NoError(t, err)

	sg2, err := b.CreateSecurityGroup("db", "DB", vpc.ID)
	require.NoError(t, err)

	vals := url.Values{
		"Action":            {"RunInstances"},
		"Version":           {"2016-11-15"},
		"ImageId":           {"ami-123"},
		"InstanceType":      {"t3.micro"},
		"MinCount":          {"1"},
		"MaxCount":          {"1"},
		"SecurityGroupId.1": {sg1.ID},
		"SecurityGroupId.2": {sg2.ID},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, sg1.ID)
	assert.Contains(t, resp, sg2.ID)
}

func TestRunInstances_InvalidSecurityGroupId_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	vals := url.Values{
		"Action":            {"RunInstances"},
		"Version":           {"2016-11-15"},
		"ImageId":           {"ami-123"},
		"InstanceType":      {"t3.micro"},
		"MinCount":          {"1"},
		"MaxCount":          {"1"},
		"SecurityGroupId.1": {"sg-doesnotexist"},
	}

	_, err := ec2.ExportDispatch(h, vals)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NotFound")
}

// ---- Gap D: RunInstances MinCount/MaxCount validation ----

// TestRunInstances_MinMaxCountValidation covers RunInstances' MinCount/MaxCount
// validation rules: both must be positive, and MaxCount must be >= MinCount.

func TestRunInstances_MinMaxCountValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		minCount      string
		maxCount      string
		wantErrSubstr string
		wantErr       bool
	}{
		{
			name: "MaxCountLessThanMinCount_Rejected", minCount: "3", maxCount: "1",
			wantErr: true, wantErrSubstr: "MaxCount",
		},
		{name: "MaxCountZero_Rejected", minCount: "1", maxCount: "0", wantErr: true},
		{name: "MinCountZero_Rejected", minCount: "0", maxCount: "1", wantErr: true},
		{name: "NegativeMinCount_Rejected", minCount: "-1", maxCount: "1", wantErr: true},
		{name: "NegativeMaxCount_Rejected", minCount: "1", maxCount: "-5", wantErr: true},
		{name: "EqualMinMaxCount_OK", minCount: "2", maxCount: "2", wantErr: false},
		{name: "MaxCountGreaterThanMinCount_OK", minCount: "1", maxCount: "5", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			vals := url.Values{
				"Action":       {"RunInstances"},
				"Version":      {"2016-11-15"},
				"ImageId":      {"ami-123"},
				"InstanceType": {"t3.micro"},
				"MinCount":     {tt.minCount},
				"MaxCount":     {tt.maxCount},
			}

			resp, err := ec2.ExportDispatch(h, vals)
			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrSubstr != "" {
					assert.Contains(t, err.Error(), tt.wantErrSubstr)
				}

				return
			}

			require.NoError(t, err)
			assert.Contains(t, resp, "RunInstancesResponse")
		})
	}
}

// ---- Gap E: AssociateAddress NetworkInterfaceId support ----

func TestRunInstances_NoSecurityGroups_GroupSetEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	vals := url.Values{
		"Action":       {"RunInstances"},
		"Version":      {"2016-11-15"},
		"ImageId":      {"ami-123"},
		"InstanceType": {"t3.micro"},
		"MinCount":     {"1"},
		"MaxCount":     {"1"},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	// No SGs specified — groupSet should have no groupId items.
	assert.NotContains(t, resp, "<groupId>")
}

// ---- Gap G: DescribeInstances filter combined with instance IDs ----

func TestRunInstances_NoMaxCount_DefaultsToMinCount(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	vals := url.Values{
		"Action":       {"RunInstances"},
		"Version":      {"2016-11-15"},
		"ImageId":      {"ami-123"},
		"InstanceType": {"t3.micro"},
		"MinCount":     {"2"},
		// MaxCount omitted — should default to MinCount.
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, "RunInstancesResponse")
}

func TestRunInstances_NoMinCount_DefaultsToOne(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	vals := url.Values{
		"Action":       {"RunInstances"},
		"Version":      {"2016-11-15"},
		"ImageId":      {"ami-123"},
		"InstanceType": {"t3.micro"},
		// MinCount omitted — should default to 1.
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, "RunInstancesResponse")
}

// ---- Gap I: DescribeSecurityGroups filter support ----

func TestRunInstances_WithDefaultVPC_HasDefaultSG(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	sgs := b.DescribeSecurityGroups(nil)

	// Default VPC comes with a default security group.
	found := false

	for _, sg := range sgs {
		if sg.Name == "default" {
			found = true

			break
		}
	}

	assert.True(t, found, "default VPC should have a default security group")
}

// ---- Gap K: DescribeInstances with no filters returns all instances ----

func TestRunInstances_GroupSetPresentInResponse(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := newTestHandlerWithBackend(b)

	vpc, err := b.CreateVpc("10.0.0.0/16")
	require.NoError(t, err)

	sg, err := b.CreateSecurityGroup("test-sg", "Test SG", vpc.ID)
	require.NoError(t, err)

	vals := url.Values{
		"Action":            {"RunInstances"},
		"Version":           {"2016-11-15"},
		"ImageId":           {"ami-123"},
		"InstanceType":      {"t3.micro"},
		"MinCount":          {"1"},
		"MaxCount":          {"1"},
		"SecurityGroupId.1": {sg.ID},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, "<groupSet>")
	assert.Contains(t, resp, "<groupId>"+sg.ID+"</groupId>")
}

// ---- parseEC2Filters helper verification ----
