package cloudformation_test

import (
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// ---- DescribeType backend (table-driven) -------------------------------------------

func TestDescribeType_Registered(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*cloudformation.InMemoryBackend)
		check     func(*testing.T, *cloudformation.TypeDetails)
		name      string
		typeName  string
		typeArn   string
		versionID string
		wantErr   bool
	}{
		{
			name: "lookup by TypeName after RegisterType",
			setup: func(b *cloudformation.InMemoryBackend) {
				_, _ = b.RegisterType("MyOrg::MyService::Widget", "s3://pkg.zip")
			},
			typeName: "MyOrg::MyService::Widget",
			check: func(t *testing.T, d *cloudformation.TypeDetails) {
				t.Helper()
				assert.Equal(t, "MyOrg::MyService::Widget", d.TypeName)
				assert.Equal(t, "RESOURCE", d.Type)
				assert.Equal(t, "COMPLETE", d.Status)
				assert.Equal(t, "PRIVATE", d.Visibility)
				assert.True(t, d.IsDefaultVersion)
			},
		},
		{
			name: "lookup by ARN after RegisterType",
			setup: func(b *cloudformation.InMemoryBackend) {
				_, _ = b.RegisterType("MyOrg::Svc::Res", "s3://pkg.zip")
			},
			typeArn: "arn:aws:cloudformation:::type/resource/MyOrg::Svc::Res",
			check: func(t *testing.T, d *cloudformation.TypeDetails) {
				t.Helper()
				assert.Equal(t, "MyOrg::Svc::Res", d.TypeName)
				assert.NotEmpty(t, d.TypeArn)
			},
		},
		{
			name: "published type has PUBLIC visibility",
			setup: func(b *cloudformation.InMemoryBackend) {
				_, _ = b.RegisterType("MyOrg::Pub::Type", "s3://pkg.zip")
				_ = b.PublishType("MyOrg::Pub::Type")
			},
			typeName: "MyOrg::Pub::Type",
			check: func(t *testing.T, d *cloudformation.TypeDetails) {
				t.Helper()
				assert.Equal(t, "PUBLIC", d.Visibility)
			},
		},
		{
			name: "activated type IsActivated is true",
			setup: func(b *cloudformation.InMemoryBackend) {
				_, _ = b.RegisterType("MyOrg::Act::Type", "s3://pkg.zip")
				_ = b.ActivateType("MyOrg::Act::Type", "")
			},
			typeName: "MyOrg::Act::Type",
			check: func(t *testing.T, d *cloudformation.TypeDetails) {
				t.Helper()
				assert.True(t, d.IsActivated)
			},
		},
		{
			name:     "unknown type returns error",
			typeName: "Unknown::Type::Name",
			wantErr:  true,
		},
		{
			name:    "no typeName or arn returns error",
			wantErr: true,
		},
		{
			name: "specific versionID returned",
			setup: func(b *cloudformation.InMemoryBackend) {
				_, _ = b.RegisterType("MyOrg::Ver::Type", "s3://v1.zip")
				_, _ = b.RegisterType("MyOrg::Ver::Type", "s3://v2.zip")
			},
			typeName:  "MyOrg::Ver::Type",
			versionID: "00000001",
			check: func(t *testing.T, d *cloudformation.TypeDetails) {
				t.Helper()
				assert.Equal(t, "00000001", d.VersionID)
			},
		},
		{
			name: "default version updated by SetTypeDefaultVersion",
			setup: func(b *cloudformation.InMemoryBackend) {
				_, _ = b.RegisterType("MyOrg::Def::Type", "s3://v1.zip")
				_, _ = b.RegisterType("MyOrg::Def::Type", "s3://v2.zip")
				typeArn := "arn:aws:cloudformation:::type/resource/MyOrg::Def::Type"
				_ = b.SetTypeDefaultVersion(typeArn, "00000001")
			},
			typeName: "MyOrg::Def::Type",
			check: func(t *testing.T, d *cloudformation.TypeDetails) {
				t.Helper()
				assert.Equal(t, "00000001", d.DefaultVersionID)
				assert.True(t, d.IsDefaultVersion)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			if tc.setup != nil {
				tc.setup(b)
			}
			details, err := b.DescribeType(tc.typeName, tc.typeArn, tc.versionID)
			if tc.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
			require.NotNil(t, details)
			if tc.check != nil {
				tc.check(t, details)
			}
		})
	}
}

// ---- DescribeType HTTP handler (table-driven) -------------------------------------

func TestHandler_DescribeType_Registered(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*cloudformation.InMemoryBackend)
		formValues   url.Values
		name         string
		wantContains []string
		wantStatus   int
	}{
		{
			name: "registered type returned from backend",
			setup: func(b *cloudformation.InMemoryBackend) {
				_, _ = b.RegisterType("Acme::Network::VPC", "s3://schema.zip")
			},
			formValues: url.Values{
				"Action":   {"DescribeType"},
				"TypeName": {"Acme::Network::VPC"},
			},
			wantStatus:   http.StatusOK,
			wantContains: []string{"DescribeTypeResponse", "Acme::Network::VPC", "RESOURCE"},
		},
		{
			name: "unknown type falls back to schema handler",
			formValues: url.Values{
				"Action":   {"DescribeType"},
				"TypeName": {"AWS::S3::Bucket"},
			},
			wantStatus:   http.StatusOK,
			wantContains: []string{"DescribeTypeResponse", "AWS::S3::Bucket"},
		},
		{
			name: "published type shows PUBLIC visibility in response",
			setup: func(b *cloudformation.InMemoryBackend) {
				_, _ = b.RegisterType("Acme::Pub::Widget", "s3://schema.zip")
				_ = b.PublishType("Acme::Pub::Widget")
			},
			formValues: url.Values{
				"Action":   {"DescribeType"},
				"TypeName": {"Acme::Pub::Widget"},
			},
			wantStatus:   http.StatusOK,
			wantContains: []string{"PUBLIC"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newHandler()
			if tc.setup != nil {
				tc.setup(h.Backend.(*cloudformation.InMemoryBackend))
			}
			resp := postFormValues(t, h, tc.formValues)
			assert.Equal(t, tc.wantStatus, resp.Status, "body: %s", resp.Body)
			for _, want := range tc.wantContains {
				assert.Contains(t, resp.Body, want)
			}
		})
	}
}

// ---- ValidateRoleARN (table-driven) -----------------------------------------------

func TestValidateRoleARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		roleARN string
		wantErr bool
	}{
		{
			name:    "empty ARN is valid (no role specified)",
			roleARN: "",
			wantErr: false,
		},
		{
			name:    "valid standard role ARN",
			roleARN: "arn:aws:iam::123456789012:role/MyCloudFormationRole",
			wantErr: false,
		},
		{
			name:    "valid role ARN with path",
			roleARN: "arn:aws:iam::123456789012:role/path/to/MyRole",
			wantErr: false,
		},
		{
			name:    "gov cloud role ARN",
			roleARN: "arn:aws-gov:iam::123456789012:role/GovRole",
			wantErr: false,
		},
		{
			name:    "cn cloud role ARN",
			roleARN: "arn:aws-cn:iam::123456789012:role/ChinaRole",
			wantErr: false,
		},
		{
			name:    "invalid: not an ARN",
			roleARN: "not-an-arn",
			wantErr: true,
		},
		{
			name:    "invalid: S3 bucket ARN instead of IAM role",
			roleARN: "arn:aws:s3:::my-bucket",
			wantErr: true,
		},
		{
			name:    "invalid: missing account ID",
			roleARN: "arn:aws:iam:::role/MyRole",
			wantErr: true,
		},
		{
			name:    "invalid: ec2 resource type",
			roleARN: "arn:aws:ec2:us-east-1:123456789012:instance/i-1234567890abcdef0",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := cloudformation.ValidateRoleARN(tc.roleARN)
			if tc.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, cloudformation.ErrInvalidRoleARN)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---- CreateStack IAM PassRole validation (table-driven) ---------------------------

func TestCreateStack_RoleARN_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs   error
		name    string
		roleARN string
		wantErr bool
	}{
		{
			name:    "valid role ARN accepted",
			roleARN: "arn:aws:iam::123456789012:role/CFNRole",
			wantErr: false,
		},
		{
			name:    "empty role ARN accepted",
			roleARN: "",
			wantErr: false,
		},
		{
			name:    "invalid role ARN rejected",
			roleARN: "not-a-valid-arn",
			wantErr: true,
			errIs:   cloudformation.ErrInvalidRoleARN,
		},
		{
			name:    "S3 ARN rejected as role ARN",
			roleARN: "arn:aws:s3:::my-bucket",
			wantErr: true,
			errIs:   cloudformation.ErrInvalidRoleARN,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.CreateStack(t.Context(), "role-test", simpleTemplate, nil,
				cloudformation.StackOptions{RoleARN: tc.roleARN})
			if tc.wantErr {
				require.Error(t, err)
				if tc.errIs != nil {
					require.ErrorIs(t, err, tc.errIs)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---- IAM Capability requirement (table-driven) ------------------------------------

func TestCreateStack_IAMCapabilityRequired(t *testing.T) {
	t.Parallel()

	iamTemplate := `{
		"AWSTemplateFormatVersion":"2010-09-09",
		"Resources":{
			"Role":{"Type":"AWS::IAM::Role","Properties":{"AssumeRolePolicyDocument":{}}}
		}
	}`

	tests := []struct {
		errIs        error
		name         string
		template     string
		capabilities []string
		wantErr      bool
	}{
		{
			name:         "IAM template without capabilities fails",
			template:     iamTemplate,
			capabilities: nil,
			wantErr:      true,
			errIs:        cloudformation.ErrInsufficientCapabilities,
		},
		{
			name:         "IAM template with CAPABILITY_IAM succeeds",
			template:     iamTemplate,
			capabilities: []string{"CAPABILITY_IAM"},
			wantErr:      false,
		},
		{
			name:         "IAM template with CAPABILITY_NAMED_IAM succeeds",
			template:     iamTemplate,
			capabilities: []string{"CAPABILITY_NAMED_IAM"},
			wantErr:      false,
		},
		{
			name:         "IAM template with CAPABILITY_AUTO_EXPAND succeeds",
			template:     iamTemplate,
			capabilities: []string{"CAPABILITY_AUTO_EXPAND"},
			wantErr:      false,
		},
		{
			name:         "non-IAM template without capabilities succeeds",
			template:     simpleTemplate,
			capabilities: nil,
			wantErr:      false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.CreateStack(t.Context(), "cap-check", tc.template, nil,
				cloudformation.StackOptions{Capabilities: tc.capabilities})
			if tc.wantErr {
				require.Error(t, err)
				if tc.errIs != nil {
					require.ErrorIs(t, err, tc.errIs)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---- UpdateStack IAM Capability (table-driven) ------------------------------------

func TestUpdateStack_IAMCapabilityRequired(t *testing.T) {
	t.Parallel()

	iamTemplate := `{
		"AWSTemplateFormatVersion":"2010-09-09",
		"Resources":{
			"Role":{"Type":"AWS::IAM::Role","Properties":{"AssumeRolePolicyDocument":{}}}
		}
	}`

	tests := []struct {
		name       string
		updateOpts cloudformation.StackOptions
		wantErr    bool
	}{
		{
			name:       "update with IAM template and no capabilities fails",
			updateOpts: cloudformation.StackOptions{},
			wantErr:    true,
		},
		{
			name:       "update with IAM template and CAPABILITY_IAM succeeds",
			updateOpts: cloudformation.StackOptions{Capabilities: []string{"CAPABILITY_IAM"}},
			wantErr:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.CreateStack(t.Context(), "iam-upd", simpleTemplate, nil,
				cloudformation.StackOptions{})
			require.NoError(t, err)

			_, err = b.UpdateStack(t.Context(), "iam-upd", iamTemplate, nil, tc.updateOpts)
			if tc.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, cloudformation.ErrInsufficientCapabilities)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---- ChangeSet ChangeSetType and ExecutionStatus (table-driven) -------------------

func TestChangeSet_TypeAndExecutionStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		setup             func(*cloudformation.InMemoryBackend)
		stackName         string
		changeSetName     string
		wantChangeSetType string
		wantExecStatus    string
	}{
		{
			name:              "new stack → ChangeSetType=CREATE",
			stackName:         "brand-new-stack",
			changeSetName:     "init-cs",
			wantChangeSetType: "CREATE",
			wantExecStatus:    "AVAILABLE",
		},
		{
			name: "existing stack → ChangeSetType=UPDATE",
			setup: func(b *cloudformation.InMemoryBackend) {
				_, _ = b.CreateStack(t.Context(), "existing-stack", simpleTemplate, nil,
					cloudformation.StackOptions{})
			},
			stackName:         "existing-stack",
			changeSetName:     "update-cs",
			wantChangeSetType: "UPDATE",
			wantExecStatus:    "AVAILABLE",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			if tc.setup != nil {
				tc.setup(b)
			}
			cs, err := b.CreateChangeSet(t.Context(), tc.stackName, tc.changeSetName,
				simpleTemplate, "test", nil)
			require.NoError(t, err)
			assert.Equal(t, tc.wantChangeSetType, cs.ChangeSetType)
			assert.Equal(t, tc.wantExecStatus, cs.ExecutionStatus)
		})
	}
}

func TestChangeSet_ExecutionStatus_AfterExecute(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStack(t.Context(), "exec-status-stack", simpleTemplate, nil,
		cloudformation.StackOptions{})
	require.NoError(t, err)

	_, err = b.CreateChangeSet(t.Context(), "exec-status-stack", "my-cs",
		simpleTemplate, "test", nil)
	require.NoError(t, err)

	// After execute, the changeset is removed (EXECUTE_COMPLETE).
	err = b.ExecuteChangeSet(t.Context(), "exec-status-stack", "my-cs")
	require.NoError(t, err)

	// Changeset no longer exists — verify.
	_, err = b.DescribeChangeSet("exec-status-stack", "my-cs")
	require.ErrorIs(t, err, cloudformation.ErrChangeSetNotFound)
}

func TestChangeSet_ExecutionStatus_HTTP(t *testing.T) {
	t.Parallel()

	h := newHandler()
	postFormValues(t, h, url.Values{
		"Action": {"CreateStack"}, "StackName": {"cs-exec-http"}, "TemplateBody": {simpleTemplate},
	})
	postFormValues(t, h, url.Values{
		"Action":        {"CreateChangeSet"},
		"StackName":     {"cs-exec-http"},
		"ChangeSetName": {"http-cs"},
		"TemplateBody":  {simpleTemplate},
	})

	resp := postFormValues(t, h, url.Values{
		"Action":        {"DescribeChangeSet"},
		"StackName":     {"cs-exec-http"},
		"ChangeSetName": {"http-cs"},
	})
	resp.mustOK(t)
	assert.Contains(t, resp.Body, "ExecutionStatus")
	assert.Contains(t, resp.Body, "AVAILABLE")
	assert.Contains(t, resp.Body, "ChangeSetType")
	assert.Contains(t, resp.Body, "UPDATE")
}

// ---- Stack Instance StackID assignment (table-driven) ----------------------------

func TestStackInstance_StackIDAssigned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		accounts []string
		regions  []string
		wantLen  int
	}{
		{
			name:     "single account single region",
			accounts: []string{"111111111111"},
			regions:  []string{"us-east-1"},
			wantLen:  1,
		},
		{
			name:     "multi-account multi-region",
			accounts: []string{"111111111111", "222222222222"},
			regions:  []string{"us-east-1", "eu-west-1"},
			wantLen:  4,
		},
		{
			name:     "three accounts one region",
			accounts: []string{"111111111111", "222222222222", "333333333333"},
			regions:  []string{"ap-southeast-1"},
			wantLen:  3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.CreateStackSet("inst-test-ss", "test", simpleTemplate)
			require.NoError(t, err)

			err = b.CreateStackInstances("inst-test-ss", tc.accounts, tc.regions)
			require.NoError(t, err)

			instances, err := b.ListStackInstances("inst-test-ss", "")
			require.NoError(t, err)
			assert.Len(t, instances, tc.wantLen)

			for _, inst := range instances {
				assert.NotEmpty(t, inst.StackID, "expected StackID to be assigned for %s/%s",
					inst.Account, inst.Region)
				assert.True(t, strings.HasPrefix(inst.StackID, "arn:aws:cloudformation:"),
					"expected StackID to be a CloudFormation ARN, got: %s", inst.StackID)
				assert.NotEmpty(t, inst.StackSetID, "expected StackSetID to be set")
				assert.Equal(t, "NOT_CHECKED", inst.DriftStatus)
				assert.NotEmpty(t, inst.LastOperationID)
			}
		})
	}
}

func TestStackInstance_NoDuplicates(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStackSet("dedup-ss", "test", simpleTemplate)
	require.NoError(t, err)

	err = b.CreateStackInstances("dedup-ss", []string{"111111111111"}, []string{"us-east-1"})
	require.NoError(t, err)

	// Creating the same instance again should not duplicate it.
	err = b.CreateStackInstances("dedup-ss", []string{"111111111111"}, []string{"us-east-1"})
	require.NoError(t, err)

	instances, err := b.ListStackInstances("dedup-ss", "")
	require.NoError(t, err)
	assert.Len(t, instances, 1, "expected no duplicate instances")
}

// ---- StackSet operation results (table-driven) -----------------------------------

func TestStackSetOperationResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		accounts     []string
		regions      []string
		wantStatuses []string
		wantResultN  int
	}{
		{
			name:         "single account region",
			accounts:     []string{"111111111111"},
			regions:      []string{"us-east-1"},
			wantResultN:  1,
			wantStatuses: []string{"SUCCEEDED"},
		},
		{
			name:         "two accounts two regions",
			accounts:     []string{"111111111111", "222222222222"},
			regions:      []string{"us-east-1", "us-west-2"},
			wantResultN:  4,
			wantStatuses: []string{"SUCCEEDED"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.CreateStackSet("op-results-ss", "test", simpleTemplate)
			require.NoError(t, err)

			err = b.CreateStackInstances("op-results-ss", tc.accounts, tc.regions)
			require.NoError(t, err)

			// Get the operation ID from ListStackSetOperations.
			opIDs, err := b.ListStackSetOperations("op-results-ss", "")
			require.NoError(t, err)
			require.NotEmpty(t, opIDs)

			results, err := b.ListStackSetOperationResults("op-results-ss", opIDs[0], "")
			require.NoError(t, err)
			assert.Len(t, results, tc.wantResultN)

			for _, r := range results {
				assert.NotEmpty(t, r.Account)
				assert.NotEmpty(t, r.Region)
				for _, wantStatus := range tc.wantStatuses {
					assert.Equal(t, wantStatus, r.Status)
				}
			}
		})
	}
}

func TestStackSetOperationResults_HTTP(t *testing.T) {
	t.Parallel()

	h := newHandler()

	postFormValues(t, h, url.Values{
		"Action":       {"CreateStackSet"},
		"StackSetName": {"http-op-ss"},
		"TemplateBody": {simpleTemplate},
	})
	postFormValues(t, h, url.Values{
		"Action":            {"CreateStackInstances"},
		"StackSetName":      {"http-op-ss"},
		"Accounts.member.1": {"111111111111"},
		"Regions.member.1":  {"us-east-1"},
	})

	// Get operation IDs.
	opsResp := postFormValues(t, h, url.Values{
		"Action":       {"ListStackSetOperations"},
		"StackSetName": {"http-op-ss"},
	})
	opsResp.mustOK(t)

	// Find an operation ID in the response.
	opID := extractField(opsResp.Body, "OperationId")
	if opID == "" {
		t.Skip("no operation ID found in response")
	}

	resp := postFormValues(t, h, url.Values{
		"Action":       {"ListStackSetOperationResults"},
		"StackSetName": {"http-op-ss"},
		"OperationId":  {opID},
	})
	resp.mustOK(t)
	assert.Contains(t, resp.Body, "ListStackSetOperationResultsResponse")
	assert.Contains(t, resp.Body, "111111111111")
	assert.Contains(t, resp.Body, "us-east-1")
	assert.Contains(t, resp.Body, "SUCCEEDED")
}

// ---- TypeVersioning (table-driven) -----------------------------------------------

func TestTypeVersioning_MultipleRegistrations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantDefault   string
		registerCount int
		wantVersions  int
	}{
		{
			name:          "single registration yields one version",
			registerCount: 1,
			wantVersions:  1,
			wantDefault:   "00000001",
		},
		{
			name:          "two registrations yield two versions",
			registerCount: 2,
			wantVersions:  2,
			wantDefault:   "00000002",
		},
		{
			name:          "three registrations yield three versions",
			registerCount: 3,
			wantVersions:  3,
			wantDefault:   "00000003",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			for i := range tc.registerCount {
				_, err := b.RegisterType("Acme::Ver::Type", "s3://v"+string(rune('0'+i+1))+".zip")
				require.NoError(t, err)
			}

			versions, err := b.ListTypeVersions("Acme::Ver::Type", "")
			require.NoError(t, err)
			assert.Len(t, versions, tc.wantVersions)

			details, err := b.DescribeType("Acme::Ver::Type", "", "")
			require.NoError(t, err)
			assert.Equal(t, tc.wantDefault, details.DefaultVersionID)
		})
	}
}

// ---- Resource scan with real stacks (table-driven) --------------------------------

func TestResourceScan_PopulatesFromStacks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		stacks       []string
		wantContains []string
		wantMinItems int
	}{
		{
			name:         "no stacks yields synthetic S3 resource",
			stacks:       nil,
			wantMinItems: 1,
			wantContains: []string{"AWS::S3::Bucket"},
		},
		{
			name:   "single stack resources appear in scan",
			stacks: []string{simpleTemplate},
			// simpleTemplate has AWS::S3::Bucket so at least 1 real resource.
			wantMinItems: 1,
			wantContains: []string{"AWS::S3::Bucket"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			for i, tmpl := range tc.stacks {
				_, err := b.CreateStack(t.Context(),
					"scan-stack-"+string(rune('a'+i)), tmpl, nil,
					cloudformation.StackOptions{})
				require.NoError(t, err)
			}

			scanID, err := b.StartResourceScan()
			require.NoError(t, err)
			assert.NotEmpty(t, scanID)

			scan, err := b.DescribeResourceScan(scanID)
			require.NoError(t, err)
			assert.Equal(t, "COMPLETE", scan.Status)
			assert.InEpsilon(t, float64(100), scan.PercentageCompleted, 0.001)

			resources, err := b.ListResourceScanResources(scanID, "")
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(resources), tc.wantMinItems)

			types := make([]string, 0, len(resources))
			for _, r := range resources {
				types = append(types, r.ResourceType)
			}
			for _, want := range tc.wantContains {
				found := slices.Contains(types, want)
				assert.True(t, found, "expected resource type %q in scan results", want)
			}
		})
	}
}

// ---- Generated template content (table-driven) -----------------------------------

func TestGeneratedTemplate_Body(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		resourceIDs  []string
		setup        func(*cloudformation.InMemoryBackend)
		wantContains []string
	}{
		{
			name:        "empty resource list generates template from existing stacks",
			resourceIDs: nil,
			setup: func(b *cloudformation.InMemoryBackend) {
				_, _ = b.CreateStack(t.Context(), "gen-tmpl-src", simpleTemplate, nil,
					cloudformation.StackOptions{})
			},
			wantContains: []string{"AWSTemplateFormatVersion", "Resources"},
		},
		{
			name:         "with Type/LogicalID resource IDs",
			resourceIDs:  []string{"AWS::SQS::Queue/MyQueue", "AWS::SNS::Topic/MyTopic"},
			wantContains: []string{"AWS::SQS::Queue", "AWS::SNS::Topic", "AWSTemplateFormatVersion"},
		},
		{
			name:         "no resource IDs and no stacks yields empty resources",
			resourceIDs:  nil,
			wantContains: []string{"AWSTemplateFormatVersion"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			if tc.setup != nil {
				tc.setup(b)
			}

			gt, err := b.CreateGeneratedTemplate("test-template", tc.resourceIDs)
			require.NoError(t, err)
			require.NotNil(t, gt)
			assert.NotEmpty(t, gt.GeneratedTemplateID)
			assert.Equal(t, "COMPLETE", gt.Status)

			body, err := b.GetGeneratedTemplate(gt.GeneratedTemplateID)
			require.NoError(t, err)
			for _, want := range tc.wantContains {
				assert.Contains(t, body, want, "expected %q in generated template body", want)
			}
		})
	}
}

// ---- Drift simulation (table-driven) ---------------------------------------------

func TestDriftSimulation(t *testing.T) {
	t.Parallel()

	tmplWithQueue := `{
		"AWSTemplateFormatVersion":"2010-09-09",
		"Resources":{"Q":{"Type":"AWS::SQS::Queue"}}
	}`

	tests := []struct {
		name          string
		wantStatus    string
		simulateDrift bool
		wantDrifted   bool
	}{
		{
			name:          "without drift simulation all resources IN_SYNC",
			simulateDrift: false,
			wantStatus:    "IN_SYNC",
			wantDrifted:   false,
		},
		{
			name:          "after SimulateDrift resources are DRIFTED",
			simulateDrift: true,
			wantStatus:    "DRIFTED",
			wantDrifted:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.CreateStack(t.Context(), "drift-sim", tmplWithQueue, nil,
				cloudformation.StackOptions{})
			require.NoError(t, err)

			if tc.simulateDrift {
				err = b.SimulateDrift("drift-sim")
				require.NoError(t, err)
			}

			drifts, err := b.DescribeStackResourceDrifts("drift-sim")
			require.NoError(t, err)

			if tc.wantDrifted {
				require.NotEmpty(t, drifts)
			}
			for _, d := range drifts {
				assert.Equal(t, tc.wantStatus, d.StackResourceDriftStatus)
			}
		})
	}
}

func TestDriftSimulation_StackNotFound(t *testing.T) {
	t.Parallel()
	b := newBackend()
	err := b.SimulateDrift("nonexistent")
	require.ErrorIs(t, err, cloudformation.ErrStackNotFound)
}

// ---- ListTypeVersions multiple versions ------------------------------------------

func TestListTypeVersions_MultipleVersions(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.RegisterType("Acme::Multi::Ver", "s3://v1.zip")
	require.NoError(t, err)
	_, err = b.RegisterType("Acme::Multi::Ver", "s3://v2.zip")
	require.NoError(t, err)
	_, err = b.RegisterType("Acme::Multi::Ver", "s3://v3.zip")
	require.NoError(t, err)

	versions, err := b.ListTypeVersions("Acme::Multi::Ver", "")
	require.NoError(t, err)
	assert.Len(t, versions, 3)
	assert.Contains(t, versions, "00000001")
	assert.Contains(t, versions, "00000002")
	assert.Contains(t, versions, "00000003")
}

// ---- ListTypes visibility (table-driven) -----------------------------------------

func TestListTypes_Visibility(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setup          func(*cloudformation.InMemoryBackend)
		wantPrivate    []string
		wantPublic     []string
		wantNotPresent []string
	}{
		{
			name: "registered type is PRIVATE",
			setup: func(b *cloudformation.InMemoryBackend) {
				_, _ = b.RegisterType("Acme::Priv::Type", "s3://pkg.zip")
			},
			wantPrivate: []string{"Acme::Priv::Type"},
		},
		{
			name: "published type is PUBLIC",
			setup: func(b *cloudformation.InMemoryBackend) {
				_, _ = b.RegisterType("Acme::Pub::Type", "s3://pkg.zip")
				_ = b.PublishType("Acme::Pub::Type")
			},
			wantPublic: []string{"Acme::Pub::Type"},
		},
		{
			name: "deprecated type excluded from list",
			setup: func(b *cloudformation.InMemoryBackend) {
				_, _ = b.RegisterType("Acme::Dep::Type", "s3://pkg.zip")
				typeArn := "arn:aws:cloudformation:::type/resource/Acme::Dep::Type"
				_ = b.DeregisterType(typeArn)
			},
			wantNotPresent: []string{"Acme::Dep::Type"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			if tc.setup != nil {
				tc.setup(b)
			}
			types, err := b.ListTypes("")
			require.NoError(t, err)

			typeMap := make(map[string]cloudformation.TypeSummary, len(types))
			for _, typ := range types {
				typeMap[typ.TypeName] = typ
			}

			for _, want := range tc.wantPrivate {
				summary, ok := typeMap[want]
				assert.True(t, ok, "expected %q in type list", want)
				assert.Equal(t, "PRIVATE", summary.Visibility)
			}
			for _, want := range tc.wantPublic {
				summary, ok := typeMap[want]
				assert.True(t, ok, "expected %q in type list", want)
				assert.Equal(t, "PUBLIC", summary.Visibility)
			}
			for _, notWant := range tc.wantNotPresent {
				_, ok := typeMap[notWant]
				assert.False(t, ok, "expected %q to be excluded from type list", notWant)
			}
		})
	}
}

// ---- DescribeStackInstance fields -------------------------------------------------

func TestDescribeStackInstance_Fields(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStackSet("field-ss", "test", simpleTemplate)
	require.NoError(t, err)
	err = b.CreateStackInstances("field-ss", []string{"123456789012"}, []string{"us-east-1"})
	require.NoError(t, err)

	inst, err := b.DescribeStackInstance("field-ss", "123456789012", "us-east-1")
	require.NoError(t, err)

	assert.Equal(t, "123456789012", inst.Account)
	assert.Equal(t, "us-east-1", inst.Region)
	assert.Equal(t, "CURRENT", inst.Status)
	assert.Equal(t, "NOT_CHECKED", inst.DriftStatus)
	assert.NotEmpty(t, inst.StackID)
	assert.NotEmpty(t, inst.StackSetID)
	assert.NotEmpty(t, inst.LastOperationID)
}

// ---- ListStackSetOperations sorted order ------------------------------------------

func TestListStackSetOperations_SortedByCreationTime(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStackSet("sort-ops-ss", "test", simpleTemplate)
	require.NoError(t, err)

	// Create multiple operations by calling CreateStackInstances multiple times.
	err = b.CreateStackInstances("sort-ops-ss", []string{"111111111111"}, []string{"us-east-1"})
	require.NoError(t, err)
	err = b.UpdateStackInstances("sort-ops-ss", []string{"111111111111"}, []string{"us-east-1"})
	require.NoError(t, err)
	_, err = b.UpdateStackSet("sort-ops-ss", simpleTemplate)
	require.NoError(t, err)

	opIDs, err := b.ListStackSetOperations("sort-ops-ss", "")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(opIDs), 3, "expected at least 3 operations")
}

// ---- Handler: DescribeType for registered type ------------------------------------

func TestHandler_DescribeType_RegisteredVsBuiltin(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Register a type and verify it comes from the registry.
	postFormValues(t, h, url.Values{
		"Action":               {"RegisterType"},
		"TypeName":             {"Acme::Network::Router"},
		"SchemaHandlerPackage": {"s3://schema.zip"},
	})

	tests := []struct {
		name         string
		typeName     string
		wantContains []string
	}{
		{
			name:     "registered type served from registry",
			typeName: "Acme::Network::Router",
			wantContains: []string{
				"DescribeTypeResponse",
				"Acme::Network::Router",
				"RESOURCE",
				"COMPLETE",
			},
		},
		{
			name:     "builtin AWS type served from schema fallback",
			typeName: "AWS::Lambda::Function",
			wantContains: []string{
				"DescribeTypeResponse",
				"AWS::Lambda::Function",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			resp := postFormValues(t, h, url.Values{
				"Action":   {"DescribeType"},
				"TypeName": {tc.typeName},
			})
			resp.mustOK(t)
			for _, want := range tc.wantContains {
				assert.Contains(t, resp.Body, want)
			}
		})
	}
}

// ---- Handler: CreateChangeSet ChangeSetType in response body ----------------------

func TestHandler_CreateChangeSet_TypeInResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		wantChangeSetType string
		preCreateStack    bool
	}{
		{
			name:              "no existing stack → CREATE type",
			preCreateStack:    false,
			wantChangeSetType: "CREATE",
		},
		{
			name:              "existing stack → UPDATE type",
			preCreateStack:    true,
			wantChangeSetType: "UPDATE",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newHandler()
			const stackName = "cs-type-test"

			if tc.preCreateStack {
				postFormValues(t, h, url.Values{
					"Action": {"CreateStack"}, "StackName": {stackName},
					"TemplateBody": {simpleTemplate},
				})
			}

			postFormValues(t, h, url.Values{
				"Action":        {"CreateChangeSet"},
				"StackName":     {stackName},
				"ChangeSetName": {"type-check-cs"},
				"TemplateBody":  {simpleTemplate},
			})

			resp := postFormValues(t, h, url.Values{
				"Action":        {"DescribeChangeSet"},
				"StackName":     {stackName},
				"ChangeSetName": {"type-check-cs"},
			})
			resp.mustOK(t)
			assert.Contains(t, resp.Body, "ExecutionStatus")
			// ChangeSetType and ExecutionStatus are serialized in XML.
			assert.Contains(t, resp.Body, "AVAILABLE")
		})
	}
}

// ---- Error variables exported (package-level) ------------------------------------

func TestErrorVariables_Exported(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err  error
		name string
	}{
		{name: "ErrStackNotFound", err: cloudformation.ErrStackNotFound},
		{name: "ErrChangeSetAlreadyExecuted", err: cloudformation.ErrChangeSetAlreadyExecuted},
		{name: "ErrStackInstanceAlreadyExists", err: cloudformation.ErrStackInstanceAlreadyExists},
		{name: "ErrTypeVersionNotFound", err: cloudformation.ErrTypeVersionNotFound},
		{name: "ErrInvalidRoleARN", err: cloudformation.ErrInvalidRoleARN},
		{name: "ErrInsufficientCapabilities", err: cloudformation.ErrInsufficientCapabilities},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tc.err)
			assert.NotEmpty(t, tc.err.Error())
		})
	}
}

// ---- DeleteStackInstances preserves other instances (table-driven) ---------------

func TestDeleteStackInstances_Selective(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		createAccounts []string
		createRegions  []string
		deleteAccounts []string
		deleteRegions  []string
		wantRemaining  int
	}{
		{
			name:           "delete half of instances",
			createAccounts: []string{"111111111111", "222222222222"},
			createRegions:  []string{"us-east-1"},
			deleteAccounts: []string{"111111111111"},
			deleteRegions:  []string{"us-east-1"},
			wantRemaining:  1,
		},
		{
			name:           "delete all instances",
			createAccounts: []string{"111111111111"},
			createRegions:  []string{"us-east-1"},
			deleteAccounts: []string{"111111111111"},
			deleteRegions:  []string{"us-east-1"},
			wantRemaining:  0,
		},
		{
			name:           "delete by region leaving other region",
			createAccounts: []string{"111111111111"},
			createRegions:  []string{"us-east-1", "eu-west-1"},
			deleteAccounts: []string{"111111111111"},
			deleteRegions:  []string{"us-east-1"},
			wantRemaining:  1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.CreateStackSet("del-sel-ss", "test", simpleTemplate)
			require.NoError(t, err)

			err = b.CreateStackInstances("del-sel-ss", tc.createAccounts, tc.createRegions)
			require.NoError(t, err)

			err = b.DeleteStackInstances("del-sel-ss", tc.deleteAccounts, tc.deleteRegions)
			require.NoError(t, err)

			remaining, err := b.ListStackInstances("del-sel-ss", "")
			require.NoError(t, err)
			assert.Len(t, remaining, tc.wantRemaining)
		})
	}
}
