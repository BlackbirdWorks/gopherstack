package ec2_test

import (
	"context"
	"encoding/xml"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// Compile-time assertion: InMemoryBackend must satisfy the Backend interface.
// Any future backend implementation must satisfy this same interface.
var _ ec2.Backend = (*ec2.InMemoryBackend)(nil)

// newTestBackend creates a fresh backend for testing.
func newTestBackend() *ec2.InMemoryBackend {
	return ec2.NewInMemoryBackend("000000000000", "us-east-1")
}

// newHandler creates a new EC2 handler with a fresh backend.
func newHandler() *ec2.Handler {
	bk := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(bk)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	return h
}

// postForm sends a form-encoded POST to the EC2 handler.
func postForm(t *testing.T, h *ec2.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestEC2Handler_PostForm(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:     "RunInstances",
			body:     "Action=RunInstances&Version=2016-11-15&ImageId=ami-12345&InstanceType=t2.micro&MinCount=1&MaxCount=1",
			wantCode: http.StatusOK,
			wantContains: []string{
				"RunInstancesResponse",
				"<instanceId>i-",
				"pending",
			},
		},
		{
			name:         "RunInstances_MissingImageID",
			body:         "Action=RunInstances&Version=2016-11-15&MinCount=1",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "TerminateInstances_NotFound",
			body:         "Action=TerminateInstances&Version=2016-11-15&InstanceId.1=i-nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidInstanceID.NotFound"},
		},
		{
			name:         "SecurityGroup_DeleteNotFound",
			body:         "Action=DeleteSecurityGroup&Version=2016-11-15&GroupId=sg-nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidGroup.NotFound"},
		},
		{
			name:     "VPC_Describe",
			body:     "Action=DescribeVpcs&Version=2016-11-15",
			wantCode: http.StatusOK,
			wantContains: []string{
				"DescribeVpcsResponse",
				"vpc-default",
			},
		},
		{
			name:     "CreateVpc",
			body:     "Action=CreateVpc&Version=2016-11-15&CidrBlock=10.0.0.0/16",
			wantCode: http.StatusOK,
			wantContains: []string{
				"CreateVpcResponse",
				"10.0.0.0/16",
				"<vpcId>vpc-",
			},
		},
		{
			name:         "CreateVpc_MissingCIDR",
			body:         "Action=CreateVpc&Version=2016-11-15",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:     "Subnet_Describe",
			body:     "Action=DescribeSubnets&Version=2016-11-15",
			wantCode: http.StatusOK,
			wantContains: []string{
				"DescribeSubnetsResponse",
				"subnet-default",
			},
		},
		{
			name: "CreateSubnet",
			body: "Action=CreateSubnet&Version=2016-11-15&VpcId=vpc-default&" +
				"CidrBlock=172.31.16.0/24&AvailabilityZone=us-east-1b",
			wantCode: http.StatusOK,
			wantContains: []string{
				"CreateSubnetResponse",
				"172.31.16.0/24",
				"us-east-1b",
			},
		},
		{
			name:         "CreateSubnet_VPCNotFound",
			body:         "Action=CreateSubnet&Version=2016-11-15&VpcId=vpc-nonexistent&CidrBlock=10.0.1.0/24",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidVpcID.NotFound"},
		},
		{
			name:         "UnknownAction",
			body:         "Action=UnknownAction&Version=2016-11-15",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "MissingAction",
			body:         "Version=2016-11-15&ImageId=ami-test",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"MissingAction"},
		},
		{
			name:         "TerminateInstances_MissingID",
			body:         "Action=TerminateInstances&Version=2016-11-15",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "CreateSecurityGroup_MissingName",
			body:         "Action=CreateSecurityGroup&Version=2016-11-15&GroupDescription=test",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "DeleteSecurityGroup_MissingGroupID",
			body:         "Action=DeleteSecurityGroup&Version=2016-11-15",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "CreateSubnet_MissingVPC",
			body:         "Action=CreateSubnet&Version=2016-11-15&CidrBlock=10.0.1.0/24",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "CreateSubnet_MissingCIDR",
			body:         "Action=CreateSubnet&Version=2016-11-15&VpcId=vpc-default",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "CreateSecurityGroup_InvalidVPC_Handler",
			body: "Action=CreateSecurityGroup&Version=2016-11-15&GroupName=sg-name&" +
				"GroupDescription=test&VpcId=vpc-nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidVpcID.NotFound"},
		},
		{
			name:         "RunInstances_InvalidSubnet_Handler",
			body:         "Action=RunInstances&Version=2016-11-15&ImageId=ami-test&SubnetId=subnet-nonexistent&MinCount=1",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidSubnetID.NotFound"},
		},
		{
			name:         "URLEncodedCIDR",
			body:         "Action=CreateVpc&Version=2016-11-15&CidrBlock=10.0.0.0%2F16",
			wantCode:     http.StatusOK,
			wantContains: []string{"10.0.0.0/16"},
		},
		{
			name:         "DescribeLaunchTemplates_empty",
			body:         "Action=DescribeLaunchTemplates&Version=2016-11-15",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeLaunchTemplatesResponse"},
		},
		{
			name: "DescribeInstanceTypes_with_filter",
			body: "Action=DescribeInstanceTypes&Version=2016-11-15" +
				"&Filter.1.Name=instance-type&Filter.1.Value.1=t3.micro",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeInstanceTypesResponse", "t3.micro"},
		},
		{
			name:         "DescribeInstanceTypes_with_type_param",
			body:         "Action=DescribeInstanceTypes&Version=2016-11-15&InstanceType.1=t2.small",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeInstanceTypesResponse", "t2.small"},
		},
		{
			name:         "DescribeInstanceTypes_default",
			body:         "Action=DescribeInstanceTypes&Version=2016-11-15",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeInstanceTypesResponse", "t2.micro"},
		},
		{
			name:     "DescribeVpcAttribute",
			body:     "Action=DescribeVpcAttribute&Version=2016-11-15&VpcId=vpc-default&Attribute=enableDnsHostnames",
			wantCode: http.StatusOK,
			wantContains: []string{
				"DescribeVpcAttributeResponse",
				"vpc-default",
				"enableDnsHostnames",
			},
		},
		{
			name:         "RevokeSecurityGroupEgress",
			body:         "Action=RevokeSecurityGroupEgress&Version=2016-11-15&GroupId=sg-default",
			wantCode:     http.StatusOK,
			wantContains: []string{"RevokeSecurityGroupEgressResponse", "true"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestEC2Handler_DescribeInstances(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Run an instance first.
	runRec := postForm(t, h,
		"Action=RunInstances&Version=2016-11-15&ImageId=ami-test&InstanceType=t2.micro&MinCount=1")
	require.Equal(t, http.StatusOK, runRec.Code)

	// Extract instance ID from response.
	var runResp struct {
		InstancesSet struct {
			Items []struct {
				InstanceID string `xml:"instanceId"`
			} `xml:"item"`
		} `xml:"instancesSet"`
	}

	err := xml.Unmarshal([]byte(strings.TrimPrefix(runRec.Body.String(), xml.Header)), &runResp)
	require.NoError(t, err)
	require.Len(t, runResp.InstancesSet.Items, 1)

	instanceID := runResp.InstancesSet.Items[0].InstanceID
	require.NotEmpty(t, instanceID)

	// Describe the instance.
	descRec := postForm(t, h,
		"Action=DescribeInstances&Version=2016-11-15&InstanceId.1="+instanceID)
	assert.Equal(t, http.StatusOK, descRec.Code)
	assert.Contains(t, descRec.Body.String(), instanceID)
}

func TestEC2Handler_TerminateInstances(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Run instance.
	runRec := postForm(t, h,
		"Action=RunInstances&Version=2016-11-15&ImageId=ami-test&InstanceType=t3.small&MinCount=1")
	require.Equal(t, http.StatusOK, runRec.Code)

	var runResp struct {
		InstancesSet struct {
			Items []struct {
				InstanceID string `xml:"instanceId"`
			} `xml:"item"`
		} `xml:"instancesSet"`
	}

	err := xml.Unmarshal([]byte(strings.TrimPrefix(runRec.Body.String(), xml.Header)), &runResp)
	require.NoError(t, err)
	instanceID := runResp.InstancesSet.Items[0].InstanceID

	// Terminate — AWS state machine returns shutting-down as CurrentState immediately.
	termRec := postForm(t, h,
		"Action=TerminateInstances&Version=2016-11-15&InstanceId.1="+instanceID)
	assert.Equal(t, http.StatusOK, termRec.Code)
	assert.Contains(t, termRec.Body.String(), "TerminateInstancesResponse")
	assert.Contains(t, termRec.Body.String(), "shutting-down")
}

func TestEC2Handler_SecurityGroupCRUD(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create security group.
	createRec := postForm(
		t,
		h,
		"Action=CreateSecurityGroup&Version=2016-11-15&GroupName=my-sg&GroupDescription=test+sg&VpcId=vpc-default",
	)
	assert.Equal(t, http.StatusOK, createRec.Code)
	assert.Contains(t, createRec.Body.String(), "CreateSecurityGroupResponse")
	assert.Contains(t, createRec.Body.String(), "<groupId>sg-")

	// Extract group ID.
	var createResp struct {
		GroupID string `xml:"groupId"`
	}

	err := xml.Unmarshal(
		[]byte(strings.TrimPrefix(createRec.Body.String(), xml.Header)),
		&createResp,
	)
	require.NoError(t, err)
	groupID := createResp.GroupID
	require.NotEmpty(t, groupID)

	// Describe security groups.
	descRec := postForm(t, h, "Action=DescribeSecurityGroups&Version=2016-11-15")
	assert.Equal(t, http.StatusOK, descRec.Code)
	assert.Contains(t, descRec.Body.String(), "my-sg")

	// Delete security group.
	delRec := postForm(t, h,
		"Action=DeleteSecurityGroup&Version=2016-11-15&GroupId="+groupID)
	assert.Equal(t, http.StatusOK, delRec.Code)
	assert.Contains(t, delRec.Body.String(), "DeleteSecurityGroupResponse")
}

func TestEC2Handler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newHandler()
	matcher := h.RouteMatcher()
	e := echo.New()

	tests := []struct {
		name        string
		method      string
		path        string
		body        string
		contentType string
		wantMatch   bool
	}{
		{
			name:        "matches EC2 POST",
			method:      http.MethodPost,
			path:        "/",
			body:        "Version=2016-11-15&Action=DescribeInstances",
			contentType: "application/x-www-form-urlencoded",
			wantMatch:   true,
		},
		{
			name:      "does not match GET",
			method:    http.MethodGet,
			path:      "/",
			wantMatch: false,
		},
		{
			name:        "does not match dashboard path",
			method:      http.MethodPost,
			path:        "/dashboard/ec2",
			body:        "Version=2016-11-15",
			contentType: "application/x-www-form-urlencoded",
			wantMatch:   false,
		},
		{
			name:        "does not match wrong version",
			method:      http.MethodPost,
			path:        "/",
			body:        "Version=2010-12-01&Action=SendEmail",
			contentType: "application/x-www-form-urlencoded",
			wantMatch:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var req *http.Request
			if tt.body != "" {
				req = httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			} else {
				req = httptest.NewRequest(tt.method, tt.path, nil)
			}

			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			}

			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
}

func TestEC2Handler_GetMethod(t *testing.T) {
	t.Parallel()

	h := newHandler()

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	// GET with no body should return MissingAction error.
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestEC2Provider_Name(t *testing.T) {
	t.Parallel()

	p := &ec2.Provider{}
	assert.Equal(t, "EC2", p.Name())
}

func TestEC2Provider_Init(t *testing.T) {
	t.Parallel()

	p := &ec2.Provider{}
	appCtx := &service.AppContext{Logger: slog.Default()}

	reg, err := p.Init(appCtx)
	require.NoError(t, err)
	require.NotNil(t, reg)
	assert.Equal(t, "EC2", reg.Name())
	t.Cleanup(func() { reg.(*ec2.Handler).Shutdown(context.Background()) })
}

func TestEC2Handler_NameAndOperations(t *testing.T) {
	t.Parallel()

	h := newHandler()
	assert.Equal(t, "EC2", h.Name())

	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "RunInstances")
	assert.Contains(t, ops, "DescribeInstances")
	assert.Contains(t, ops, "TerminateInstances")
	assert.Contains(t, ops, "CreateLaunchTemplate")
	assert.Contains(t, ops, "DescribeVpcEndpoints")
	assert.Contains(t, ops, "DescribeNetworkAcls")
}

func TestEC2Handler_DeepDiveOperations(t *testing.T) {
	t.Parallel()

	h := newHandler()

	runRec := postForm(t, h,
		"Action=RunInstances&Version=2016-11-15&ImageId=ami-test&InstanceType=t2.micro&MinCount=1")
	require.Equal(t, http.StatusOK, runRec.Code)

	var runResp struct {
		InstancesSet struct {
			Items []struct {
				InstanceID string `xml:"instanceId"`
			} `xml:"item"`
		} `xml:"instancesSet"`
	}

	err := xml.Unmarshal([]byte(strings.TrimPrefix(runRec.Body.String(), xml.Header)), &runResp)
	require.NoError(t, err)
	require.Len(t, runResp.InstancesSet.Items, 1)
	instanceID := runResp.InstancesSet.Items[0].InstanceID

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "CreateImage",
			body: "Action=CreateImage&Version=2016-11-15&InstanceId=" + instanceID +
				"&Name=test-image&Description=test",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateImageResponse", "<imageId>ami-"},
		},
		{
			name:         "DescribeImageUsageReports",
			body:         "Action=DescribeImageUsageReports&Version=2016-11-15",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeImageUsageReportsResponse", "<imageUsageReportSet>"},
		},
		{
			name: "CreateLaunchTemplate",
			body: "Action=CreateLaunchTemplate&Version=2016-11-15&LaunchTemplateName=test-lt" +
				"&LaunchTemplateData.ImageId=ami-test&LaunchTemplateData.InstanceType=t3.micro",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateLaunchTemplateResponse", "test-lt"},
		},
		{
			name:         "DescribeLaunchTemplates",
			body:         "Action=DescribeLaunchTemplates&Version=2016-11-15",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeLaunchTemplatesResponse"},
		},
		{
			name: "CreateVpcEndpoint",
			body: "Action=CreateVpcEndpoint&Version=2016-11-15&VpcId=vpc-default&ServiceName=com.amazonaws.us-east-1.s3" +
				"&VpcEndpointType=Interface&SubnetId.1=subnet-default",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateVpcEndpointResponse", "<vpcEndpointId>vpce-"},
		},
		{
			name:         "DescribeVpcEndpoints",
			body:         "Action=DescribeVpcEndpoints&Version=2016-11-15",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeVpcEndpointsResponse"},
		},
		{
			name:         "DescribeNetworkAcls",
			body:         "Action=DescribeNetworkAcls&Version=2016-11-15&Filter.1.Name=vpc-id&Filter.1.Value.1=vpc-default",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeNetworkAclsResponse", "acl-default-vpc-default"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, expected := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), expected)
			}
		})
	}
}

func TestEC2Handler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newHandler()
	assert.Equal(t, 80, h.MatchPriority())
}

func TestEC2Handler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newHandler()
	e := echo.New()

	req := httptest.NewRequest(
		http.MethodPost,
		"/",
		strings.NewReader("Action=DescribeInstances&Version=2016-11-15"),
	)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	c := e.NewContext(req, httptest.NewRecorder())

	assert.Equal(t, "DescribeInstances", h.ExtractOperation(c))
}

func TestEC2Handler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantResource string
	}{
		{
			name:         "InstanceId",
			body:         "Action=TerminateInstances&InstanceId.1=i-abc123&Version=2016-11-15",
			wantResource: "i-abc123",
		},
		{
			name:         "GroupId",
			body:         "Action=DeleteSecurityGroup&GroupId=sg-abc123&Version=2016-11-15",
			wantResource: "sg-abc123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			e := echo.New()

			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantResource, h.ExtractResource(c))
		})
	}
}

func TestInMemoryBackend_Defaults(t *testing.T) {
	t.Parallel()

	bk := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	vpcs := bk.DescribeVpcs(nil)
	require.Len(t, vpcs, 1)
	assert.Equal(t, "vpc-default", vpcs[0].ID)
	assert.True(t, vpcs[0].IsDefault)

	subnets := bk.DescribeSubnets(nil)
	require.Len(t, subnets, 1)
	assert.Equal(t, "subnet-default", subnets[0].ID)

	sgs := bk.DescribeSecurityGroups(nil)
	require.Len(t, sgs, 1)
	assert.Equal(t, "sg-default", sgs[0].ID)
}

func TestInMemoryBackend_DescribeInstances_FilterByState(t *testing.T) {
	t.Parallel()

	bk := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	// Run instance.
	instances, err := bk.RunInstances("ami-test", "t2.micro", "", 1)
	require.NoError(t, err)
	require.Len(t, instances, 1)

	// Describe running instances (tick lifecycle so pending → running).
	bk.TickLifecycleForTest()
	running := bk.DescribeInstances(nil, "running")
	assert.Len(t, running, 1)

	// Describe terminated (should be empty).
	terminated := bk.DescribeInstances(nil, "terminated")
	assert.Empty(t, terminated)
}

func TestInMemoryBackend_DuplicateSecurityGroup(t *testing.T) {
	t.Parallel()

	bk := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := bk.CreateSecurityGroup("my-sg", "test", "vpc-default")
	require.NoError(t, err)

	_, err = bk.CreateSecurityGroup("my-sg", "test", "vpc-default")
	require.ErrorIs(t, err, ec2.ErrDuplicateSGName)
}

func TestInMemoryBackend_DescribeSecurityGroupsByID(t *testing.T) {
	t.Parallel()

	bk := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	sg, err := bk.CreateSecurityGroup("test-sg", "test", "vpc-default")
	require.NoError(t, err)

	// Describe by ID.
	sgs := bk.DescribeSecurityGroups([]string{sg.ID})
	require.Len(t, sgs, 1)
	assert.Equal(t, sg.ID, sgs[0].ID)

	// Non-existent ID.
	sgs = bk.DescribeSecurityGroups([]string{"sg-nonexistent"})
	assert.Empty(t, sgs)
}

func TestInMemoryBackend_RunInstances_InvalidSubnet(t *testing.T) {
	t.Parallel()

	bk := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := bk.RunInstances("ami-test", "t2.micro", "subnet-nonexistent", 1)
	require.ErrorIs(t, err, ec2.ErrSubnetNotFound)
}

func TestInMemoryBackend_CreateSecurityGroup_InvalidVPC(t *testing.T) {
	t.Parallel()

	bk := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := bk.CreateSecurityGroup("sg-name", "test", "vpc-nonexistent")
	require.ErrorIs(t, err, ec2.ErrVPCNotFound)
}

func extractXMLValue(t *testing.T, body, elementName string) string {
	t.Helper()

	open := "<" + elementName + ">"
	closeTag := "</" + elementName + ">"
	start := strings.Index(body, open)

	if start == -1 {
		return ""
	}

	start += len(open)
	end := strings.Index(body[start:], closeTag)

	if end == -1 {
		return ""
	}

	return body[start : start+end]
}

// extractTagsFromDescribeTagsXML parses a DescribeTagsResponse XML body and returns a key→value map.
// The test fails immediately via require.NoError if the XML is malformed.
// Returns an empty map if the response contains no tag items.
func extractTagsFromDescribeTagsXML(t *testing.T, body string) map[string]string {
	t.Helper()

	type itemEl struct {
		Key   string `xml:"key"`
		Value string `xml:"value"`
	}
	type tagSet struct {
		Items []itemEl `xml:"item"`
	}
	type resp struct {
		TagSet tagSet `xml:"tagSet"`
	}

	var r resp

	require.NoError(t, xml.Unmarshal([]byte(body), &r), "DescribeTagsResponse should be valid XML")

	m := make(map[string]string, len(r.TagSet.Items))
	for _, item := range r.TagSet.Items {
		m[item.Key] = item.Value
	}

	return m
}

func TestEC2Handler_DescribeInstanceAttribute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args struct {
			attr string
		}
		want struct {
			bodyContains string
		}
	}{
		{
			name: "shutdown_behavior_returns_stop",
			args: struct{ attr string }{attr: "instanceInitiatedShutdownBehavior"},
			want: struct{ bodyContains string }{bodyContains: "stop"},
		},
		{
			name: "disable_api_termination_returns_false",
			args: struct{ attr string }{attr: "disableApiTermination"},
			want: struct{ bodyContains string }{bodyContains: "false"},
		},
		{
			// AWS defaults sourceDestCheck to true for VPC instances (it must
			// be explicitly disabled, e.g. for NAT instances); the handler
			// previously hardcoded "false" regardless of state, which this
			// test encoded as correct. Fixed to read the primary ENI's real
			// sourceDestCheck flag (defaulting to true).
			name: "source_dest_check_returns_true",
			args: struct{ attr string }{attr: "sourceDestCheck"},
			want: struct{ bodyContains string }{bodyContains: "true"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			// Create an instance first to get a real instance ID.
			createRec := postForm(
				t,
				h,
				"Action=RunInstances&Version=2016-11-15&ImageId=ami-test&InstanceType=t2.micro&MinCount=1&MaxCount=1",
			)
			require.Equal(t, http.StatusOK, createRec.Code)

			instanceID := extractXMLValue(t, createRec.Body.String(), "instanceId")
			require.NotEmpty(t, instanceID)

			rec := postForm(t, h,
				"Action=DescribeInstanceAttribute&Version=2016-11-15"+
					"&InstanceId="+instanceID+"&Attribute="+tt.args.attr)

			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), "DescribeInstanceAttributeResponse")
			assert.Contains(t, rec.Body.String(), tt.want.bodyContains)
		})
	}
}
