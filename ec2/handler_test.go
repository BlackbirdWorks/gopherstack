package ec2_test

import (
	"encoding/xml"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/ec2"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// newHandler creates a new EC2 handler with a fresh backend.
func newHandler() *ec2.Handler {
	bk := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(bk, slog.Default())
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
		wantCode     int
		wantContains []string
	}{
		{
			name:     "RunInstances",
			body:     "Action=RunInstances&Version=2016-11-15&ImageId=ami-12345&InstanceType=t2.micro&MinCount=1&MaxCount=1",
			wantCode: http.StatusOK,
			wantContains: []string{
				"RunInstancesResponse",
				"<instanceId>i-",
				"running",
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
			name:     "CreateSubnet",
			body:     "Action=CreateSubnet&Version=2016-11-15&VpcId=vpc-default&CidrBlock=10.0.1.0/24&AvailabilityZone=us-east-1b",
			wantCode: http.StatusOK,
			wantContains: []string{
				"CreateSubnetResponse",
				"10.0.1.0/24",
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
			name:         "CreateSecurityGroup_InvalidVPC_Handler",
			body:         "Action=CreateSecurityGroup&Version=2016-11-15&GroupName=sg-name&GroupDescription=test&VpcId=vpc-nonexistent",
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
			name:     "URLEncodedCIDR",
			body:     "Action=CreateVpc&Version=2016-11-15&CidrBlock=10.0.0.0%2F16",
			wantCode: http.StatusOK,
			wantContains: []string{"10.0.0.0/16"},
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
		fmt.Sprintf("Action=DescribeInstances&Version=2016-11-15&InstanceId.1=%s", instanceID))
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

	// Terminate.
	termRec := postForm(t, h,
		fmt.Sprintf("Action=TerminateInstances&Version=2016-11-15&InstanceId.1=%s", instanceID))
	assert.Equal(t, http.StatusOK, termRec.Code)
	assert.Contains(t, termRec.Body.String(), "TerminateInstancesResponse")
	assert.Contains(t, termRec.Body.String(), "terminated")
}

func TestEC2Handler_SecurityGroupCRUD(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create security group.
	createRec := postForm(t, h,
		"Action=CreateSecurityGroup&Version=2016-11-15&GroupName=my-sg&GroupDescription=test+sg&VpcId=vpc-default")
	assert.Equal(t, http.StatusOK, createRec.Code)
	assert.Contains(t, createRec.Body.String(), "CreateSecurityGroupResponse")
	assert.Contains(t, createRec.Body.String(), "<groupId>sg-")

	// Extract group ID.
	var createResp struct {
		GroupID string `xml:"groupId"`
	}

	err := xml.Unmarshal([]byte(strings.TrimPrefix(createRec.Body.String(), xml.Header)), &createResp)
	require.NoError(t, err)
	groupID := createResp.GroupID
	require.NotEmpty(t, groupID)

	// Describe security groups.
	descRec := postForm(t, h, "Action=DescribeSecurityGroups&Version=2016-11-15")
	assert.Equal(t, http.StatusOK, descRec.Code)
	assert.Contains(t, descRec.Body.String(), "my-sg")

	// Delete security group.
	delRec := postForm(t, h,
		fmt.Sprintf("Action=DeleteSecurityGroup&Version=2016-11-15&GroupId=%s", groupID))
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
}

func TestEC2Handler_NameAndOperations(t *testing.T) {
	t.Parallel()

	h := newHandler()
	assert.Equal(t, "EC2", h.Name())

	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "RunInstances")
	assert.Contains(t, ops, "DescribeInstances")
	assert.Contains(t, ops, "TerminateInstances")
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

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=DescribeInstances&Version=2016-11-15"))
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

	// Describe running instances.
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
