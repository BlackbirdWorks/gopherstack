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

func TestEC2_Backend_Defaults(t *testing.T) {
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

func TestEC2_RunInstances(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h,
		"Action=RunInstances&Version=2016-11-15&ImageId=ami-12345&InstanceType=t2.micro&MinCount=1&MaxCount=1")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "RunInstancesResponse")
	assert.Contains(t, rec.Body.String(), "<instanceId>i-")
	assert.Contains(t, rec.Body.String(), "running")
}

func TestEC2_RunInstances_MissingImageID(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Action=RunInstances&Version=2016-11-15&MinCount=1")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
}

func TestEC2_DescribeInstances(t *testing.T) {
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

func TestEC2_TerminateInstances(t *testing.T) {
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

func TestEC2_TerminateInstances_NotFound(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Action=TerminateInstances&Version=2016-11-15&InstanceId.1=i-nonexistent")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidInstanceID.NotFound")
}

func TestEC2_SecurityGroup_CRUD(t *testing.T) {
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

func TestEC2_SecurityGroup_DeleteNotFound(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Action=DeleteSecurityGroup&Version=2016-11-15&GroupId=sg-nonexistent")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidGroup.NotFound")
}

func TestEC2_VPC_Describe(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Action=DescribeVpcs&Version=2016-11-15")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeVpcsResponse")
	assert.Contains(t, rec.Body.String(), "vpc-default")
}

func TestEC2_CreateVpc(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Action=CreateVpc&Version=2016-11-15&CidrBlock=10.0.0.0/16")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreateVpcResponse")
	assert.Contains(t, rec.Body.String(), "10.0.0.0/16")
	assert.Contains(t, rec.Body.String(), "<vpcId>vpc-")
}

func TestEC2_CreateVpc_MissingCIDR(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Action=CreateVpc&Version=2016-11-15")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
}

func TestEC2_Subnet_Describe(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Action=DescribeSubnets&Version=2016-11-15")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeSubnetsResponse")
	assert.Contains(t, rec.Body.String(), "subnet-default")
}

func TestEC2_CreateSubnet(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h,
		"Action=CreateSubnet&Version=2016-11-15&VpcId=vpc-default&CidrBlock=10.0.1.0/24&AvailabilityZone=us-east-1b")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreateSubnetResponse")
	assert.Contains(t, rec.Body.String(), "10.0.1.0/24")
	assert.Contains(t, rec.Body.String(), "us-east-1b")
}

func TestEC2_CreateSubnet_VPCNotFound(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h,
		"Action=CreateSubnet&Version=2016-11-15&VpcId=vpc-nonexistent&CidrBlock=10.0.1.0/24")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidVpcID.NotFound")
}

func TestEC2_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Action=UnknownAction&Version=2016-11-15")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
}

func TestEC2_MissingAction(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Version=2016-11-15&ImageId=ami-test")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "MissingAction")
}

func TestEC2_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newHandler()
	matcher := h.RouteMatcher()

	e := echo.New()

	t.Run("matches EC2 POST", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Version=2016-11-15&Action=DescribeInstances"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		c := e.NewContext(req, httptest.NewRecorder())
		assert.True(t, matcher(c))
	})

	t.Run("does not match GET", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		c := e.NewContext(req, httptest.NewRecorder())
		assert.False(t, matcher(c))
	})

	t.Run("does not match dashboard path", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodPost, "/dashboard/ec2", strings.NewReader("Version=2016-11-15"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		c := e.NewContext(req, httptest.NewRecorder())
		assert.False(t, matcher(c))
	})

	t.Run("does not match wrong version", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Version=2010-12-01&Action=SendEmail"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		c := e.NewContext(req, httptest.NewRecorder())
		assert.False(t, matcher(c))
	})
}

func TestEC2_Handler_GetMethod(t *testing.T) {
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

func TestEC2_Provider_Name(t *testing.T) {
	t.Parallel()

	p := &ec2.Provider{}
	assert.Equal(t, "EC2", p.Name())
}

func TestEC2_Provider_Init(t *testing.T) {
	t.Parallel()

	p := &ec2.Provider{}
	appCtx := &service.AppContext{Logger: slog.Default()}

	reg, err := p.Init(appCtx)
	require.NoError(t, err)
	require.NotNil(t, reg)
	assert.Equal(t, "EC2", reg.Name())
}

func TestEC2_Name_And_Operations(t *testing.T) {
	t.Parallel()

	h := newHandler()
	assert.Equal(t, "EC2", h.Name())

	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "RunInstances")
	assert.Contains(t, ops, "DescribeInstances")
	assert.Contains(t, ops, "TerminateInstances")
}

func TestEC2_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newHandler()
	assert.Equal(t, 80, h.MatchPriority())
}

func TestEC2_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newHandler()
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=DescribeInstances&Version=2016-11-15"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	c := e.NewContext(req, httptest.NewRecorder())

	assert.Equal(t, "DescribeInstances", h.ExtractOperation(c))
}

func TestEC2_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newHandler()
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/",
		strings.NewReader("Action=TerminateInstances&InstanceId.1=i-abc123&Version=2016-11-15"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	c := e.NewContext(req, httptest.NewRecorder())

	assert.Equal(t, "i-abc123", h.ExtractResource(c))
}

func TestEC2_TerminateInstances_MissingID(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Action=TerminateInstances&Version=2016-11-15")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
}

func TestEC2_CreateSecurityGroup_MissingName(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Action=CreateSecurityGroup&Version=2016-11-15&GroupDescription=test")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
}

func TestEC2_DeleteSecurityGroup_MissingGroupID(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Action=DeleteSecurityGroup&Version=2016-11-15")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
}

func TestEC2_CreateSubnet_MissingVPC(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Action=CreateSubnet&Version=2016-11-15&CidrBlock=10.0.1.0/24")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
}

func TestEC2_CreateSubnet_MissingCIDR(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Action=CreateSubnet&Version=2016-11-15&VpcId=vpc-default")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
}

func TestEC2_DescribeInstances_FilterByState(t *testing.T) {
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

func TestEC2_DuplicateSecurityGroup(t *testing.T) {
	t.Parallel()

	bk := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := bk.CreateSecurityGroup("my-sg", "test", "vpc-default")
	require.NoError(t, err)

	_, err = bk.CreateSecurityGroup("my-sg", "test", "vpc-default")
	require.ErrorIs(t, err, ec2.ErrDuplicateSGName)
}

func TestEC2_DescribeSecurityGroups_ByID(t *testing.T) {
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
