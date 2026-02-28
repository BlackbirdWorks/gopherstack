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

func TestEC2Handler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "Backend_Defaults",
			run: func(t *testing.T) {
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
			},
		},
		{
			name: "RunInstances",
			run: func(t *testing.T) {
				h := newHandler()
				rec := postForm(t, h,
					"Action=RunInstances&Version=2016-11-15&ImageId=ami-12345&InstanceType=t2.micro&MinCount=1&MaxCount=1")

				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "RunInstancesResponse")
				assert.Contains(t, rec.Body.String(), "<instanceId>i-")
				assert.Contains(t, rec.Body.String(), "running")
			},
		},
		{
			name: "RunInstances_MissingImageID",
			run: func(t *testing.T) {
				h := newHandler()
				rec := postForm(t, h, "Action=RunInstances&Version=2016-11-15&MinCount=1")

				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
			},
		},
		{
			name: "DescribeInstances",
			run: func(t *testing.T) {
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
			},
		},
		{
			name: "TerminateInstances",
			run: func(t *testing.T) {
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
			},
		},
		{
			name: "TerminateInstances_NotFound",
			run: func(t *testing.T) {
				h := newHandler()
				rec := postForm(t, h, "Action=TerminateInstances&Version=2016-11-15&InstanceId.1=i-nonexistent")

				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "InvalidInstanceID.NotFound")
			},
		},
		{
			name: "SecurityGroup_CRUD",
			run: func(t *testing.T) {
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
			},
		},
		{
			name: "SecurityGroup_DeleteNotFound",
			run: func(t *testing.T) {
				h := newHandler()
				rec := postForm(t, h, "Action=DeleteSecurityGroup&Version=2016-11-15&GroupId=sg-nonexistent")

				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "InvalidGroup.NotFound")
			},
		},
		{
			name: "VPC_Describe",
			run: func(t *testing.T) {
				h := newHandler()
				rec := postForm(t, h, "Action=DescribeVpcs&Version=2016-11-15")

				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "DescribeVpcsResponse")
				assert.Contains(t, rec.Body.String(), "vpc-default")
			},
		},
		{
			name: "CreateVpc",
			run: func(t *testing.T) {
				h := newHandler()
				rec := postForm(t, h, "Action=CreateVpc&Version=2016-11-15&CidrBlock=10.0.0.0/16")

				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "CreateVpcResponse")
				assert.Contains(t, rec.Body.String(), "10.0.0.0/16")
				assert.Contains(t, rec.Body.String(), "<vpcId>vpc-")
			},
		},
		{
			name: "CreateVpc_MissingCIDR",
			run: func(t *testing.T) {
				h := newHandler()
				rec := postForm(t, h, "Action=CreateVpc&Version=2016-11-15")

				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
			},
		},
		{
			name: "Subnet_Describe",
			run: func(t *testing.T) {
				h := newHandler()
				rec := postForm(t, h, "Action=DescribeSubnets&Version=2016-11-15")

				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "DescribeSubnetsResponse")
				assert.Contains(t, rec.Body.String(), "subnet-default")
			},
		},
		{
			name: "CreateSubnet",
			run: func(t *testing.T) {
				h := newHandler()
				rec := postForm(t, h,
					"Action=CreateSubnet&Version=2016-11-15&VpcId=vpc-default&CidrBlock=10.0.1.0/24&AvailabilityZone=us-east-1b")

				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "CreateSubnetResponse")
				assert.Contains(t, rec.Body.String(), "10.0.1.0/24")
				assert.Contains(t, rec.Body.String(), "us-east-1b")
			},
		},
		{
			name: "CreateSubnet_VPCNotFound",
			run: func(t *testing.T) {
				h := newHandler()
				rec := postForm(t, h,
					"Action=CreateSubnet&Version=2016-11-15&VpcId=vpc-nonexistent&CidrBlock=10.0.1.0/24")

				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "InvalidVpcID.NotFound")
			},
		},
		{
			name: "UnknownAction",
			run: func(t *testing.T) {
				h := newHandler()
				rec := postForm(t, h, "Action=UnknownAction&Version=2016-11-15")

				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
			},
		},
		{
			name: "MissingAction",
			run: func(t *testing.T) {
				h := newHandler()
				rec := postForm(t, h, "Version=2016-11-15&ImageId=ami-test")

				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "MissingAction")
			},
		},
		{
			name: "RouteMatcher",
			run: func(t *testing.T) {
				h := newHandler()
				matcher := h.RouteMatcher()

				e := echo.New()

				t.Run("matches EC2 POST", func(t *testing.T) {
					t.Parallel()

					req := httptest.NewRequest(
						http.MethodPost,
						"/",
						strings.NewReader("Version=2016-11-15&Action=DescribeInstances"),
					)
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
			},
		},
		{
			name: "Handler_GetMethod",
			run: func(t *testing.T) {
				h := newHandler()

				e := echo.New()
				req := httptest.NewRequest(http.MethodGet, "/", nil)
				rec := httptest.NewRecorder()
				c := e.NewContext(req, rec)

				err := h.Handler()(c)
				require.NoError(t, err)
				// GET with no body should return MissingAction error.
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "Provider_Name",
			run: func(t *testing.T) {
				p := &ec2.Provider{}
				assert.Equal(t, "EC2", p.Name())
			},
		},
		{
			name: "Provider_Init",
			run: func(t *testing.T) {
				p := &ec2.Provider{}
				appCtx := &service.AppContext{Logger: slog.Default()}

				reg, err := p.Init(appCtx)
				require.NoError(t, err)
				require.NotNil(t, reg)
				assert.Equal(t, "EC2", reg.Name())
			},
		},
		{
			name: "Name_And_Operations",
			run: func(t *testing.T) {
				h := newHandler()
				assert.Equal(t, "EC2", h.Name())

				ops := h.GetSupportedOperations()
				assert.Contains(t, ops, "RunInstances")
				assert.Contains(t, ops, "DescribeInstances")
				assert.Contains(t, ops, "TerminateInstances")
			},
		},
		{
			name: "MatchPriority",
			run: func(t *testing.T) {
				h := newHandler()
				assert.Equal(t, 80, h.MatchPriority())
			},
		},
		{
			name: "ExtractOperation",
			run: func(t *testing.T) {
				h := newHandler()
				e := echo.New()

				req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=DescribeInstances&Version=2016-11-15"))
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				c := e.NewContext(req, httptest.NewRecorder())

				assert.Equal(t, "DescribeInstances", h.ExtractOperation(c))
			},
		},
		{
			name: "ExtractResource",
			run: func(t *testing.T) {
				h := newHandler()
				e := echo.New()

				req := httptest.NewRequest(http.MethodPost, "/",
					strings.NewReader("Action=TerminateInstances&InstanceId.1=i-abc123&Version=2016-11-15"))
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				c := e.NewContext(req, httptest.NewRecorder())

				assert.Equal(t, "i-abc123", h.ExtractResource(c))
			},
		},
		{
			name: "TerminateInstances_MissingID",
			run: func(t *testing.T) {
				h := newHandler()
				rec := postForm(t, h, "Action=TerminateInstances&Version=2016-11-15")

				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
			},
		},
		{
			name: "CreateSecurityGroup_MissingName",
			run: func(t *testing.T) {
				h := newHandler()
				rec := postForm(t, h, "Action=CreateSecurityGroup&Version=2016-11-15&GroupDescription=test")

				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
			},
		},
		{
			name: "DeleteSecurityGroup_MissingGroupID",
			run: func(t *testing.T) {
				h := newHandler()
				rec := postForm(t, h, "Action=DeleteSecurityGroup&Version=2016-11-15")

				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
			},
		},
		{
			name: "CreateSubnet_MissingVPC",
			run: func(t *testing.T) {
				h := newHandler()
				rec := postForm(t, h, "Action=CreateSubnet&Version=2016-11-15&CidrBlock=10.0.1.0/24")

				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
			},
		},
		{
			name: "CreateSubnet_MissingCIDR",
			run: func(t *testing.T) {
				h := newHandler()
				rec := postForm(t, h, "Action=CreateSubnet&Version=2016-11-15&VpcId=vpc-default")

				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
			},
		},
		{
			name: "DescribeInstances_FilterByState",
			run: func(t *testing.T) {
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
			},
		},
		{
			name: "DuplicateSecurityGroup",
			run: func(t *testing.T) {
				bk := ec2.NewInMemoryBackend("000000000000", "us-east-1")

				_, err := bk.CreateSecurityGroup("my-sg", "test", "vpc-default")
				require.NoError(t, err)

				_, err = bk.CreateSecurityGroup("my-sg", "test", "vpc-default")
				require.ErrorIs(t, err, ec2.ErrDuplicateSGName)
			},
		},
		{
			name: "DescribeSecurityGroups_ByID",
			run: func(t *testing.T) {
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
			},
		},
		{
			name: "RunInstances_InvalidSubnet",
			run: func(t *testing.T) {
				bk := ec2.NewInMemoryBackend("000000000000", "us-east-1")
				_, err := bk.RunInstances("ami-test", "t2.micro", "subnet-nonexistent", 1)
				require.ErrorIs(t, err, ec2.ErrSubnetNotFound)
			},
		},
		{
			name: "CreateSecurityGroup_InvalidVPC",
			run: func(t *testing.T) {
				bk := ec2.NewInMemoryBackend("000000000000", "us-east-1")
				_, err := bk.CreateSecurityGroup("sg-name", "test", "vpc-nonexistent")
				require.ErrorIs(t, err, ec2.ErrVPCNotFound)
			},
		},
		{
			name: "CreateSecurityGroup_InvalidVPC_Handler",
			run: func(t *testing.T) {
				h := newHandler()
				rec := postForm(t, h,
					"Action=CreateSecurityGroup&Version=2016-11-15&GroupName=sg-name&GroupDescription=test&VpcId=vpc-nonexistent")

				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "InvalidVpcID.NotFound")
			},
		},
		{
			name: "RunInstances_InvalidSubnet_Handler",
			run: func(t *testing.T) {
				h := newHandler()
				rec := postForm(t, h,
					"Action=RunInstances&Version=2016-11-15&ImageId=ami-test&SubnetId=subnet-nonexistent&MinCount=1")

				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "InvalidSubnetID.NotFound")
			},
		},
		{
			name: "URLEncodedCIDR",
			run: func(t *testing.T) {
				h := newHandler()
				// CIDR with slash percent-encoded as %2F (as real AWS clients send it).
				rec := postForm(t, h, "Action=CreateVpc&Version=2016-11-15&CidrBlock=10.0.0.0%2F16")

				assert.Equal(t, http.StatusOK, rec.Code)
				// url.ParseQuery decodes %2F → / so CIDR should be stored correctly.
				assert.Contains(t, rec.Body.String(), "10.0.0.0/16")
			},
		},
		{
			name: "ExtractResource_GroupId",
			run: func(t *testing.T) {
				h := newHandler()
				e := echo.New()

				// DeleteSecurityGroup uses non-indexed GroupId=.
				req := httptest.NewRequest(http.MethodPost, "/",
					strings.NewReader("Action=DeleteSecurityGroup&GroupId=sg-abc123&Version=2016-11-15"))
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				c := e.NewContext(req, httptest.NewRecorder())

				assert.Equal(t, "sg-abc123", h.ExtractResource(c))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}
