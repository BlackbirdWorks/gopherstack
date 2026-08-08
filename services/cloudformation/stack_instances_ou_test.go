package cloudformation_test

import (
	"encoding/xml"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	"github.com/blackbirdworks/gopherstack/services/organizations"
)

// newOrgBackedHandler builds a CloudFormation handler wired to a real
// Organizations backend with one OU containing two accounts, mirroring the
// cli.go wireCloudFormationOrganizations pattern but in-process so the test
// doesn't need the full server. Returns the handler and the OU ID.
func newOrgBackedHandler(t *testing.T) (*cloudformation.Handler, string) {
	t.Helper()

	orgBackend := organizations.NewInMemoryBackend("000000000000", "us-east-1")
	_, root, err := orgBackend.CreateOrganization("ALL")
	require.NoError(t, err)

	ou, err := orgBackend.CreateOrganizationalUnit(root.ID, "Workloads", nil)
	require.NoError(t, err)

	for _, email := range []string{"acct1@example.com", "acct2@example.com"} {
		status, acctErr := orgBackend.CreateAccount(email, email, "OrganizationAccountAccessRole", "ALLOW", nil)
		require.NoError(t, acctErr)
		require.NoError(t, orgBackend.MoveAccount(status.AccountID, root.ID, ou.ID))
	}

	cfnBackend := cloudformation.NewInMemoryBackendWithConfig(
		"000000000000", "us-east-1", cloudformation.NewResourceCreator(nil),
	)
	cfnBackend.SetOrganizationsDirectory(orgBackend)

	return cloudformation.NewHandler(cfnBackend), ou.ID
}

type listStackInstancesResponse struct {
	XMLName xml.Name `xml:"ListStackInstancesResponse"`
	Result  struct {
		Summaries []struct {
			Account              string `xml:"Account"`
			Region               string `xml:"Region"`
			OrganizationalUnitID string `xml:"OrganizationalUnitId"`
		} `xml:"Summaries>member"`
	} `xml:"ListStackInstancesResult"`
}

// TestCreateStackInstances_ServiceManagedOU drives a real
// DeploymentTargets.OrganizationalUnitIds request against a SERVICE_MANAGED
// StackSet wired to a real Organizations hierarchy (two accounts under one
// OU) and verifies CloudFormation actually resolves the OU to its member
// accounts -- the deployment-target math -- rather than rejecting or
// no-oping on OU targets.
func TestCreateStackInstances_ServiceManagedOU(t *testing.T) {
	t.Parallel()

	h, ouID := newOrgBackedHandler(t)

	postForm(t, h, url.Values{"Action": {"ActivateOrganizationsAccess"}}.Encode())
	postForm(t, h, url.Values{
		"Action":          {"CreateStackSet"},
		"StackSetName":    {"ou-ss"},
		"TemplateBody":    {simpleTemplate},
		"PermissionModel": {"SERVICE_MANAGED"},
	}.Encode())

	rec := postForm(t, h, url.Values{
		"Action":       {"CreateStackInstances"},
		"StackSetName": {"ou-ss"},
		"DeploymentTargets.OrganizationalUnitIds.member.1": {ouID},
		"Regions.member.1": {"us-east-1"},
	}.Encode())
	require.Equal(t, 200, rec.Code, "body: %s", rec.Body.String())

	rec = postForm(t, h, url.Values{
		"Action":       {"ListStackInstances"},
		"StackSetName": {"ou-ss"},
	}.Encode())
	require.Equal(t, 200, rec.Code)

	var list listStackInstancesResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &list))
	require.Len(t, list.Result.Summaries, 2, "one stack instance per account under the OU")
	for _, s := range list.Result.Summaries {
		assert.Equal(t, "us-east-1", s.Region)
		assert.Equal(t, ouID, s.OrganizationalUnitID)
		assert.NotEmpty(t, s.Account)
	}

	rec = postForm(t, h, url.Values{
		"Action":       {"ListStackSetAutoDeploymentTargets"},
		"StackSetName": {"ou-ss"},
	}.Encode())
	require.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), ouID)
}

// TestCreateStackInstances_OURequiresServiceManaged ensures OU-based
// deployment targets are rejected against a self-managed StackSet instead
// of silently expanding them.
func TestCreateStackInstances_OURequiresServiceManaged(t *testing.T) {
	t.Parallel()

	h, ouID := newOrgBackedHandler(t)

	postForm(t, h, url.Values{"Action": {"ActivateOrganizationsAccess"}}.Encode())
	postForm(t, h, url.Values{
		"Action":       {"CreateStackSet"},
		"StackSetName": {"self-managed-ss"},
		"TemplateBody": {simpleTemplate},
	}.Encode())

	rec := postForm(t, h, url.Values{
		"Action":       {"CreateStackInstances"},
		"StackSetName": {"self-managed-ss"},
		"DeploymentTargets.OrganizationalUnitIds.member.1": {ouID},
		"Regions.member.1": {"us-east-1"},
	}.Encode())
	assert.NotEqual(t, 200, rec.Code)
}

// TestCreateStackInstances_OURequiresOrganizationsAccess ensures OU targets
// are rejected before ActivateOrganizationsAccess, matching real AWS.
func TestCreateStackInstances_OURequiresOrganizationsAccess(t *testing.T) {
	t.Parallel()

	h, ouID := newOrgBackedHandler(t)

	postForm(t, h, url.Values{
		"Action":          {"CreateStackSet"},
		"StackSetName":    {"no-access-ss"},
		"TemplateBody":    {simpleTemplate},
		"PermissionModel": {"SERVICE_MANAGED"},
	}.Encode())

	rec := postForm(t, h, url.Values{
		"Action":       {"CreateStackInstances"},
		"StackSetName": {"no-access-ss"},
		"DeploymentTargets.OrganizationalUnitIds.member.1": {ouID},
		"Regions.member.1": {"us-east-1"},
	}.Encode())
	assert.NotEqual(t, 200, rec.Code)
}

// TestCreateStackInstances_OURequiresWiredDirectory ensures OU targets fail
// honestly (rather than silently doing nothing) when no Organizations
// backend has been wired.
func TestCreateStackInstances_OURequiresWiredDirectory(t *testing.T) {
	t.Parallel()

	h := newHandler() // no SetOrganizationsDirectory call
	postForm(t, h, url.Values{"Action": {"ActivateOrganizationsAccess"}}.Encode())
	postForm(t, h, url.Values{
		"Action":          {"CreateStackSet"},
		"StackSetName":    {"unwired-ss"},
		"TemplateBody":    {simpleTemplate},
		"PermissionModel": {"SERVICE_MANAGED"},
	}.Encode())

	rec := postForm(t, h, url.Values{
		"Action":       {"CreateStackInstances"},
		"StackSetName": {"unwired-ss"},
		"DeploymentTargets.OrganizationalUnitIds.member.1": {"ou-fake"},
		"Regions.member.1": {"us-east-1"},
	}.Encode())
	assert.NotEqual(t, 200, rec.Code)
}
