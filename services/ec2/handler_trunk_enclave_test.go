package ec2_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_TrunkInterface_AssociateDescribeDisassociate(t *testing.T) {
	t.Parallel()

	h := newHandler()

	branchENI, err := h.Backend.CreateNetworkInterface("subnet-default", "branch")
	require.NoError(t, err)
	trunkENI, err := h.Backend.CreateNetworkInterface("subnet-default", "trunk")
	require.NoError(t, err)

	assocVals := url.Values{}
	assocVals.Set("Action", "AssociateTrunkInterface")
	assocVals.Set("Version", "2016-11-15")
	assocVals.Set("BranchInterfaceId", branchENI.ID)
	assocVals.Set("TrunkInterfaceId", trunkENI.ID)
	assocVals.Set("VlanId", "5")

	assocRec := postForm(t, h, assocVals.Encode())
	require.Equal(t, http.StatusOK, assocRec.Code)
	assocBody := assocRec.Body.String()
	assert.Contains(t, assocBody, "<AssociateTrunkInterfaceResponse")
	assert.Contains(t, assocBody, "<interfaceProtocol>VLAN</interfaceProtocol>")

	associationID := extractXMLValue(t, assocBody, "associationId")
	require.NotEmpty(t, associationID)

	describeVals := url.Values{}
	describeVals.Set("Action", "DescribeTrunkInterfaceAssociations")
	describeVals.Set("Version", "2016-11-15")
	describeVals.Set("AssociationId.1", associationID)

	describeRec := postForm(t, h, describeVals.Encode())
	require.Equal(t, http.StatusOK, describeRec.Code)
	assert.Contains(t, describeRec.Body.String(), associationID)

	disassocVals := url.Values{}
	disassocVals.Set("Action", "DisassociateTrunkInterface")
	disassocVals.Set("Version", "2016-11-15")
	disassocVals.Set("AssociationId", associationID)

	disassocRec := postForm(t, h, disassocVals.Encode())
	require.Equal(t, http.StatusOK, disassocRec.Code)
	assert.Contains(t, disassocRec.Body.String(), "<return>true</return>")

	describeRec2 := postForm(t, h, describeVals.Encode())
	require.Equal(t, http.StatusOK, describeRec2.Code)
	assert.NotContains(t, describeRec2.Body.String(), associationID)
}

func TestHandler_TrunkInterface_MissingProtocolSelectorFails(t *testing.T) {
	t.Parallel()

	h := newHandler()

	branchENI, err := h.Backend.CreateNetworkInterface("subnet-default", "branch")
	require.NoError(t, err)
	trunkENI, err := h.Backend.CreateNetworkInterface("subnet-default", "trunk")
	require.NoError(t, err)

	vals := url.Values{}
	vals.Set("Action", "AssociateTrunkInterface")
	vals.Set("Version", "2016-11-15")
	vals.Set("BranchInterfaceId", branchENI.ID)
	vals.Set("TrunkInterfaceId", trunkENI.ID)

	rec := postForm(t, h, vals.Encode())
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Code>InvalidParameterValue</Code>")
}

func TestHandler_EnclaveCertIamRole_AssociateGetDisassociate(t *testing.T) {
	t.Parallel()

	h := newHandler()
	certArn := "arn:aws:acm:us-east-1:123456789012:certificate/abc"
	roleArn := "arn:aws:iam::123456789012:role/enclave-role"

	assocVals := url.Values{}
	assocVals.Set("Action", "AssociateEnclaveCertificateIamRole")
	assocVals.Set("Version", "2016-11-15")
	assocVals.Set("CertificateArn", certArn)
	assocVals.Set("RoleArn", roleArn)

	assocRec := postForm(t, h, assocVals.Encode())
	require.Equal(t, http.StatusOK, assocRec.Code)
	assocBody := assocRec.Body.String()
	assert.Contains(t, assocBody, "<AssociateEnclaveCertificateIamRoleResponse")
	assert.NotEmpty(t, extractXMLValue(t, assocBody, "certificateS3BucketName"))

	getVals := url.Values{}
	getVals.Set("Action", "GetAssociatedEnclaveCertificateIamRoles")
	getVals.Set("Version", "2016-11-15")
	getVals.Set("CertificateArn", certArn)

	getRec := postForm(t, h, getVals.Encode())
	require.Equal(t, http.StatusOK, getRec.Code)
	assert.Contains(t, getRec.Body.String(), roleArn)

	disassocVals := url.Values{}
	disassocVals.Set("Action", "DisassociateEnclaveCertificateIamRole")
	disassocVals.Set("Version", "2016-11-15")
	disassocVals.Set("CertificateArn", certArn)
	disassocVals.Set("RoleArn", roleArn)

	disassocRec := postForm(t, h, disassocVals.Encode())
	require.Equal(t, http.StatusOK, disassocRec.Code)
	assert.Contains(t, disassocRec.Body.String(), "<return>true</return>")

	getRec2 := postForm(t, h, getVals.Encode())
	require.Equal(t, http.StatusOK, getRec2.Code)
	assert.NotContains(t, getRec2.Body.String(), roleArn)
}

func TestHandler_EnclaveCertIamRole_DisassociateNotFoundFails(t *testing.T) {
	t.Parallel()

	h := newHandler()

	vals := url.Values{}
	vals.Set("Action", "DisassociateEnclaveCertificateIamRole")
	vals.Set("Version", "2016-11-15")
	vals.Set("CertificateArn", "arn:cert")
	vals.Set("RoleArn", "arn:role")

	rec := postForm(t, h, vals.Encode())
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Code>InvalidParameterValue</Code>")
}
