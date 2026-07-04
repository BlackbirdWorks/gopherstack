package ec2_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_FpgaImage_CreateDescribeDelete(t *testing.T) {
	t.Parallel()

	h := newHandler()

	createVals := url.Values{}
	createVals.Set("Action", "CreateFpgaImage")
	createVals.Set("Version", "2016-11-15")
	createVals.Set("Name", "my-afi")
	createVals.Set("Description", "test afi")

	createRec := postForm(t, h, createVals.Encode())
	require.Equal(t, http.StatusOK, createRec.Code)
	createBody := createRec.Body.String()
	assert.Contains(t, createBody, "<CreateFpgaImageResponse")

	afiID := extractBatch4XMLValue(createBody, "fpgaImageId")
	require.NotEmpty(t, afiID)
	assert.Contains(t, createBody, "<fpgaImageGlobalId>agfi-")

	descVals := url.Values{}
	descVals.Set("Action", "DescribeFpgaImages")
	descVals.Set("Version", "2016-11-15")
	descVals.Set("FpgaImageId.1", afiID)

	descRec := postForm(t, h, descVals.Encode())
	require.Equal(t, http.StatusOK, descRec.Code)
	descBody := descRec.Body.String()
	assert.Contains(t, descBody, "<DescribeFpgaImagesResponse")
	assert.Contains(t, descBody, "<name>my-afi</name>")
	assert.Contains(t, descBody, "<code>available</code>")

	deleteVals := url.Values{}
	deleteVals.Set("Action", "DeleteFpgaImage")
	deleteVals.Set("Version", "2016-11-15")
	deleteVals.Set("FpgaImageId", afiID)

	deleteRec := postForm(t, h, deleteVals.Encode())
	require.Equal(t, http.StatusOK, deleteRec.Code)
	assert.Contains(t, deleteRec.Body.String(), "<return>true</return>")

	descRec2 := postForm(t, h, descVals.Encode())
	require.Equal(t, http.StatusOK, descRec2.Code)
	assert.NotContains(t, descRec2.Body.String(), afiID)
}

func TestHandler_FpgaImage_CopyFpgaImage(t *testing.T) {
	t.Parallel()

	h := newHandler()

	createVals := url.Values{}
	createVals.Set("Action", "CreateFpgaImage")
	createVals.Set("Version", "2016-11-15")
	createVals.Set("Name", "source-afi")

	createRec := postForm(t, h, createVals.Encode())
	require.Equal(t, http.StatusOK, createRec.Code)
	afiID := extractBatch4XMLValue(createRec.Body.String(), "fpgaImageId")
	require.NotEmpty(t, afiID)

	copyVals := url.Values{}
	copyVals.Set("Action", "CopyFpgaImage")
	copyVals.Set("Version", "2016-11-15")
	copyVals.Set("SourceFpgaImageId", afiID)
	copyVals.Set("SourceRegion", "us-east-1")

	copyRec := postForm(t, h, copyVals.Encode())
	require.Equal(t, http.StatusOK, copyRec.Code)
	body := copyRec.Body.String()
	assert.Contains(t, body, "<CopyFpgaImageResponse")

	newID := extractBatch4XMLValue(body, "fpgaImageId")
	assert.NotEmpty(t, newID)
	assert.NotEqual(t, afiID, newID)
}

func TestHandler_FpgaImage_AttributeLifecycle(t *testing.T) {
	t.Parallel()

	h := newHandler()

	createVals := url.Values{}
	createVals.Set("Action", "CreateFpgaImage")
	createVals.Set("Version", "2016-11-15")
	createVals.Set("Name", "attr-afi")
	createVals.Set("Description", "initial")

	createRec := postForm(t, h, createVals.Encode())
	require.Equal(t, http.StatusOK, createRec.Code)
	afiID := extractBatch4XMLValue(createRec.Body.String(), "fpgaImageId")
	require.NotEmpty(t, afiID)

	// Steps below are intentionally sequential (not parallel subtests): the
	// reset step depends on the load permission set by the modify step.
	modVals := url.Values{}
	modVals.Set("Action", "ModifyFpgaImageAttribute")
	modVals.Set("Version", "2016-11-15")
	modVals.Set("FpgaImageId", afiID)
	modVals.Set("Attribute", "loadPermission")
	modVals.Set("LoadPermission.Add.1.Group", "all")

	modRec := postForm(t, h, modVals.Encode())
	require.Equal(t, http.StatusOK, modRec.Code)
	body := modRec.Body.String()
	assert.Contains(t, body, "<ModifyFpgaImageAttributeResponse")
	assert.Contains(t, body, "<group>all</group>")

	descAttrVals := url.Values{}
	descAttrVals.Set("Action", "DescribeFpgaImageAttribute")
	descAttrVals.Set("Version", "2016-11-15")
	descAttrVals.Set("FpgaImageId", afiID)
	descAttrVals.Set("Attribute", "loadPermission")

	descAttrRec := postForm(t, h, descAttrVals.Encode())
	require.Equal(t, http.StatusOK, descAttrRec.Code)
	descAttrBody := descAttrRec.Body.String()
	assert.Contains(t, descAttrBody, "<DescribeFpgaImageAttributeResponse")
	assert.Contains(t, descAttrBody, "<group>all</group>")

	descVals := url.Values{}
	descVals.Set("Action", "DescribeFpgaImages")
	descVals.Set("Version", "2016-11-15")
	descVals.Set("FpgaImageId.1", afiID)

	descRec := postForm(t, h, descVals.Encode())
	require.Equal(t, http.StatusOK, descRec.Code)
	assert.Contains(t, descRec.Body.String(), "<public>true</public>")

	resetVals := url.Values{}
	resetVals.Set("Action", "ResetFpgaImageAttribute")
	resetVals.Set("Version", "2016-11-15")
	resetVals.Set("FpgaImageId", afiID)
	resetVals.Set("Attribute", "loadPermission")

	resetRec := postForm(t, h, resetVals.Encode())
	require.Equal(t, http.StatusOK, resetRec.Code)
	assert.Contains(t, resetRec.Body.String(), "<return>true</return>")

	descRec2 := postForm(t, h, descVals.Encode())
	require.Equal(t, http.StatusOK, descRec2.Code)
	assert.Contains(t, descRec2.Body.String(), "<public>false</public>")
}

func TestHandler_FpgaImage_DeleteNotFound(t *testing.T) {
	t.Parallel()

	h := newHandler()

	vals := url.Values{}
	vals.Set("Action", "DeleteFpgaImage")
	vals.Set("Version", "2016-11-15")
	vals.Set("FpgaImageId", "afi-doesnotexist")

	rec := postForm(t, h, vals.Encode())
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidFpgaImageID.NotFound")
}
