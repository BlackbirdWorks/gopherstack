package dms_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeleteInstanceProfile(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddInstanceProfileInternal("del-ip")

	rec := doDMS(t, h, "DeleteInstanceProfile", map[string]any{
		"InstanceProfileArn": "del-ip",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, h.Backend.InstanceProfileCount())

	rec2 := doDMS(t, h, "DeleteInstanceProfile", map[string]any{
		"InstanceProfileArn": "del-ip",
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

func TestModifyInstanceProfile(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddInstanceProfileInternal("mod-ip")

	descRec := doDMS(t, h, "DescribeInstanceProfiles", map[string]any{})
	require.Equal(t, http.StatusOK, descRec.Code)
	profiles := parseJSON(t, descRec)["InstanceProfiles"].([]any)
	require.Len(t, profiles, 1)
	ipArn := profiles[0].(map[string]any)["InstanceProfileArn"].(string)

	rec := doDMS(t, h, "ModifyInstanceProfile", map[string]any{
		"InstanceProfileArn": ipArn,
		"Description":        "updated description",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doDMS(t, h, "ModifyInstanceProfile", map[string]any{
		"InstanceProfileArn": "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}
