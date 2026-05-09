package cloudformation_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCFN_GeneratedTemplates(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// CreateGeneratedTemplate
	rec := postForm(t, h, url.Values{
		"Action":                []string{"CreateGeneratedTemplate"},
		"GeneratedTemplateName": []string{"my-gen-template"},
	}.Encode())
	require.Equal(t, 200, rec.Code)

	// ListGeneratedTemplates
	rec = postForm(t, h, url.Values{
		"Action": []string{"ListGeneratedTemplates"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	// DescribeGeneratedTemplate
	rec = postForm(t, h, url.Values{
		"Action": []string{"DescribeGeneratedTemplate"},
		"GeneratedTemplateName": []string{"my-gen-template"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	// GetGeneratedTemplate
	rec = postForm(t, h, url.Values{
		"Action": []string{"GetGeneratedTemplate"},
		"GeneratedTemplateName": []string{"my-gen-template"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	// UpdateGeneratedTemplate
	rec = postForm(t, h, url.Values{
		"Action": []string{"UpdateGeneratedTemplate"},
		"GeneratedTemplateName": []string{"my-gen-template"},
		"NewGeneratedTemplateName": []string{"my-gen-template-v2"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	// DeleteGeneratedTemplate
	rec = postForm(t, h, url.Values{
		"Action": []string{"DeleteGeneratedTemplate"},
		"GeneratedTemplateName": []string{"my-gen-template-v2"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)
}

func TestCFN_ResourceScans(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// StartResourceScan
	rec := postForm(t, h, url.Values{
		"Action": []string{"StartResourceScan"},
	}.Encode())
	require.Equal(t, 200, rec.Code)

	// ListResourceScans
	rec = postForm(t, h, url.Values{
		"Action": []string{"ListResourceScans"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)
}
