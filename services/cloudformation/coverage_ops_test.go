package cloudformation_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCFN_GeneratedTemplates(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// CreateGeneratedTemplate
	rec := postForm(t, h, url.Values{
		"Action":                []string{"CreateGeneratedTemplate"},
		"GeneratedTemplateName": []string{"my-gen-template"},
	}.Encode())
	assert.True(t, rec.Code >= 200 && rec.Code < 300 || rec.Code == 400)

	// ListGeneratedTemplates
	rec = postForm(t, h, url.Values{
		"Action": []string{"ListGeneratedTemplates"},
	}.Encode())
	assert.True(t, rec.Code >= 200 && rec.Code < 300 || rec.Code == 400)

	// DescribeGeneratedTemplate
	rec = postForm(t, h, url.Values{
		"Action":                []string{"DescribeGeneratedTemplate"},
		"GeneratedTemplateName": []string{"my-gen-template"},
	}.Encode())
	assert.True(t, rec.Code >= 200 && rec.Code < 300 || rec.Code == 400)

	// GetGeneratedTemplate
	rec = postForm(t, h, url.Values{
		"Action":                []string{"GetGeneratedTemplate"},
		"GeneratedTemplateName": []string{"my-gen-template"},
	}.Encode())
	assert.True(t, rec.Code >= 200 && rec.Code < 300 || rec.Code == 400)

	// UpdateGeneratedTemplate
	rec = postForm(t, h, url.Values{
		"Action":                   []string{"UpdateGeneratedTemplate"},
		"GeneratedTemplateName":    []string{"my-gen-template"},
		"NewGeneratedTemplateName": []string{"my-gen-template-v2"},
	}.Encode())
	assert.True(t, rec.Code >= 200 && rec.Code < 300 || rec.Code == 400)

	// DeleteGeneratedTemplate
	rec = postForm(t, h, url.Values{
		"Action":                []string{"DeleteGeneratedTemplate"},
		"GeneratedTemplateName": []string{"my-gen-template-v2"},
	}.Encode())
	assert.True(t, rec.Code >= 200 && rec.Code < 300 || rec.Code == 400)
}

func TestCFN_ResourceScans(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// StartResourceScan
	rec := postForm(t, h, url.Values{
		"Action": []string{"StartResourceScan"},
	}.Encode())
	assert.True(t, rec.Code >= 200 && rec.Code < 300 || rec.Code == 400)

	// ListResourceScans
	rec = postForm(t, h, url.Values{
		"Action": []string{"ListResourceScans"},
	}.Encode())
	assert.True(t, rec.Code >= 200 && rec.Code < 300 || rec.Code == 400)
}
