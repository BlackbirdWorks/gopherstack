//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestElasticTranscoderDashboard verifies the Elastic Transcoder dashboard UI renders pipelines correctly.
func TestElasticTranscoderDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.ElasticTranscoderHandler.Backend.CreatePipeline(
		"e2e-test-pipeline",
		"my-input-bucket",
		"my-output-bucket",
		"arn:aws:iam::000000000000:role/Elastic_Transcoder_Default_Role",
	)
	require.NoError(t, err)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	ctx, err := browser.NewContext()
	require.NoError(t, err)
	defer ctx.Close()

	page, err := ctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestElasticTranscoderDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/elastictranscoder")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Elastic Transcoder')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-pipeline")
}

// TestElasticTranscoderDashboard_Empty verifies the Elastic Transcoder dashboard empty state renders correctly.
func TestElasticTranscoderDashboard_Empty(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	ctx, err := browser.NewContext()
	require.NoError(t, err)
	defer ctx.Close()

	page, err := ctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestElasticTranscoderDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/elastictranscoder")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Elastic Transcoder')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No pipelines")
}
