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

// TestCloudFrontDashboard verifies the CloudFront dashboard UI renders distributions.
func TestCloudFrontDashboard(t *testing.T) {
	stack := newStack(t)

	rawConfig := `<DistributionConfig>` +
		`<CallerReference>e2e-ref-001</CallerReference>` +
		`<Comment>e2e-test-distribution</Comment>` +
		`<Enabled>true</Enabled>` +
		`</DistributionConfig>`

	_, err := stack.CloudFrontHandler.Backend.CreateDistribution(
		"e2e-ref-001",
		"e2e-test-distribution",
		true,
		[]byte(rawConfig),
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
			saveScreenshot(t, page, "TestCloudFrontDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/cloudfront")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('CloudFront')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-distribution")
	assert.Contains(t, content, "cloudfront.net")
}

// TestCloudFrontDashboard_Empty verifies the CloudFront dashboard empty state renders correctly.
func TestCloudFrontDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestCloudFrontDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/cloudfront")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('CloudFront')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No CloudFront distributions")
}
