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

// TestCloudTrailDashboard verifies the CloudTrail dashboard UI renders trails.
func TestCloudTrailDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.CloudTrailHandler.Backend.CreateTrail(
		"e2e-test-trail", "e2e-test-bucket", "", "", "", "", "",
		true, false, false, nil,
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
			saveScreenshot(t, page, "TestCloudTrailDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/cloudtrail")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('CloudTrail')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-trail")
}

// TestCloudTrailDashboard_Empty verifies the CloudTrail dashboard renders with no trails.
func TestCloudTrailDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestCloudTrailDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/cloudtrail")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('CloudTrail')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No CloudTrail trails")
}
