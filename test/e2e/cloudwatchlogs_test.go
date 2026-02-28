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

// TestCloudWatchLogsDashboard verifies the CloudWatch Logs dashboard UI renders log groups.
func TestCloudWatchLogsDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.CloudWatchLogsHandler.Backend.CreateLogGroup("/test/e2e-log-group")
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
			saveScreenshot(t, page, "TestCloudWatchLogsDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/cloudwatchlogs")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('CloudWatch Logs')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-log-group")
}

// TestCloudWatchLogsDashboard_Empty verifies the empty state renders correctly.
func TestCloudWatchLogsDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestCloudWatchLogsDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/cloudwatchlogs")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('CloudWatch Logs')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "CloudWatch Logs")
}
