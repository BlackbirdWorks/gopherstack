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

// TestIoTAnalyticsDashboard verifies the IoT Analytics dashboard renders channels correctly.
func TestIoTAnalyticsDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.IoTAnalyticsHandler.Backend.CreateChannel("e2e-test-channel", map[string]string{
		"env": "e2e",
	})
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
			saveScreenshot(t, page, "TestIoTAnalyticsDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/iotanalytics")
	require.NoError(t, err)

	err = page.Locator("h1").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "IoT Analytics")
	assert.Contains(t, content, "e2e-test-channel")
}

// TestIoTAnalyticsDashboard_Empty verifies the IoT Analytics dashboard renders correctly with no data.
func TestIoTAnalyticsDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestIoTAnalyticsDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/iotanalytics")
	require.NoError(t, err)

	err = page.Locator("h1").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "IoT Analytics")
	assert.Contains(t, content, "No channels found")
}

// TestIoTAnalyticsDashboard_CreateAndDeleteChannel tests channel creation and deletion via UI.
func TestIoTAnalyticsDashboard_CreateAndDeleteChannel(t *testing.T) {
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
			saveScreenshot(t, page, "TestIoTAnalyticsDashboard_CreateAndDeleteChannel")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/iotanalytics")
	require.NoError(t, err)

	err = page.Locator("h1").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	err = page.Locator("button:has-text('+ Create Channel')").Click()
	require.NoError(t, err)

	err = page.Locator("input#channel-name").Fill("ui-test-channel")
	require.NoError(t, err)

	err = page.Locator("button[type='submit']:has-text('Create')").Click()
	require.NoError(t, err)

	err = page.Locator("text=ui-test-channel").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "ui-test-channel")

	err = page.Locator("button:has-text('Delete')").First().Click()
	require.NoError(t, err)

	err = page.WaitForLoadState(playwright.PageWaitForLoadStateOptions{
		State: playwright.LoadStateNetworkidle,
	})
	require.NoError(t, err)

	content, err = page.Content()
	require.NoError(t, err)
	assert.NotContains(t, content, "ui-test-channel")
}
