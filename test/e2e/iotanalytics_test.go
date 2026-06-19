//go:build e2e
// +build e2e

package e2e_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIoTAnalyticsDashboard verifies the IoT Analytics dashboard renders channels correctly.
func TestIoTAnalyticsDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.IoTAnalyticsHandler.Backend.CreateChannel(
		context.Background(), "e2e_test_channel", map[string]string{
			"env": "e2e",
		}, nil, nil)
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
	assert.Contains(t, content, "e2e_test_channel")
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

	err = page.Locator("input#channel-name").Fill("ui_test_channel")
	require.NoError(t, err)

	err = page.Locator("button[type='submit']:has-text('Create')").Click()
	require.NoError(t, err)

	err = page.Locator("td.font-medium:has-text('ui_test_channel')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "ui_test_channel")

	err = page.Locator("button:has-text('Delete')").First().Click()
	require.NoError(t, err)

	err = page.Locator("text=ui_test_channel").First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateHidden,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err = page.Content()
	require.NoError(t, err)
	assert.NotContains(t, content, "ui_test_channel")
}
