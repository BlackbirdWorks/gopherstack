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

// TestIoTDataPlaneDashboard verifies the IoT Data Plane dashboard renders correctly.
func TestIoTDataPlaneDashboard(t *testing.T) {
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
			saveScreenshot(t, page, "TestIoTDataPlaneDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/iotdataplane")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('IoT Data Plane')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "IoT Data Plane")
	assert.Contains(t, content, "Publish")
	assert.Contains(t, content, "Supported Operations")
}

// TestIoTDataPlaneDashboard_Snippet verifies the IoT Data Plane dashboard snippet data renders.
func TestIoTDataPlaneDashboard_Snippet(t *testing.T) {
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
			saveScreenshot(t, page, "TestIoTDataPlaneDashboard_Snippet")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/iotdataplane")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('IoT Data Plane')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "iot-data")
}
