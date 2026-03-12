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

// TestIoTWirelessDashboard verifies the IoT Wireless dashboard UI renders service profile data.
func TestIoTWirelessDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.IoTWirelessHandler.Backend.CreateServiceProfile("000000000000", "us-east-1", "e2e-test-profile", nil)
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
			saveScreenshot(t, page, "TestIoTWirelessDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/iotwireless")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('IoT Wireless')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-profile")
	assert.Contains(t, content, "Create Service Profile")
}

// TestIoTWirelessDashboard_Empty verifies the IoT Wireless dashboard renders correctly with no data.
func TestIoTWirelessDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestIoTWirelessDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/iotwireless")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('IoT Wireless')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No service profiles found")
}
