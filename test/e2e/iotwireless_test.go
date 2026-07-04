//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/mxschmitt/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	iotbackend "github.com/blackbirdworks/gopherstack/services/iot"
)

// TestIoTWirelessDashboard verifies the IoT Wireless dashboard UI renders service profile data.
func TestIoTWirelessDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.IoTHandler.Backend.CreateThing(&iotbackend.CreateThingInput{
		ThingName: "e2e-test-thing",
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
			saveScreenshot(t, page, "TestIoTWirelessDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/iot")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-thing")
	assert.Contains(t, content, "AWS IoT Core")
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

	_, err = page.Goto(server.URL + "/dashboard/iot")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No IoT things found")
}
