//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	iotbackend "github.com/blackbirdworks/gopherstack/services/iot"
)

// TestIoTDashboard verifies the IoT dashboard UI: things and rules render correctly.
func TestIoTDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.IoTHandler.Backend.CreateThing(&iotbackend.CreateThingInput{
		ThingName: "test-device-1",
	})
	require.NoError(t, err)

	_, err = stack.IoTHandler.Backend.CreateThing(&iotbackend.CreateThingInput{
		ThingName: "test-device-2",
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
			saveScreenshot(t, page, "TestIoTDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/iot")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('IoT Core')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "test-device-1")
	assert.Contains(t, content, "test-device-2")
}

// TestIoTDashboard_Empty verifies the IoT dashboard empty state renders correctly.
func TestIoTDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestIoTDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/iot")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('IoT Core')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Things")
	assert.Contains(t, content, "No things")
}

// TestIoTDashboard_CreateAndDeleteThing verifies the create and delete thing UI flows.
func TestIoTDashboard_CreateAndDeleteThing(t *testing.T) {
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
			saveScreenshot(t, page, "TestIoTDashboard_CreateAndDeleteThing")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/iot")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('IoT Core')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Open create modal
	err = page.Locator("button:has-text('+ Create Thing')").Click()
	require.NoError(t, err)

	// Fill in thing name
	err = page.Locator("input[name='name']").Fill("e2e-test-device")
	require.NoError(t, err)

	// Submit form (use Last() since "Create Thing" text appears in both the header button and submit button)
	err = page.Locator("button:has-text('Create Thing')").Last().Click()
	require.NoError(t, err)

	// Wait for redirect and verify
	err = page.Locator("td:has-text('e2e-test-device')").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-device")

	// Delete the thing
	err = page.Locator("form[action='/dashboard/iot/thing/delete'] button").Click()
	require.NoError(t, err)

	err = page.Locator("td:has-text('No things')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)
}
