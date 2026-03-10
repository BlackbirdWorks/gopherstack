//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/amplify"
	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAmplifyDashboard verifies the Amplify dashboard UI renders apps.
func TestAmplifyDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.AmplifyHandler.Backend.CreateApp(
		"e2e-test-app",
		"",
		"",
		string(amplify.PlatformWEB),
		nil,
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
			saveScreenshot(t, page, "TestAmplifyDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/amplify")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Amplify Apps')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-app")
}

// TestAmplifyDashboard_Empty verifies the Amplify dashboard empty state renders correctly.
func TestAmplifyDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestAmplifyDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/amplify")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Amplify Apps')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Amplify Apps")
}
