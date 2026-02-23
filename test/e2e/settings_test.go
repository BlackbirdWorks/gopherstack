//go:build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestE2E_SettingsPage(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	ctx, err := browser.NewContext()
	require.NoError(t, err)
	defer ctx.Close()

	page, err := ctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	// Navigate to the settings page.
	_, err = page.Goto(server.URL + "/dashboard/settings")
	require.NoError(t, err)

	// Page title should contain "Settings".
	title, err := page.Title()
	require.NoError(t, err)
	assert.Contains(t, title, "Settings")

	// The page should show the Account ID value.
	body, err := page.TextContent("body")
	require.NoError(t, err)
	assert.Contains(t, body, "000000000000", "expected account ID on settings page")
	assert.Contains(t, body, "us-east-1", "expected region on settings page")
}

func TestE2E_SettingsPage_NavbarIcon(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	ctx, err := browser.NewContext()
	require.NoError(t, err)
	defer ctx.Close()

	page, err := ctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	// Navigate to the main dashboard (redirects to /dynamodb).
	_, err = page.Goto(server.URL + "/dashboard/dynamodb")
	require.NoError(t, err)

	// The settings icon link should be present in the navbar.
	settingsLink, err := page.QuerySelector("a[href='/dashboard/settings']")
	require.NoError(t, err)
	require.NotNil(t, settingsLink, "settings icon link not found in navbar")

	// Click the settings icon to navigate to the settings page.
	err = settingsLink.Click()
	require.NoError(t, err)

	err = page.WaitForURL("**/dashboard/settings", playwright.PageWaitForURLOptions{
		Timeout: playwright.Float(5000),
	})
	require.NoError(t, err)

	body, err := page.TextContent("body")
	require.NoError(t, err)
	assert.Contains(t, body, "Runtime configuration")
}
