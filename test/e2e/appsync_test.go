//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/appsync"
	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAppSyncDashboard verifies the AppSync dashboard UI renders GraphQL APIs.
func TestAppSyncDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.AppSyncHandler.Backend.CreateGraphqlAPI(
		"e2e-test-api",
		appsync.AuthTypeAPIKey,
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
			saveScreenshot(t, page, "TestAppSyncDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/appsync")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('AppSync GraphQL APIs')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-api")
}

// TestAppSyncDashboard_Empty verifies the empty state renders correctly.
func TestAppSyncDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestAppSyncDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/appsync")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('AppSync GraphQL APIs')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "AppSync GraphQL APIs")
}
