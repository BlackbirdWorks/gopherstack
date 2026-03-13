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

// TestQLDBSessionDashboard verifies the QLDB Session dashboard UI renders session data.
func TestQLDBSessionDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.QLDBSessionHandler.Backend.StartSession("e2e-test-ledger")
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
			saveScreenshot(t, page, "TestQLDBSessionDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/qldbsession")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('QLDB Session')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-ledger")
}

// TestQLDBSessionDashboard_Empty verifies the QLDB Session dashboard renders correctly with no sessions.
func TestQLDBSessionDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestQLDBSessionDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/qldbsession")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('QLDB Session')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No active sessions")
}
