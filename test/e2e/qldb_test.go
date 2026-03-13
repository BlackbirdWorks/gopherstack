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

// TestQLDBDashboard verifies the QLDB dashboard UI renders ledger data.
func TestQLDBDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.QLDBHandler.Backend.CreateLedger("e2e-test-ledger", "ALLOW_ALL", false, map[string]string{
		"Environment": "test",
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
			saveScreenshot(t, page, "TestQLDBDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/qldb")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('QLDB Ledgers')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-ledger")
	assert.Contains(t, content, "Create Ledger")
}

// TestQLDBDashboard_Empty verifies the QLDB dashboard renders correctly with no ledgers.
func TestQLDBDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestQLDBDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/qldb")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('QLDB Ledgers')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No ledgers found")
}
