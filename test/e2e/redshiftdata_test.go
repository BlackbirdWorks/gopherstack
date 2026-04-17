//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/require"
)

// TestRedshiftDataDashboard verifies the removed RedshiftData route returns 404.
func TestRedshiftDataDashboard(t *testing.T) {
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
			saveScreenshot(t, page, "TestRedshiftDataDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/redshiftdata")
	require.NoError(t, err)

	err = page.Locator("text=404").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(30000),
	})
	require.NoError(t, err)
}

// TestRedshiftDataDashboard_Empty is no longer applicable (route removed).
// Kept as placeholder for backward compatibility with test runner.
