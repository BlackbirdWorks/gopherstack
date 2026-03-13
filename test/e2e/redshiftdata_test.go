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

// TestRedshiftDataDashboard verifies the Redshift Data dashboard UI renders statements.
func TestRedshiftDataDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.RedshiftDataHandler.Backend.ExecuteStatement(
		"SELECT 1", "test-cluster", "", "dev", "", "", "test-statement",
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
			saveScreenshot(t, page, "TestRedshiftDataDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/redshiftdata")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Redshift Data Statements')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "SELECT 1")
}

// TestRedshiftDataDashboard_Empty verifies the Redshift Data dashboard renders correctly with no statements.
func TestRedshiftDataDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestRedshiftDataDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/redshiftdata")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Redshift Data Statements')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Redshift Data Statements")
}
