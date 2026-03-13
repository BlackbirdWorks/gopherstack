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

// TestRDSDataDashboard verifies the RDS Data dashboard UI renders statement data.
func TestRDSDataDashboard(t *testing.T) {
	stack := newStack(t)

	_, _, _, err := stack.RDSDataHandler.Backend.ExecuteStatement(
		"arn:aws:rds:us-east-1:000000000000:cluster:e2e-test-cluster",
		"SELECT 1",
		"",
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
			saveScreenshot(t, page, "TestRDSDataDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/rdsdata")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('RDS Data Statements')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "SELECT 1")
	assert.Contains(t, content, "Execute Statement")
}

// TestRDSDataDashboard_Empty verifies the RDS Data dashboard renders correctly with no statements.
func TestRDSDataDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestRDSDataDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/rdsdata")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('RDS Data Statements')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No statements executed yet")
}
