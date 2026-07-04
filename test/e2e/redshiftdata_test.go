//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/mxschmitt/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRedshiftDataDashboard verifies the RedshiftData dashboard UI renders the SQL editor.
func TestRedshiftDataDashboard(t *testing.T) {
	stack := newStack(t)

	_ = stack.RedshiftDataHandler

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

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Redshift Data")
	assert.Contains(t, content, "Run Query")
}

// TestRedshiftDataDashboard_Empty verifies the RedshiftData dashboard empty state.
func TestRedshiftDataDashboard_Empty(t *testing.T) {
	stack := newStack(t)

	_ = stack.RedshiftDataHandler

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

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Redshift Data")
	assert.Contains(t, content, "SQL Editor")
}
