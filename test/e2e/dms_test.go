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

// TestDMSDashboard verifies the DMS dashboard UI renders replication instances.
func TestDMSDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.DMSHandler.Backend.CreateReplicationInstance(
		"e2e-rep-inst", "dms.t3.medium", "", "", 50, false, true, false, nil,
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
			saveScreenshot(t, page, "TestDMSDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/dms")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Database Migration')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-rep-inst")
}

// TestDMSDashboard_Empty verifies the DMS dashboard empty state renders correctly.
func TestDMSDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestDMSDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/dms")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Database Migration')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No replication instances")
}
