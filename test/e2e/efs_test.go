//go:build e2e
// +build e2e

package e2e_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/mxschmitt/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	efsbackend "github.com/blackbirdworks/gopherstack/services/efs"
)

// TestEFSDashboard verifies the EFS dashboard UI renders file systems.
func TestEFSDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.EFSHandler.Backend.CreateFileSystem(context.Background(), efsbackend.CreateFileSystemRequest{
		CreationToken:   "e2e-test-token",
		PerformanceMode: "generalPurpose",
		ThroughputMode:  "bursting",
		Tags:            map[string]string{"Name": "e2e-test-fs"},
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
			saveScreenshot(t, page, "TestEFSDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/efs")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Wait for the fs to appear in the table.
	err = page.Locator("text=e2e-test-token").First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)
}

// TestEFSDashboard_Empty verifies the EFS dashboard empty state renders correctly.
func TestEFSDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestEFSDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/efs")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No file systems found")
}

// Note: Create/Delete UI flow for EFS is not currently implemented in the Svelte UI.
