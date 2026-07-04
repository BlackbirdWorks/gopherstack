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

// TestTransferDashboard verifies the Transfer dashboard UI renders servers.
func TestTransferDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.TransferHandler.Backend.CreateServer([]string{"SFTP"}, nil)
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
			saveScreenshot(t, page, "TestTransferDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/transfer")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "SFTP")
	assert.Contains(t, content, "ONLINE")
}

// TestTransferDashboard_Empty verifies the empty state renders correctly.
func TestTransferDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestTransferDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/transfer")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No Transfer Family servers found")
}

// TestTransferDashboard_CreateServer verifies the create server form works.
func TestTransferDashboard_CreateServer(t *testing.T) {
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
			saveScreenshot(t, page, "TestTransferDashboard_CreateServer")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/transfer")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Open create server modal
	err = page.Locator("button:has-text('Create Server')").Click()
	require.NoError(t, err)

	// Submit the form
	err = page.Locator("button:has-text('Create Server')").Last().Click()
	require.NoError(t, err)

	err = page.Locator("h2:has-text('Create Transfer Server')").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateHidden,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	// Wait for redirect back to index
	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Transfer Family")
}
