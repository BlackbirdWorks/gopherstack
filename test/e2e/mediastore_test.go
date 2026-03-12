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

// TestMediaStoreDashboard verifies the MediaStore dashboard UI renders container data.
func TestMediaStoreDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.MediaStoreHandler.Backend.CreateContainer(
		"us-east-1",
		"000000000000",
		"e2e-test-container",
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
			saveScreenshot(t, page, "TestMediaStoreDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/mediastore")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('MediaStore')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-container")
	assert.Contains(t, content, "Create Container")
}

// TestMediaStoreDashboard_Empty verifies the MediaStore dashboard renders correctly with no data.
func TestMediaStoreDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestMediaStoreDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/mediastore")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('MediaStore')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No containers found")
}
