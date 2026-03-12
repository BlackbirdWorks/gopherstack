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

// TestMediaStoreDataDashboard verifies the MediaStore Data dashboard UI renders object data.
func TestMediaStoreDataDashboard(t *testing.T) {
	stack := newStack(t)

	stack.MediaStoreDataHandler.Backend.PutObject(
		"/videos/e2e-clip.mp4",
		[]byte("e2e video content"),
		"video/mp4",
		"",
		"",
	)

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
			saveScreenshot(t, page, "TestMediaStoreDataDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/mediastoredata")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('MediaStore Data')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "videos/e2e-clip.mp4")
}

// TestMediaStoreDataDashboard_Empty verifies the MediaStore Data dashboard renders correctly with no data.
func TestMediaStoreDataDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestMediaStoreDataDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/mediastoredata")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('MediaStore Data')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No objects stored.")
}
