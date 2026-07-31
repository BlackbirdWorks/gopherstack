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

// TestMediaStoreDataDashboard verifies the MediaStore Data dashboard UI renders object data.
func TestMediaStoreDataDashboard(t *testing.T) {
	stack := newStack(t)

	_, _ = stack.MediaStoreDataHandler.Backend.PutObject(
		t.Context(),
		"/videos/e2e-clip.mp4",
		[]byte("e2e video content"),
		"video/mp4",
		"",
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

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// The page is a real path-prefix object browser matching MediaStore's
	// ListItems semantics: the root view (Path="") only lists items directly
	// under root, so a nested object at "videos/e2e-clip.mp4" does not appear
	// as a literal string anywhere at root -- it surfaces as a "videos"
	// FOLDER item. Drill into it to reach the actual object, exactly as a
	// real user/client would.
	folderBtn := page.Locator("button:has-text('videos')").First()
	err = folderBtn.WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(30000)})
	require.NoError(t, err)
	err = folderBtn.Click()
	require.NoError(t, err)

	// Breadcrumb reflects the navigated-into path.
	breadcrumb := page.Locator("nav[aria-label='Breadcrumb']")
	err = breadcrumb.Locator("text=videos").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	// The object itself is now listed by its name within that folder.
	err = page.Locator("text=e2e-clip.mp4").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-clip.mp4")
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

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	// The rebuilt page's DataTable emptyMessage for an empty root listing
	// (see +page.svelte: emptyMessage on the browser DataTable) -- "No
	// objects stored." was the previous page's text and is not what this
	// page renders.
	assert.Contains(t, content, "No objects or folders here. Upload a file to get started.")
}
