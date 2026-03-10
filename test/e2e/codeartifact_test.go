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

// TestCodeArtifactDashboard verifies the CodeArtifact dashboard UI renders domains.
func TestCodeArtifactDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.CodeArtifactHandler.Backend.CreateDomain("e2e-test-domain", "", nil)
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
			saveScreenshot(t, page, "TestCodeArtifactDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/codeartifact")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('CodeArtifact')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-domain")
	assert.Contains(t, content, "Active")
}

// TestCodeArtifactDashboard_Empty verifies the CodeArtifact dashboard empty state renders correctly.
func TestCodeArtifactDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestCodeArtifactDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/codeartifact")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('CodeArtifact')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No CodeArtifact domains")
}
