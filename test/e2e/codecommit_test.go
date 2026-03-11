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

// TestCodeCommitDashboard verifies the CodeCommit dashboard UI renders repositories.
func TestCodeCommitDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.CodeCommitHandler.Backend.CreateRepository("e2e-test-repo", "E2E test repository", nil)
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
			saveScreenshot(t, page, "TestCodeCommitDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/codecommit")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('CodeCommit')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-repo")
}

// TestCodeCommitDashboard_Empty verifies the CodeCommit dashboard empty state renders correctly.
func TestCodeCommitDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestCodeCommitDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/codecommit")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('CodeCommit')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No CodeCommit repositories")
}
