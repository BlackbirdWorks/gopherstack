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

// TestECRDashboard verifies the ECR dashboard UI: repositories render correctly.
func TestECRDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.ECRHandler.Backend.CreateRepository(t.Context(), "demo-app/backend", "", false, "", "")
	require.NoError(t, err)

	_, err = stack.ECRHandler.Backend.CreateRepository(t.Context(), "demo-app/frontend", "", false, "", "")
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
			saveScreenshot(t, page, "TestECRDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/ecr")
	require.NoError(t, err)
	waitForSPA(t, page)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "demo-app/backend")
	assert.Contains(t, content, "demo-app/frontend")
}

// TestECRDashboard_Empty verifies the ECR dashboard empty state renders correctly.
func TestECRDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestECRDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/ecr")
	require.NoError(t, err)
	waitForSPA(t, page)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Repositories")
	assert.Contains(t, content, "No repositories found")
}

// TestECRDashboard_CreateAndDeleteRepository verifies the create and delete repository UI flows.
func TestECRDashboard_CreateAndDeleteRepository(t *testing.T) {
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
			saveScreenshot(t, page, "TestECRDashboard_CreateAndDeleteRepository")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/ecr")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Open create modal
	err = page.Locator("#create-repo-btn").First().Click()
	require.NoError(t, err)

	// Fill in repository name
	err = page.Locator("#new-repo-name").Fill("e2e-test-repo")
	require.NoError(t, err)

	// Submit form
	err = page.Locator("#confirm-create-repo-btn").Click()
	require.NoError(t, err)

	// Wait for the repository card to appear.
	err = page.Locator("text=e2e-test-repo").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-repo")

	// Delete the repository
	// Click the trash icon button for the specific repository
	err = page.Locator("#delete-repo-e2e-test-repo").Click()
	require.NoError(t, err)
	confirmDialog := page.Locator("[role='alertdialog']")
	err = confirmDialog.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)
	err = confirmDialog.Locator("button:has-text('Delete')").Click()
	require.NoError(t, err)

	err = page.Locator("text=e2e-test-repo").First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateHidden,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)
}
