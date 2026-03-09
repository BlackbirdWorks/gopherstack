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

// TestECRDashboard verifies the ECR dashboard UI: repositories render correctly.
func TestECRDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.ECRHandler.Backend.CreateRepository("demo-app/backend")
	require.NoError(t, err)

	_, err = stack.ECRHandler.Backend.CreateRepository("demo-app/frontend")
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

	err = page.Locator("h1:has-text('ECR')").WaitFor(playwright.LocatorWaitForOptions{
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

	err = page.Locator("h1:has-text('ECR')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Repositories")
	assert.Contains(t, content, "No repositories")
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

	err = page.Locator("h1:has-text('ECR')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Open create modal
	err = page.Locator("button:has-text('+ Create Repository')").Click()
	require.NoError(t, err)

	// Fill in repository name
	err = page.Locator("input[name='name']").Fill("e2e-test-repo")
	require.NoError(t, err)

	// Submit form (use Last() since "Create Repository" text appears in both the header button and submit button)
	err = page.Locator("button:has-text('Create Repository')").Last().Click()
	require.NoError(t, err)

	// Wait for redirect and verify
	err = page.Locator("td:has-text('e2e-test-repo')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-repo")

	// Delete the repository
	err = page.Locator("form[action='/dashboard/ecr/repository/delete'] button").Click()
	require.NoError(t, err)

	err = page.Locator("td:has-text('No repositories')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)
}
