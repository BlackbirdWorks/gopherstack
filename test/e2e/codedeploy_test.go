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

// TestCodeDeployDashboard verifies the CodeDeploy dashboard UI renders applications.
func TestCodeDeployDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.CodeDeployHandler.Backend.CreateApplication("e2e-test-app", "Server", nil)
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
			saveScreenshot(t, page, "TestCodeDeployDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/codedeploy")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('CodeDeploy')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-app")
}

// TestCodeDeployDashboard_Empty verifies the CodeDeploy dashboard empty state renders correctly.
func TestCodeDeployDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestCodeDeployDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/codedeploy")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('CodeDeploy')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No CodeDeploy applications")
}
