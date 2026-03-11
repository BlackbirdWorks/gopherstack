//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	codebuildbackend "github.com/blackbirdworks/gopherstack/services/codebuild"
)

// TestCodeBuildDashboard verifies the CodeBuild dashboard UI renders projects.
func TestCodeBuildDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.CodeBuildHandler.Backend.CreateProject(
		"e2e-test-project",
		"E2E test project",
		codebuildbackend.ProjectSource{Type: "NO_SOURCE"},
		codebuildbackend.ProjectArtifacts{Type: "NO_ARTIFACTS"},
		codebuildbackend.ProjectEnvironment{
			Type:        "LINUX_CONTAINER",
			Image:       "aws/codebuild/standard:1.0",
			ComputeType: "BUILD_GENERAL1_SMALL",
		},
		"",
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
			saveScreenshot(t, page, "TestCodeBuildDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/codebuild")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('AWS CodeBuild')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-project")
}

// TestCodeBuildDashboard_Empty verifies the CodeBuild dashboard renders with no projects.
func TestCodeBuildDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestCodeBuildDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/codebuild")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('AWS CodeBuild')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "AWS CodeBuild")
	assert.Contains(t, content, "No projects")
}

// TestCodeBuildDashboard_CreateProject verifies creating a project via the dashboard.
func TestCodeBuildDashboard_CreateProject(t *testing.T) {
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
			saveScreenshot(t, page, "TestCodeBuildDashboard_CreateProject")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/codebuild")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('AWS CodeBuild')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	err = page.Locator("button:has-text('+ Project')").Click()
	require.NoError(t, err)

	err = page.Locator("#project-name").Fill("e2e-created-project")
	require.NoError(t, err)

	err = page.Locator("button:has-text('Create')").Last().Click()
	require.NoError(t, err)

	err = page.Locator("h1:has-text('AWS CodeBuild')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-created-project")
}
