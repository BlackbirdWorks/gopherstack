//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	codepipelinebackend "github.com/blackbirdworks/gopherstack/services/codepipeline"
)

// TestCodePipelineDashboard verifies the CodePipeline dashboard UI renders pipelines.
func TestCodePipelineDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.CodePipelineHandler.Backend.CreatePipeline(
		codepipelinebackend.PipelineDeclaration{
			Name:    "e2e-test-pipeline",
			RoleArn: "arn:aws:iam::000000000000:role/pipeline-role",
			ArtifactStore: codepipelinebackend.ArtifactStore{
				Type:     "S3",
				Location: "my-bucket",
			},
			Stages: []codepipelinebackend.Stage{
				{
					Name: "Source",
					Actions: []codepipelinebackend.Action{
						{
							Name: "SourceAction",
							ActionTypeID: codepipelinebackend.ActionTypeID{
								Category: "Source",
								Owner:    "AWS",
								Provider: "CodeCommit",
								Version:  "1",
							},
						},
					},
				},
			},
		},
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
			saveScreenshot(t, page, "TestCodePipelineDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/codepipeline")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('AWS CodePipeline')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-pipeline")
}

// TestCodePipelineDashboard_Empty verifies the CodePipeline dashboard renders with no pipelines.
func TestCodePipelineDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestCodePipelineDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/codepipeline")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('AWS CodePipeline')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "AWS CodePipeline")
	assert.Contains(t, content, "No pipelines")
}

// TestCodePipelineDashboard_CreatePipeline verifies creating a pipeline via the dashboard.
func TestCodePipelineDashboard_CreatePipeline(t *testing.T) {
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
			saveScreenshot(t, page, "TestCodePipelineDashboard_CreatePipeline")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/codepipeline")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('AWS CodePipeline')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	err = page.Locator("button:has-text('+ Pipeline')").Click()
	require.NoError(t, err)

	err = page.Locator("#pipeline-name").Fill("e2e-created-pipeline")
	require.NoError(t, err)

	err = page.Locator("button:has-text('Create')").Last().Click()
	require.NoError(t, err)

	err = page.Locator("h1:has-text('AWS CodePipeline')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-created-pipeline")
}
