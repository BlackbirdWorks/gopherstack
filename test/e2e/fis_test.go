//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	fisbackend "github.com/blackbirdworks/gopherstack/services/fis"
)

// TestFISDashboard verifies the FIS dashboard renders with templates and actions.
func TestFISDashboard(t *testing.T) {
	stack := newStack(t)

	// Seed an experiment template via the backend.
	_, err := stack.FISHandler.Backend.CreateExperimentTemplate(
		&fisbackend.ExportedCreateTemplateRequest{
			Description: "Stop DynamoDB writes",
			RoleArn:     "arn:aws:iam::000000000000:role/fis-role",
			Actions: map[string]fisbackend.ExportedActionDTO{
				"throttleDDB": {
					ActionID: "aws:fis:inject-api-throttle-error",
					Parameters: map[string]string{
						"service":    "dynamodb",
						"operations": "PutItem,GetItem",
						"percentage": "50",
						"duration":   "PT2M",
					},
				},
			},
			StopConditions: []fisbackend.ExportedStopConditionDTO{
				{Source: "none"},
			},
			Tags: map[string]string{},
		},
		"000000000000",
		"us-east-1",
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
			saveScreenshot(t, page, "TestFISDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/fis")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('FIS')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Experiment Templates")
	assert.Contains(t, content, "Stop DynamoDB writes")
	assert.Contains(t, content, "Action Catalog")
	assert.Contains(t, content, "aws:fis:inject-api-throttle-error")
}

// TestFISDashboard_Empty verifies the FIS dashboard renders correctly with no data.
func TestFISDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestFISDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/fis")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('FIS')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Experiment Templates")
	assert.Contains(t, content, "No experiment templates")
	assert.Contains(t, content, "Action Catalog")
}

// TestFISDashboard_CreateAndDeleteTemplate verifies the create and delete template UI flows.
func TestFISDashboard_CreateAndDeleteTemplate(t *testing.T) {
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
			saveScreenshot(t, page, "TestFISDashboard_CreateAndDeleteTemplate")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/fis")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('FIS')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Open create modal.
	err = page.Locator("button:has-text('+ Create Template')").Click()
	require.NoError(t, err)

	// Fill in description.
	err = page.Locator("input[name='description']").Fill("E2E test template")
	require.NoError(t, err)

	// Fill in role ARN.
	err = page.Locator("input[name='roleArn']").Fill("arn:aws:iam::000000000000:role/e2e-role")
	require.NoError(t, err)

	// Select action ID.
	_, err = page.Locator("select[name='actionId']").SelectOption(playwright.SelectOptionValues{
		Values: playwright.StringSlice("aws:fis:inject-api-internal-error"),
	})
	require.NoError(t, err)

	// Fill in service parameter.
	err = page.Locator("input[name='service']").Fill("s3")
	require.NoError(t, err)

	// Submit (use Last() since "Create Template" text appears in both button and submit).
	err = page.Locator("button:has-text('Create Template')").Last().Click()
	require.NoError(t, err)

	// Wait for redirect and confirm the template appears.
	err = page.Locator("td:has-text('E2E test template')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "E2E test template")

	// Delete the template.
	err = page.Locator("form[action='/dashboard/fis/templates/delete'] button").Click()
	require.NoError(t, err)

	err = page.Locator("td:has-text('No experiment templates')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)
}

// TestFISDashboard_StartAndStopExperiment verifies starting and stopping an experiment.
func TestFISDashboard_StartAndStopExperiment(t *testing.T) {
	stack := newStack(t)

	// Pre-create a template.
	tpl, err := stack.FISHandler.Backend.CreateExperimentTemplate(
		&fisbackend.ExportedCreateTemplateRequest{
			Description: "Wait experiment",
			RoleArn:     "arn:aws:iam::000000000000:role/fis-role",
			Actions: map[string]fisbackend.ExportedActionDTO{
				"wait": {
					ActionID: "aws:fis:wait",
					Parameters: map[string]string{
						"duration": "PT60S",
					},
				},
			},
			StopConditions: []fisbackend.ExportedStopConditionDTO{
				{Source: "none"},
			},
			Tags: map[string]string{},
		},
		"000000000000",
		"us-east-1",
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
			saveScreenshot(t, page, "TestFISDashboard_StartAndStopExperiment")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/fis")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('FIS')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Start the experiment.
	err = page.Locator("form[action='/dashboard/fis/experiments/start'] button").First().Click()
	require.NoError(t, err)

	// Wait for the experiment to appear in the Active Experiments section.
	err = page.Locator("#experiments-container").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Active Experiments")

	// Stop the running experiment via the backend.
	experiments, listErr := stack.FISHandler.Backend.ListExperiments()
	require.NoError(t, listErr)
	require.NotEmpty(t, experiments)

	_, stopErr := stack.FISHandler.Backend.StopExperiment(experiments[0].ID)
	// It's OK if the experiment already completed (no error) or was stopped.
	if stopErr != nil && stopErr.Error() != fisbackend.ErrExperimentNotRunning.Error() {
		require.NoError(t, stopErr)
	}

	// Ensure the template is still present.
	got, getErr := stack.FISHandler.Backend.GetExperimentTemplate(tpl.ID)
	require.NoError(t, getErr)
	assert.Equal(t, "Wait experiment", got.Description)
}
