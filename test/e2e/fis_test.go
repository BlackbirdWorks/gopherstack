//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/mxschmitt/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	fisbackend "github.com/blackbirdworks/gopherstack/services/fis"
)

// TestFISDashboard verifies the FIS dashboard renders template data in the current UI.
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

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	err = page.Locator("button:has-text('Experiment Templates')").Click()
	require.NoError(t, err)

	err = page.Locator("text=Stop DynamoDB writes").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Experiment Templates")
	assert.Contains(t, content, "Stop DynamoDB writes")
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

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Live Experiments")
	assert.Contains(t, content, "No registered chaos assets.")
}

// TestFISDashboard_TemplatesTab verifies the templates tab lists seeded templates.
func TestFISDashboard_TemplatesTab(t *testing.T) {
	stack := newStack(t)

	_, err := stack.FISHandler.Backend.CreateExperimentTemplate(
		&fisbackend.ExportedCreateTemplateRequest{
			Description: "E2E template",
			RoleArn:     "arn:aws:iam::000000000000:role/e2e-role",
			Actions: map[string]fisbackend.ExportedActionDTO{
				"wait": {
					ActionID: "aws:fis:wait",
					Parameters: map[string]string{
						"duration": "PT60S",
					},
				},
			},
			StopConditions: []fisbackend.ExportedStopConditionDTO{{Source: "none"}},
			Tags:           map[string]string{},
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
			saveScreenshot(t, page, "TestFISDashboard_TemplatesTab")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/fis")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	err = page.Locator("button:has-text('Experiment Templates')").Click()
	require.NoError(t, err)

	err = page.Locator("text=E2E template").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "E2E template")
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

	experiment, err := stack.FISHandler.Backend.StartExperiment(
		t.Context(),
		&fisbackend.ExportedStartExperimentRequest{ExperimentTemplateID: tpl.ID},
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

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	err = page.Locator("text=" + experiment.ID).WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	err = page.Locator("text=" + experiment.ID).Click()
	require.NoError(t, err)

	err = page.Locator("button[title='Stop Chaos']").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "running")

	err = page.Locator("button[title='Stop Chaos']").Click()
	require.NoError(t, err)
	confirmDialog := page.Locator("[role='alertdialog']")
	err = confirmDialog.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)
	err = confirmDialog.Locator("button:has-text('Stop')").Click()
	require.NoError(t, err)

	err = page.Locator("text=stopped").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	// Ensure the template is still present.
	got, getErr := stack.FISHandler.Backend.GetExperimentTemplate(tpl.ID)
	require.NoError(t, getErr)
	assert.Equal(t, "Wait experiment", got.Description)
}
