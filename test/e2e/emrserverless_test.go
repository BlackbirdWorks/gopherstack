//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/mxschmitt/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	emrserverlessbackend "github.com/blackbirdworks/gopherstack/services/emrserverless"
)

// TestEMRServerlessDashboard verifies the EMR Serverless dashboard renders applications.
func TestEMRServerlessDashboard(t *testing.T) {
	stack := newStack(t)

	app, err := stack.EmrServerlessHandler.Backend.CreateApplication(
		"e2e-spark-app",
		"SPARK",
		"emr-6.10.0",
		"",
		map[string]string{"env": "test"},
	)
	require.NoError(t, err)

	require.NoError(t, stack.EmrServerlessHandler.Backend.StartApplication(app.ApplicationID))

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
			saveScreenshot(t, page, "TestEMRServerlessDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/emrserverless")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-spark-app")
	assert.Contains(t, content, "Amazon EMR Serverless")
}

// TestEMRServerlessDashboard_Empty verifies the empty state renders correctly.
func TestEMRServerlessDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestEMRServerlessDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/emrserverless")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Amazon EMR Serverless")
	assert.Contains(t, content, "No applications found")
}

// TestEMRServerlessDashboard_JobRuns verifies job runs appear after clicking an application.
func TestEMRServerlessDashboard_JobRuns(t *testing.T) {
	stack := newStack(t)

	app, err := stack.EmrServerlessHandler.Backend.CreateApplication(
		"e2e-hive-app",
		"HIVE",
		"emr-6.10.0",
		"",
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, stack.EmrServerlessHandler.Backend.StartApplication(app.ApplicationID))

	jr, err := stack.EmrServerlessHandler.Backend.StartJobRun(
		app.ApplicationID,
		"arn:aws:iam::000000000000:role/EMRServerlessRole",
		"e2e-hive-job",
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
			saveScreenshot(t, page, "TestEMRServerlessDashboard_JobRuns")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/emrserverless")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Click the application to load its job runs.
	err = page.Locator("button:has-text('e2e-hive-app')").First().Click()
	require.NoError(t, err)

	err = page.Locator("text=e2e-hive-job").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, jr.JobRunID)
}

// TestEMRServerlessDashboard_Filtering verifies state filter buttons work.
func TestEMRServerlessDashboard_Filtering(t *testing.T) {
	stack := newStack(t)

	app, err := stack.EmrServerlessHandler.Backend.CreateApplication(
		"e2e-filter-app",
		"SPARK",
		"emr-6.10.0",
		"",
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, stack.EmrServerlessHandler.Backend.StartApplication(app.ApplicationID))

	// Create a job run and cancel it so we have two distinct states.
	jr, err := stack.EmrServerlessHandler.Backend.StartJobRun(
		app.ApplicationID,
		"arn:aws:iam::000000000000:role/EMRServerlessRole",
		"running-job",
		"",
		nil,
	)
	require.NoError(t, err)

	_, err = stack.EmrServerlessHandler.Backend.CancelJobRun(app.ApplicationID, jr.JobRunID)
	require.NoError(t, err)

	_, err = stack.EmrServerlessHandler.Backend.StartJobRun(
		app.ApplicationID,
		"arn:aws:iam::000000000000:role/EMRServerlessRole",
		"submitted-job",
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
			saveScreenshot(t, page, "TestEMRServerlessDashboard_Filtering")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/emrserverless")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Click the application to load job runs.
	err = page.Locator("button:has-text('e2e-filter-app')").First().Click()
	require.NoError(t, err)

	// Wait for job runs to appear.
	err = page.Locator("text=running-job").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	// Both jobs should be visible with ALL filter.
	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "running-job")
	assert.Contains(t, content, "submitted-job")

	// Click CANCELLED filter.
	err = page.Locator("button:has-text('CANCELLED')").Click()
	require.NoError(t, err)

	// Wait for the CANCELLED filter to apply (submitted-job should disappear).
	err = page.Locator("text=running-job").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(5000),
	})
	require.NoError(t, err)

	content, err = page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "running-job")
	assert.NotContains(t, content, "submitted-job")
}

// TestEMRServerlessDashboard_Refresh verifies the refresh button reloads applications.
func TestEMRServerlessDashboard_Refresh(t *testing.T) {
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
			saveScreenshot(t, page, "TestEMRServerlessDashboard_Refresh")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/emrserverless")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Initially no applications.
	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No applications found")

	// Add an application after page load.
	_, err = stack.EmrServerlessHandler.Backend.CreateApplication(
		"refresh-test-app",
		"SPARK",
		"emr-6.10.0",
		"",
		nil,
	)
	require.NoError(t, err)

	// Click refresh.
	err = page.Locator("button:has-text('Refresh')").Click()
	require.NoError(t, err)

	err = page.Locator("text=refresh-test-app").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)
}

// TestEMRServerlessDashboard_Tags verifies TagResource and ListTagsForResource round-trips.
func TestEMRServerlessDashboard_Tags(t *testing.T) {
	stack := newStack(t)

	app, err := stack.EmrServerlessHandler.Backend.CreateApplication(
		"tagged-app",
		"SPARK",
		"emr-6.10.0",
		"",
		map[string]string{"project": "phoenix"},
	)
	require.NoError(t, err)

	tags, err := stack.EmrServerlessHandler.Backend.ListTagsForResource(app.Arn)
	require.NoError(t, err)
	assert.Equal(t, "phoenix", tags["project"])

	require.NoError(t, stack.EmrServerlessHandler.Backend.TagResource(app.Arn, map[string]string{"owner": "alice"}))

	tags, err = stack.EmrServerlessHandler.Backend.ListTagsForResource(app.Arn)
	require.NoError(t, err)
	assert.Equal(t, "alice", tags["owner"])
	assert.Equal(t, "phoenix", tags["project"])

	require.NoError(t, stack.EmrServerlessHandler.Backend.UntagResource(app.Arn, []string{"project"}))

	tags, err = stack.EmrServerlessHandler.Backend.ListTagsForResource(app.Arn)
	require.NoError(t, err)
	assert.Equal(t, "alice", tags["owner"])
	_, hasProject := tags["project"]
	assert.False(t, hasProject)
}

// TestEMRServerlessDashboard_StateFilter verifies the ListJobRuns states query param.
func TestEMRServerlessDashboard_StateFilter(t *testing.T) {
	stack := newStack(t)

	app, err := stack.EmrServerlessHandler.Backend.CreateApplication(
		"state-filter-app",
		"SPARK",
		"emr-6.10.0",
		"",
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, stack.EmrServerlessHandler.Backend.StartApplication(app.ApplicationID))

	jr, err := stack.EmrServerlessHandler.Backend.StartJobRun(
		app.ApplicationID,
		"arn:aws:iam::000000000000:role/EMRServerlessRole",
		"to-cancel",
		"",
		nil,
	)
	require.NoError(t, err)
	_, err = stack.EmrServerlessHandler.Backend.CancelJobRun(app.ApplicationID, jr.JobRunID)
	require.NoError(t, err)

	_, err = stack.EmrServerlessHandler.Backend.StartJobRun(
		app.ApplicationID,
		"arn:aws:iam::000000000000:role/EMRServerlessRole",
		"pending-job",
		"",
		nil,
	)
	require.NoError(t, err)

	// Filter by CANCELLED: should return only 1.
	runs, _, err := stack.EmrServerlessHandler.Backend.ListJobRuns(
		app.ApplicationID,
		"",
		0,
		emrserverlessbackend.JobRunStateCancelled,
	)
	require.NoError(t, err)
	assert.Len(t, runs, 1)
	assert.Equal(t, emrserverlessbackend.JobRunStateCancelled, runs[0].State)

	// Filter by SUBMITTED: should return only 1.
	runs, _, err = stack.EmrServerlessHandler.Backend.ListJobRuns(
		app.ApplicationID,
		"",
		0,
		emrserverlessbackend.JobRunStateSubmitted,
	)
	require.NoError(t, err)
	assert.Len(t, runs, 1)
	assert.Equal(t, emrserverlessbackend.JobRunStateSubmitted, runs[0].State)

	// No filter: should return all 2.
	runs, _, err = stack.EmrServerlessHandler.Backend.ListJobRuns(app.ApplicationID, "", 0)
	require.NoError(t, err)
	assert.Len(t, runs, 2)
}
