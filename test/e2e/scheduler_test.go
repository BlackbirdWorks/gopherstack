//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	schedulerbackend "github.com/blackbirdworks/gopherstack/scheduler"
)

// TestSchedulerDashboard verifies the Scheduler dashboard UI renders schedules and supports create/delete.
func TestSchedulerDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.SchedulerHandler.Backend.CreateSchedule(
		"test-schedule",
		"rate(5 minutes)",
		schedulerbackend.Target{
			ARN:     "arn:aws:lambda:us-east-1:000000000000:function:my-fn",
			RoleARN: "arn:aws:iam::000000000000:role/scheduler-role",
		},
		"ENABLED",
		schedulerbackend.FlexibleTimeWindow{Mode: "OFF"},
	)
	require.NoError(t, err)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	context, err := browser.NewContext()
	require.NoError(t, err)
	defer context.Close()

	page, err := context.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestSchedulerDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/scheduler")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Scheduler')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "test-schedule")
	assert.Contains(t, content, "rate(5 minutes)")
}

// TestSchedulerDashboard_Empty verifies the empty state renders correctly.
func TestSchedulerDashboard_Empty(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	context, err := browser.NewContext()
	require.NoError(t, err)
	defer context.Close()

	page, err := context.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestSchedulerDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/scheduler")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Scheduler')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No schedules")
}

// TestSchedulerDashboard_CreateAndDelete verifies creating and deleting a schedule via the UI.
func TestSchedulerDashboard_CreateAndDelete(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	context, err := browser.NewContext()
	require.NoError(t, err)
	defer context.Close()

	page, err := context.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestSchedulerDashboard_CreateAndDelete")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/scheduler")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Scheduler')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Open the create modal.
	err = page.Locator("button:has-text('+ Create Schedule')").Click()
	require.NoError(t, err)

	// Fill in the form.
	err = page.Locator("#name").Fill("ui-test-schedule")
	require.NoError(t, err)

	err = page.Locator("#expression").Fill("rate(1 hour)")
	require.NoError(t, err)

	// Submit the form.
	err = page.Locator("button[type=submit]:has-text('Create')").Click()
	require.NoError(t, err)

	// Wait for redirect back to the scheduler page.
	err = page.WaitForURL("**/dashboard/scheduler", playwright.PageWaitForURLOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "ui-test-schedule")

	// Delete the schedule.
	err = page.Locator("form[action='/dashboard/scheduler/delete'] button[type=submit]").First().Click()
	require.NoError(t, err)

	err = page.WaitForURL("**/dashboard/scheduler", playwright.PageWaitForURLOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	content, err = page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No schedules")
}
