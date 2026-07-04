//go:build e2e
// +build e2e

package e2e_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/mxschmitt/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	schedulerbackend "github.com/blackbirdworks/gopherstack/services/scheduler"
)

// TestSchedulerDashboard verifies the Scheduler dashboard UI renders schedules and supports create/delete.
func TestSchedulerDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.SchedulerHandler.Backend.CreateSchedule(
		context.Background(),
		"test-schedule",
		"",
		"rate(5 minutes)",
		"",
		"",
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

	bctx, err := browser.NewContext()
	require.NoError(t, err)
	defer bctx.Close()

	page, err := bctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestSchedulerDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/scheduler")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "test-schedule")
	assert.Contains(t, content, "EventBridge Scheduler")
}

// TestSchedulerDashboard_Empty verifies the empty state renders correctly.
func TestSchedulerDashboard_Empty(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	bctx, err := browser.NewContext()
	require.NoError(t, err)
	defer bctx.Close()

	page, err := bctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestSchedulerDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/scheduler")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
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

	bctx, err := browser.NewContext()
	require.NoError(t, err)
	defer bctx.Close()

	page, err := bctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestSchedulerDashboard_CreateAndDelete")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/scheduler")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Open the create modal.
	err = page.Locator("button:has-text('Create Schedule')").First().Click()
	require.NoError(t, err)

	// Fill in the form.
	err = page.Locator("#sch-name").Fill("ui-test-schedule")
	require.NoError(t, err)

	err = page.Locator("#sch-expr").Fill("rate(1 hour)")
	require.NoError(t, err)

	err = page.Locator("#sch-target").Fill("arn:aws:lambda:us-east-1:000000000000:function:test-fn")
	require.NoError(t, err)

	err = page.Locator("#sch-role").Fill("arn:aws:iam::000000000000:role/scheduler-role")
	require.NoError(t, err)

	// Submit the form.
	err = page.Locator("button:has-text('Create Schedule')").Last().Click()
	require.NoError(t, err)

	err = page.Locator("h2:has-text('Create Schedule')").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateHidden,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "EventBridge Scheduler")
}
