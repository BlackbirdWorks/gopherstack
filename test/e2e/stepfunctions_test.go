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

// TestStepFunctionsDashboard verifies the Step Functions dashboard UI renders state machines.
func TestStepFunctionsDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.StepFunctionsHandler.Backend.CreateStateMachine(
		"test-state-machine",
		`{"Comment":"test","StartAt":"Hello","States":{"Hello":{"Type":"Pass","End":true}}}`,
		"arn:aws:iam::000000000000:role/sfn-role",
		"STANDARD",
	)
	require.NoError(t, err)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	ctx2, err := browser.NewContext()
	require.NoError(t, err)
	defer ctx2.Close()

	page, err := ctx2.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestStepFunctionsDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/stepfunctions")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Step Functions')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "test-state-machine")
}

// TestStepFunctionsDashboard_Empty verifies the empty state renders correctly.
func TestStepFunctionsDashboard_Empty(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	ctx2, err := browser.NewContext()
	require.NoError(t, err)
	defer ctx2.Close()

	page, err := ctx2.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestStepFunctionsDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/stepfunctions")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Step Functions')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Step Functions")
}
