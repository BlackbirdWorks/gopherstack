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

// TestSageMakerRuntimeDashboard verifies the SageMaker Runtime dashboard UI renders invocation data.
func TestSageMakerRuntimeDashboard(t *testing.T) {
	stack := newStack(t)

	stack.SageMakerRuntimeHandler.Backend.RecordInvocation(
		"InvokeEndpoint",
		"e2e-test-endpoint",
		`{"data": "test input"}`,
		`{"Body":"mock response from Gopherstack"}`,
	)

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
			saveScreenshot(t, page, "TestSageMakerRuntimeDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/sagemakerrumtime")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('SageMaker Runtime')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-endpoint")
	assert.Contains(t, content, "InvokeEndpoint")
}

// TestSageMakerRuntimeDashboard_Empty verifies the SageMaker Runtime dashboard renders correctly with no invocations.
func TestSageMakerRuntimeDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestSageMakerRuntimeDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/sagemakerrumtime")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('SageMaker Runtime')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No endpoint invocations recorded yet")
}
