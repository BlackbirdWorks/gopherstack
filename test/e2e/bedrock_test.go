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

// TestBedrockDashboard verifies the Bedrock dashboard UI renders guardrails and foundation models.
func TestBedrockDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.BedrockHandler.Backend.CreateGuardrail(
		"e2e-test-guardrail",
		"E2E test guardrail",
		"Input blocked.",
		"Output blocked.",
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
			saveScreenshot(t, page, "TestBedrockDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/bedrock")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Amazon Bedrock')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-guardrail")
	assert.Contains(t, content, "READY")
	assert.Contains(t, content, "amazon.titan-text-express-v1")
}

// TestBedrockDashboard_Empty verifies the Bedrock dashboard empty state (only foundation models).
func TestBedrockDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestBedrockDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/bedrock")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Amazon Bedrock')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Amazon Bedrock")
	assert.Contains(t, content, "amazon.titan-text-express-v1")
}

// TestBedrockDashboard_CreateGuardrail verifies creating a guardrail via the dashboard.
func TestBedrockDashboard_CreateGuardrail(t *testing.T) {
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
			saveScreenshot(t, page, "TestBedrockDashboard_CreateGuardrail")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/bedrock")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Amazon Bedrock')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Open the create guardrail modal
	err = page.Locator("button:has-text('+ Guardrail')").Click()
	require.NoError(t, err)

	err = page.Locator("#guardrail-name").Fill("e2e-created-guardrail")
	require.NoError(t, err)

	err = page.Locator("button:has-text('Create')").Last().Click()
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Amazon Bedrock')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-created-guardrail")
}
