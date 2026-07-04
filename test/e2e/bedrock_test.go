//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/mxschmitt/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBedrockDashboard verifies the Bedrock dashboard UI renders foundation models.
func TestBedrockDashboard(t *testing.T) {
	stack := newStack(t)
	var err error

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

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Amazon Bedrock")
	assert.Contains(t, content, "Foundation Models")
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

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Amazon Bedrock")
	assert.Contains(t, content, "amazon.titan-text-express-v1")
}

// TestBedrockDashboard_CustomModels verifies the custom models tab renders.
func TestBedrockDashboard_CustomModels(t *testing.T) {
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
			saveScreenshot(t, page, "TestBedrockDashboard_CustomModels")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/bedrock")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	err = page.Locator("button:has-text('Custom Models')").Click()
	require.NoError(t, err)

	err = page.Locator("text=No custom models found").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Custom Models")
	assert.Contains(t, content, "No custom models found")
}
