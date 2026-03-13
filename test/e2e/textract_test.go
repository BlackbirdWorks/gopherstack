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

// TestTextractDashboard verifies the Textract dashboard UI renders async jobs.
func TestTextractDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.TextractHandler.Backend.StartDocumentAnalysis("s3://my-bucket/invoice.pdf")
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
			saveScreenshot(t, page, "TestTextractDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/textract")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Textract Jobs')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "DocumentAnalysis")
	assert.Contains(t, content, "SUCCEEDED")
}

// TestTextractDashboard_Empty verifies the empty state renders correctly.
func TestTextractDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestTextractDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/textract")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Textract Jobs')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No Textract jobs found")
}

// TestTextractDashboard_StartAnalysis verifies starting an analysis job via the UI.
func TestTextractDashboard_StartAnalysis(t *testing.T) {
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
			saveScreenshot(t, page, "TestTextractDashboard_StartAnalysis")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/textract")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Textract Jobs')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	err = page.Click("button:has-text('+ Analyze Document')")
	require.NoError(t, err)

	require.NoError(t, page.Fill("input[name='bucket']", "ui-test-bucket"))
	require.NoError(t, page.Fill("input[name='key']", "ui-test-document.pdf"))
	require.NoError(t, page.Click("button[type='submit']:has-text('Start Analysis')"))

	err = page.Locator("h1:has-text('Textract Jobs')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "DocumentAnalysis")
}
