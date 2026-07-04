//go:build e2e

package e2e_test

import (
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/mxschmitt/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSTSDashboard verifies the STS dashboard UI.
func TestSTSDashboard(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	if u, err := url.Parse(server.URL); err == nil {
		stack.S3Handler.Endpoint = u.Host
	}

	ctx, err := browser.NewContext()
	require.NoError(t, err)
	defer ctx.Close()

	page, err := ctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestSTSDashboard")
		}
	}()

	// Navigate to the STS dashboard page.
	_, err = page.Goto(server.URL + "/dashboard/sts")
	require.NoError(t, err)

	// Wait for the page heading to appear.
	err = page.Locator("h1").First().WaitFor(
		playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)},
	)
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "000000000000")
}

// TestSTSDashboard_GenerateSessionToken verifies the session token generation flow.
func TestSTSDashboard_GenerateSessionToken(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	if u, err := url.Parse(server.URL); err == nil {
		stack.S3Handler.Endpoint = u.Host
	}

	ctx, err := browser.NewContext()
	require.NoError(t, err)
	defer ctx.Close()

	page, err := ctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestSTSDashboard_GenerateSessionToken")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/sts")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(
		playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)},
	)
	require.NoError(t, err)

	// Locate and click the Generate Session Token button via id selector.
	genBtn := page.Locator("button#gen-session-btn")
	require.NoError(t, genBtn.WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	}))

	require.NoError(t, genBtn.Click())

	// Wait for the session token credentials to appear (the result div is only rendered when
	// the token has been returned by the server).
	sessionResult := page.Locator("div#session-token-result")
	err = sessionResult.WaitFor(
		playwright.LocatorWaitForOptions{Timeout: playwright.Float(15000)},
	)
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "ASIA", "access key ID should be visible")
	assert.Contains(t, content, "expires in", "relative expiration time should appear")
}

// TestSTSDashboard_AssumeRole verifies the AssumeRole form flow.
func TestSTSDashboard_AssumeRole(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	if u, err := url.Parse(server.URL); err == nil {
		stack.S3Handler.Endpoint = u.Host
	}

	ctx, err := browser.NewContext()
	require.NoError(t, err)
	defer ctx.Close()

	page, err := ctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestSTSDashboard_AssumeRole")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/sts")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(
		playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)},
	)
	require.NoError(t, err)

	// Fill in AssumeRole form fields.
	roleArnInput := page.Locator("input#assume-role-arn")
	require.NoError(t, roleArnInput.WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	}))
	require.NoError(t, roleArnInput.Fill("arn:aws:iam::000000000000:role/E2ETestRole"))

	sessionInput := page.Locator("input#assume-role-session")
	require.NoError(t, sessionInput.Fill("e2e-test-session"))

	// Click Assume Role button via id selector.
	assumeBtn := page.Locator("button#assume-role-btn")
	require.NoError(t, assumeBtn.WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	}))
	require.NoError(t, assumeBtn.Click())

	// Wait for credential to appear (use First() to avoid strict-mode violation when
	// other p.font-mono elements are already on the page from GetCallerIdentity).
	err = page.Locator("p.font-mono").First().WaitFor(
		playwright.LocatorWaitForOptions{Timeout: playwright.Float(15000)},
	)
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "ASIA", "assumed role access key should be visible")
}

// TestSTSDashboard_ValidatorClear verifies the validator has a clear button.
func TestSTSDashboard_ValidatorClear(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	if u, err := url.Parse(server.URL); err == nil {
		stack.S3Handler.Endpoint = u.Host
	}

	ctx, err := browser.NewContext()
	require.NoError(t, err)
	defer ctx.Close()

	page, err := ctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestSTSDashboard_ValidatorClear")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/sts")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(
		playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)},
	)
	require.NoError(t, err)

	// Type into the validator input.
	validatorInput := page.Locator("input[placeholder='ASIA...']")
	require.NoError(t, validatorInput.WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	}))
	require.NoError(t, validatorInput.Fill("ASIAIOSFODNN7EXAMPLE"))

	// Clear button should now appear (CSS :has-text avoids *string panic).
	clearBtn := page.Locator("button:has-text('Clear')").First()
	require.NoError(t, clearBtn.WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(5000),
	}))
	require.NoError(t, clearBtn.Click())

	// Input should be empty.
	value, err := validatorInput.InputValue()
	require.NoError(t, err)
	assert.Empty(t, value)
}

// TestSTSDashboard_MetricsPanel verifies the STS metrics panel renders.
func TestSTSDashboard_MetricsPanel(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	if u, err := url.Parse(server.URL); err == nil {
		stack.S3Handler.Endpoint = u.Host
	}

	ctx, err := browser.NewContext()
	require.NoError(t, err)
	defer ctx.Close()

	page, err := ctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestSTSDashboard_MetricsPanel")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/sts")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(
		playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)},
	)
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	// Metrics panel heading must be present.
	assert.Contains(t, content, "STS Janitor Metrics")
}

// TestSTSDashboard_DecodeAuthMessageClear verifies the decode panel clear button.
func TestSTSDashboard_DecodeAuthMessageClear(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	if u, err := url.Parse(server.URL); err == nil {
		stack.S3Handler.Endpoint = u.Host
	}

	ctx, err := browser.NewContext()
	require.NoError(t, err)
	defer ctx.Close()

	page, err := ctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestSTSDashboard_DecodeAuthMessageClear")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/sts")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(
		playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)},
	)
	require.NoError(t, err)

	// Fill in the decode textarea.
	decodeArea := page.Locator("textarea[placeholder*='base64']")
	require.NoError(t, decodeArea.WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	}))
	require.NoError(t, decodeArea.Fill("dGVzdA=="))

	// Decode button — use id selector to avoid HasText *string panic in playwright-go.
	decodeBtn := page.Locator("button#decode-auth-btn")
	require.NoError(t, decodeBtn.WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	}))
	require.NoError(t, decodeBtn.Click())

	// Wait for decoded output.
	err = page.Locator("pre").First().WaitFor(
		playwright.LocatorWaitForOptions{Timeout: playwright.Float(10000)},
	)
	require.NoError(t, err)

	// Now click Clear — locate by CSS :has-text pseudo-class (plain string, no *string).
	clearBtn := page.Locator("button:has-text('Clear')").Last()
	require.NoError(t, clearBtn.WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(5000),
	}))
	require.NoError(t, clearBtn.Click())

	// Pre should have disappeared.
	err = page.Locator("pre").First().WaitFor(
		playwright.LocatorWaitForOptions{
			State:   playwright.WaitForSelectorStateDetached,
			Timeout: playwright.Float(5000),
		},
	)
	require.NoError(t, err)
}
