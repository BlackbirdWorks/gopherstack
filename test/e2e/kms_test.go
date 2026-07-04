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

// TestKMSDashboard tests the KMS dashboard UI: create key, view detail, encrypt, decrypt.
func TestKMSDashboard(t *testing.T) {
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
			saveScreenshot(t, page, "TestKMSDashboard")
		}
	}()

	// Navigate to the KMS dashboard page.
	_, err = page.Goto(server.URL + "/dashboard/kms")
	require.NoError(t, err)

	// Wait for the KMS Keys heading.
	err = page.Locator("h1").First().WaitFor(
		playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)},
	)
	require.NoError(t, err)

	// Step 1: Open the Create Key form.
	err = page.Locator("#create-key-btn").Click()
	require.NoError(t, err)

	// Fill the optional description.
	err = page.Locator("#new-key-description").Fill("e2e-test-key")
	require.NoError(t, err)

	// Submit.
	err = page.Locator("button[type='submit']:has-text('Create')").Click()
	require.NoError(t, err)

	// Wait for the new key row to appear.
	keyRow := page.Locator("text=e2e-test-key").First()
	err = keyRow.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	// Step 2: Open the Crypto modal.
	err = page.Locator("button:has-text('Crypto')").First().Click()
	require.NoError(t, err)

	// Wait for Crypto modal.
	err = page.Locator("role=dialog").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	// Step 3: Encrypt some plaintext.
	err = page.Locator("#plaintext-input").Fill("hello-kms")
	require.NoError(t, err)

	err = page.Locator("#encrypt-submit").Click()
	require.NoError(t, err)

	// Wait for the encrypt result to appear.
	encryptResult := page.Locator("#encrypt-result")
	err = encryptResult.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	ciphertext, err := encryptResult.TextContent()
	require.NoError(t, err)
	assert.NotEmpty(t, ciphertext, "encrypt result should contain ciphertext")

	// Step 4: Decrypt the ciphertext.
	// (Note: in the UI, encrypt sets the ciphertext input already, but we'll re-fill it to be sure)
	err = page.Locator("#ciphertext-input").Fill(ciphertext)
	require.NoError(t, err)

	err = page.Locator("#decrypt-submit").Click()
	require.NoError(t, err)

	decryptResult := page.Locator("#decrypt-result")
	err = decryptResult.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	decrypted, err := decryptResult.TextContent()
	require.NoError(t, err)
	assert.Contains(t, decrypted, "hello-kms", "decrypted text should match original plaintext")
}
