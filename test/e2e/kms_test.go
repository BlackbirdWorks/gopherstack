//go:build e2e

package e2e_test

import (
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/playwright-community/playwright-go"
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
	err = page.Locator("h1:has-text('KMS Keys')").WaitFor(
		playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)},
	)
	require.NoError(t, err)

	// Step 1: Open the Create Key form.
	err = page.Click("button:has-text('Create Key')")
	require.NoError(t, err)

	// Fill the optional description.
	require.NoError(t, page.Fill("input[name='description']", "e2e-test-key"))

	// Submit.
	err = page.Click("button[type='submit']:has-text('Create')")
	require.NoError(t, err)

	// Wait for the new key row to appear.
	time.Sleep(500 * time.Millisecond)

	keyRow := page.Locator("td:has-text('e2e-test-key')").First()
	err = keyRow.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Step 2: Click the key link to open the detail page.
	keyLink := page.Locator("a[href*='/dashboard/kms/key?id=']").First()
	err = keyLink.Click()
	require.NoError(t, err)

	// Wait for KMS Key Detail page.
	err = page.Locator("h1:has-text('KMS Key Detail')").WaitFor(
		playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)},
	)
	require.NoError(t, err)

	// Step 3: Encrypt some plaintext.
	require.NoError(t, page.Fill("input[name='plaintext']", "hello-kms"))

	err = page.Locator("form[hx-post*='/dashboard/kms/encrypt'] button[type='submit']").Click()
	require.NoError(t, err)

	// Wait for the encrypt result to appear.
	encryptResult := page.Locator("#encrypt-result")
	err = encryptResult.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	ciphertext, err := encryptResult.TextContent()
	require.NoError(t, err)
	assert.NotEmpty(t, ciphertext, "encrypt result should contain ciphertext")

	// Step 4: Decrypt the ciphertext.
	require.NoError(t, page.Fill("input[name='ciphertext']", ciphertext))

	err = page.Locator("form[hx-post='/dashboard/kms/decrypt'] button[type='submit']").Click()
	require.NoError(t, err)

	decryptResult := page.Locator("#decrypt-result")
	err = decryptResult.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	decrypted, err := decryptResult.TextContent()
	require.NoError(t, err)
	assert.Contains(t, decrypted, "hello-kms", "decrypted text should match original plaintext")
}
