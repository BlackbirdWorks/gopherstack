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

// TestRDSDashboard verifies the RDS dashboard UI renders instances.
func TestRDSDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.RDSHandler.Backend.CreateDBInstance(
		"test-db", "postgres", "db.t3.micro", "mydb", "admin", "", 20,
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
			saveScreenshot(t, page, "TestRDSDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/rds")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('RDS Instances')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "test-db")
	assert.Contains(t, content, "postgres")
}

// TestRDSDashboard_Empty verifies the empty state renders correctly.
func TestRDSDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestRDSDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/rds")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('RDS Instances')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No RDS instances")
}

// TestRDSDashboard_CreateAndDelete verifies creating and deleting an instance via the UI.
func TestRDSDashboard_CreateAndDelete(t *testing.T) {
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
			saveScreenshot(t, page, "TestRDSDashboard_CreateAndDelete")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/rds")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('RDS Instances')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Open the create modal.
	err = page.Locator("button:has-text('+ Create Instance')").Click()
	require.NoError(t, err)

	// Fill in the form.
	err = page.Locator("#db_instance_id").Fill("ui-test-db")
	require.NoError(t, err)

	// Submit the form.
	err = page.Locator("button[type=submit]:has-text('Create')").Click()
	require.NoError(t, err)

	// Wait for redirect back.
	err = page.WaitForURL("**/dashboard/rds", playwright.PageWaitForURLOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "ui-test-db")

	// Delete the instance.
	err = page.Locator("form[action='/dashboard/rds/delete'] button[type=submit]").First().Click()
	require.NoError(t, err)

	err = page.WaitForURL("**/dashboard/rds", playwright.PageWaitForURLOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	content, err = page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No RDS instances")
}
