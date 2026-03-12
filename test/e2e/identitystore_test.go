//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	identitystorebackend "github.com/blackbirdworks/gopherstack/services/identitystore"
)

const testE2EStoreID = "d-0000000000"

// TestIdentityStoreDashboard verifies the Identity Store dashboard renders with seeded data.
func TestIdentityStoreDashboard(t *testing.T) {
	stack := newStack(t)

	// Seed a user.
	user, err := stack.IdentityStoreHandler.Backend.CreateUser(testE2EStoreID, &identitystorebackend.CreateUserRequest{
		UserName:    "alice.smith",
		DisplayName: "Alice Smith",
		Name:        &identitystorebackend.Name{GivenName: "Alice", FamilyName: "Smith"},
	})
	require.NoError(t, err)

	// Seed a group.
	group, err := stack.IdentityStoreHandler.Backend.CreateGroup(testE2EStoreID, &identitystorebackend.CreateGroupRequest{
		DisplayName: "Engineering",
		Description: "Engineering team",
	})
	require.NoError(t, err)

	// Add user to group.
	_, err = stack.IdentityStoreHandler.Backend.CreateGroupMembership(
		testE2EStoreID,
		group.GroupID,
		identitystorebackend.MemberID{UserID: user.UserID},
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
			saveScreenshot(t, page, "TestIdentityStoreDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/identitystore")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Identity Store')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "alice.smith")
	assert.Contains(t, content, "Alice Smith")
	assert.Contains(t, content, "Engineering")
}

// TestIdentityStoreDashboard_Empty verifies the Identity Store dashboard renders with no data.
func TestIdentityStoreDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestIdentityStoreDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/identitystore")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Identity Store')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Identity Store")
}

// TestIdentityStoreDashboard_CreateUser verifies the create user form flow.
func TestIdentityStoreDashboard_CreateUser(t *testing.T) {
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
			saveScreenshot(t, page, "TestIdentityStoreDashboard_CreateUser")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/identitystore")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Identity Store')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Open the create user modal.
	err = page.Locator("button:has-text('+ Create User')").Click()
	require.NoError(t, err)

	// Fill in user name.
	err = page.Locator("input[name='user_name']").Fill("bob.jones")
	require.NoError(t, err)

	// Fill in display name.
	err = page.Locator("#create-user-modal input[name='display_name']").Fill("Bob Jones")
	require.NoError(t, err)

	// Submit the form.
	err = page.Locator("button[type='submit']:has-text('Create')").Last().Click()
	require.NoError(t, err)

	// Wait for the table to show the new user.
	err = page.Locator("td:has-text('bob.jones')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "bob.jones")
	assert.Contains(t, content, "Bob Jones")
}

// TestIdentityStoreDashboard_CreateGroup verifies the create group form flow.
func TestIdentityStoreDashboard_CreateGroup(t *testing.T) {
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
			saveScreenshot(t, page, "TestIdentityStoreDashboard_CreateGroup")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/identitystore")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Identity Store')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Switch to Groups tab.
	err = page.Locator("button:has-text('Groups')").Click()
	require.NoError(t, err)

	// Open the create group modal.
	err = page.Locator("button:has-text('+ Create Group')").Click()
	require.NoError(t, err)

	// Fill in group name.
	err = page.Locator("#create-group-modal input[name='display_name']").Fill("Platform Team")
	require.NoError(t, err)

	// Submit the form.
	err = page.Locator("button[type='submit']:has-text('Create')").Last().Click()
	require.NoError(t, err)

	// After the POST redirect the page reloads to the Users tab by default;
	// click the Groups tab again to make the Groups pane visible.
	err = page.Locator("h1:has-text('Identity Store')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	err = page.Locator("button:has-text('Groups')").Click()
	require.NoError(t, err)

	// Wait for the table to show the new group.
	err = page.Locator("td:has-text('Platform Team')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Platform Team")
}
