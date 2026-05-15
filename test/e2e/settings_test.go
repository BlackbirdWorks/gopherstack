//go:build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
)

type e2eConfigManager struct {
	settings dashboard.Settings
}

func (m *e2eConfigManager) GetSettings() dashboard.Settings {
	return m.settings
}

func (m *e2eConfigManager) UpdateSettings(s dashboard.Settings) {
	m.settings = s
}

func (m *e2eConfigManager) SaveConfig() error {
	return nil
}

func TestE2E_SettingsPage(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	ctx, err := browser.NewContext()
	require.NoError(t, err)
	defer ctx.Close()

	page, err := ctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	// Navigate to the settings page.
	_, err = page.Goto(server.URL + "/dashboard/settings")
	require.NoError(t, err)

	require.NoError(t, page.Locator("h1:has-text('Settings')").First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(5000),
	}))

	accountInput := page.Locator("#accountID")
	require.NoError(t, accountInput.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(5000),
	}))
	accountValue, err := accountInput.InputValue()
	require.NoError(t, err)
	assert.Equal(t, "000000000000", accountValue, "expected account ID value on settings page")

	regionInput := page.Locator("#region")
	require.NoError(t, regionInput.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(5000),
	}))
	regionValue, err := regionInput.InputValue()
	require.NoError(t, err)
	assert.Equal(t, "us-east-1", regionValue, "expected region value on settings page")
}

func TestE2E_SettingsPage_NavbarIcon(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	ctx, err := browser.NewContext()
	require.NoError(t, err)
	defer ctx.Close()

	page, err := ctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	// Navigate to the main dashboard (redirects to /dynamodb).
	_, err = page.Goto(server.URL + "/dashboard/dynamodb")
	require.NoError(t, err)

	// The settings icon link should be present in the navbar.
	settingsLink := page.Locator("#nav-settings").First()
	require.NoError(t, settingsLink.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	}))

	// Click the settings icon to navigate to the settings page.
	require.NoError(t, settingsLink.Click())

	err = page.WaitForURL("**/dashboard/settings", playwright.PageWaitForURLOptions{
		Timeout: playwright.Float(5000),
	})
	require.NoError(t, err)

	body, err := page.TextContent("body")
	require.NoError(t, err)
	assert.Contains(t, body, "Runtime configuration")

	autoPurgeInput := page.Locator("input[name='autoPurgeTTL']")
	require.NoError(t, autoPurgeInput.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(5000),
	}))

	currentValue, err := autoPurgeInput.InputValue()
	require.NoError(t, err)

	newValue := "35m"
	if currentValue == newValue {
		newValue = "36m"
	}

	require.NoError(t, autoPurgeInput.Fill(newValue))

	saveBtn := page.Locator("#save-settings-btn")
	require.NoError(t, saveBtn.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(5000),
	}))

	isDisabled, err := saveBtn.IsDisabled()
	require.NoError(t, err)
	require.False(t, isDisabled, "save button should be enabled after changing settings when arriving via navbar navigation")
}

func TestE2E_SettingsPage_SaveAutoPurgeTTL_PersistsAfterRefresh(t *testing.T) {
	t.Parallel()

	stack := newStack(t)
	stack.Dashboard.ConfigManager = &e2eConfigManager{
		settings: dashboard.Settings{
			AccountID:      "000000000000",
			Region:         "us-east-1",
			AutoPurgeTTL:   10 * time.Minute,
			JanitorTimeout: 30 * time.Second,
		},
	}

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	ctx, err := browser.NewContext()
	require.NoError(t, err)
	defer ctx.Close()

	page, err := ctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	_, err = page.Goto(server.URL + "/dashboard/settings")
	require.NoError(t, err)

	autoPurgeInput := page.Locator("input[name='autoPurgeTTL']")
	require.NoError(t, autoPurgeInput.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(5000),
	}))

	currentValue, err := autoPurgeInput.InputValue()
	require.NoError(t, err)

	newValue := "37m"
	if currentValue == newValue {
		newValue = "38m"
	}

	require.NoError(t, autoPurgeInput.Fill(newValue))

	saveBtn := page.Locator("#save-settings-btn")
	require.NoError(t, saveBtn.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(5000),
	}))

	isDisabled, err := saveBtn.IsDisabled()
	require.NoError(t, err)
	require.False(t, isDisabled, "save button should be enabled after changing autoPurgeTTL")

	require.NoError(t, saveBtn.Click())

	successToast := page.Locator("#toast-container:has-text('updated')")
	require.NoError(t, successToast.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	}))

	_, err = page.Reload()
	require.NoError(t, err)

	require.NoError(t, autoPurgeInput.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(5000),
	}))

	persistedValue, err := autoPurgeInput.InputValue()
	require.NoError(t, err)

	wantTTL, err := time.ParseDuration(newValue)
	require.NoError(t, err)

	gotTTL, err := time.ParseDuration(persistedValue)
	require.NoError(t, err)
	assert.Equal(t, wantTTL, gotTTL)
}
