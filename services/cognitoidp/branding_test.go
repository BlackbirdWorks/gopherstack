package cognitoidp_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUICustomization_SetGet_WithImageUrl(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "ui-custom-pool")

	rec := doCognitoRequest(t, h, "SetUICustomization", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
		"CSS":        ".banner { background: blue; }",
		"ImageData":  "https://example.com/logo.png",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var setOut struct {
		UICustomization *struct {
			UserPoolID       string  `json:"UserPoolId,omitempty"`
			ClientID         string  `json:"ClientId,omitempty"`
			CSS              string  `json:"CSS,omitempty"`
			ImageURL         string  `json:"ImageUrl,omitempty"`
			CreationDate     float64 `json:"CreationDate,omitempty"`
			LastModifiedDate float64 `json:"LastModifiedDate,omitempty"`
		} `json:"UICustomization"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &setOut))
	require.NotNil(t, setOut.UICustomization)
	assert.Equal(t, poolID, setOut.UICustomization.UserPoolID)
	assert.Equal(t, clientID, setOut.UICustomization.ClientID)
	assert.Equal(t, ".banner { background: blue; }", setOut.UICustomization.CSS)
	assert.Equal(t, "https://example.com/logo.png", setOut.UICustomization.ImageURL)
	assert.Greater(t, setOut.UICustomization.CreationDate, float64(0))
	assert.Greater(t, setOut.UICustomization.LastModifiedDate, float64(0))

	// Get and verify.
	rec = doCognitoRequest(t, h, "GetUICustomization", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var getOut struct {
		UICustomization *struct {
			CSS              string  `json:"CSS,omitempty"`
			ImageURL         string  `json:"ImageUrl,omitempty"`
			LastModifiedDate float64 `json:"LastModifiedDate,omitempty"`
		} `json:"UICustomization"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getOut))
	require.NotNil(t, getOut.UICustomization)
	assert.Equal(t, ".banner { background: blue; }", getOut.UICustomization.CSS)
	assert.Equal(t, "https://example.com/logo.png", getOut.UICustomization.ImageURL)
	assert.Greater(t, getOut.UICustomization.LastModifiedDate, float64(0))
}

func TestUICustomization_Get_Empty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "ui-empty-pool")

	rec := doCognitoRequest(t, h, "GetUICustomization", map[string]any{
		"UserPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		UICustomization *struct {
			UserPoolID string `json:"UserPoolId,omitempty"`
			CSS        string `json:"CSS,omitempty"`
		} `json:"UICustomization"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotNil(t, out.UICustomization)
	assert.Empty(t, out.UICustomization.CSS)
}

func TestUICustomization_InvalidPool(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doCognitoRequest(t, h, "GetUICustomization", map[string]any{
		"UserPoolId": "bad-pool",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec = doCognitoRequest(t, h, "SetUICustomization", map[string]any{
		"UserPoolId": "bad-pool",
		"CSS":        ".foo {}",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestUICustomization_Backend_Direct(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("ui-backend-pool")
	require.NoError(t, err)
	client, err := b.CreateUserPoolClient(pool.ID, "ui-client")
	require.NoError(t, err)

	ui, err := b.SetUICustomizationFull(
		pool.ID,
		client.ClientID,
		".body { color: red; }",
		"https://img.example.com/logo.png",
	)
	require.NoError(t, err)
	assert.Equal(t, ".body { color: red; }", ui.CSS)
	assert.Equal(t, "https://img.example.com/logo.png", ui.ImageURL)
	assert.False(t, ui.CreatedAt.IsZero())
	assert.False(t, ui.LastModifiedAt.IsZero())

	got, err := b.GetUICustomizationFull(pool.ID, client.ClientID)
	require.NoError(t, err)
	assert.Equal(t, ui.CSS, got.CSS)
	assert.Equal(t, ui.ImageURL, got.ImageURL)
}

func TestUICustomization(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "ui-custom-pool")

	// Set
	rec := doCognitoRequest(t, h, "SetUICustomization", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
		"CSS":        ".logo { color: red; }",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get
	rec = doCognitoRequest(t, h, "GetUICustomization", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		UICustomization struct {
			CSS string `json:"CSS,omitempty"`
		} `json:"UICustomization"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, ".logo { color: red; }", resp.UICustomization.CSS)
}

func TestManagedLoginBranding_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "mlb-crud-pool")

	// Create
	rec := doCognitoRequest(t, h, "CreateManagedLoginBranding", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp struct {
		ManagedLoginBranding struct {
			ManagedLoginBrandingID string `json:"ManagedLoginBrandingId,omitempty"`
		} `json:"ManagedLoginBranding"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	brandingID := createResp.ManagedLoginBranding.ManagedLoginBrandingID
	require.NotEmpty(t, brandingID)

	// Describe by ID
	rec = doCognitoRequest(t, h, "DescribeManagedLoginBranding", map[string]any{
		"UserPoolId":             poolID,
		"ManagedLoginBrandingId": brandingID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe by client
	rec = doCognitoRequest(t, h, "DescribeManagedLoginBrandingByClient", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Update
	rec = doCognitoRequest(t, h, "UpdateManagedLoginBranding", map[string]any{
		"UserPoolId":             poolID,
		"ManagedLoginBrandingId": brandingID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = doCognitoRequest(t, h, "DeleteManagedLoginBranding", map[string]any{
		"UserPoolId":             poolID,
		"ManagedLoginBrandingId": brandingID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe after delete — should error
	rec = doCognitoRequest(t, h, "DescribeManagedLoginBranding", map[string]any{
		"UserPoolId":             poolID,
		"ManagedLoginBrandingId": brandingID,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

type managedLoginBrandingWire struct {
	ManagedLoginBrandingID   string           `json:"ManagedLoginBrandingId,omitempty"`
	UserPoolID               string           `json:"UserPoolId,omitempty"`
	Settings                 map[string]any   `json:"Settings,omitempty"`
	Assets                   []map[string]any `json:"Assets,omitempty"`
	UseCognitoProvidedValues bool             `json:"UseCognitoProvidedValues,omitempty"`
	CreationDate             float64          `json:"CreationDate,omitempty"`
	LastModifiedDate         float64          `json:"LastModifiedDate,omitempty"`
}

func TestManagedLoginBranding_SettingsAndAssets_AcceptedAndEchoed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "mlb-settings-pool")

	settings := map[string]any{"componentClasses": map[string]any{"primaryButton": map[string]any{"lightMode": "blue"}}}
	assets := []any{map[string]any{
		"Category":  "PAGE_BACKGROUND",
		"ColorMode": "LIGHT",
		"Extension": "PNG",
		"Bytes":     "aGVsbG8=",
	}}

	rec := doCognitoRequest(t, h, "CreateManagedLoginBranding", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
		"Settings":   settings,
		"Assets":     assets,
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var createResp struct {
		ManagedLoginBranding managedLoginBrandingWire `json:"ManagedLoginBranding"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	componentClasses, ok := createResp.ManagedLoginBranding.Settings["componentClasses"].(map[string]any)
	require.True(t, ok)
	primaryButton, ok := componentClasses["primaryButton"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "blue", primaryButton["lightMode"],
		"Settings is a required accepted field and must be echoed back verbatim",
	)
	require.Len(t, createResp.ManagedLoginBranding.Assets, 1)
	assert.Equal(t, "PAGE_BACKGROUND", createResp.ManagedLoginBranding.Assets[0]["Category"])
	assert.NotZero(t, createResp.ManagedLoginBranding.CreationDate)

	brandingID := createResp.ManagedLoginBranding.ManagedLoginBrandingID

	// Update with UseCognitoProvidedValues only: Settings/Assets omitted must be retained.
	updRec := doCognitoRequest(t, h, "UpdateManagedLoginBranding", map[string]any{
		"UserPoolId":               poolID,
		"ManagedLoginBrandingId":   brandingID,
		"UseCognitoProvidedValues": true,
	})
	require.Equal(t, http.StatusOK, updRec.Code, updRec.Body.String())

	var updResp struct {
		ManagedLoginBranding managedLoginBrandingWire `json:"ManagedLoginBranding"`
	}
	require.NoError(t, json.Unmarshal(updRec.Body.Bytes(), &updResp))
	assert.True(t, updResp.ManagedLoginBranding.UseCognitoProvidedValues)
	assert.NotEmpty(t, updResp.ManagedLoginBranding.Settings, "omitting Settings on update must not clear it")
}
