package dashboard

import (
	"encoding/base64"
	"net/http"

	"github.com/labstack/echo/v5"

	kmsbackend "github.com/blackbirdworks/gopherstack/kms"
)

// kmsKeyDetailData is the template data for the KMS key detail page.
type kmsKeyDetailData struct {
	PageData

	KeyID       string
	Arn         string
	Description string
	KeyState    string
	KeyUsage    string
	Aliases     []kmsbackend.Alias
	Rotation    bool
}

// kmsEncryptResultData is the template data for the KMS encrypt result fragment.
type kmsEncryptResultData struct {
	KeyID      string
	Ciphertext string
}

// kmsDecryptResultData is the template data for the KMS decrypt result fragment.
type kmsDecryptResultData struct {
	KeyID     string
	Plaintext string
}

// kmsIndex renders the list of all KMS keys.
func (h *DashboardHandler) kmsIndex(c *echo.Context) error {
	w := c.Response()

	data := struct {
		PageData

		Keys []any
	}{
		PageData: PageData{
			Title:     "KMS Keys",
			ActiveTab: "kms",
		},
		Keys: make([]any, 0),
	}

	if h.KMSOps != nil {
		out, err := h.KMSOps.Backend.ListKeys(&kmsbackend.ListKeysInput{})
		if err == nil {
			for _, k := range out.Keys {
				data.Keys = append(data.Keys, k)
			}
		}
	}

	h.renderTemplate(w, "kms/index.html", data)

	return nil
}

// kmsCreateKey handles creating a new KMS key.
func (h *DashboardHandler) kmsCreateKey(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	if h.KMSOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := r.ParseForm(); err != nil {
		return c.String(http.StatusBadRequest, "Invalid request")
	}

	desc := r.FormValue("description")

	if _, err := h.KMSOps.Backend.CreateKey(&kmsbackend.CreateKeyInput{Description: desc}); err != nil {
		h.Logger.Error("Failed to create KMS key", "error", err)
		return c.String(http.StatusInternalServerError, "Failed to create key: "+err.Error())
	}

	w.Header().Set("Hx-Redirect", "/dashboard/kms")

	return c.NoContent(http.StatusOK)
}

// kmsKeyDetail renders the detail view for a specific KMS key.
func (h *DashboardHandler) kmsKeyDetail(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	if h.KMSOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	keyID := r.URL.Query().Get("id")
	if keyID == "" {
		return c.String(http.StatusBadRequest, "Missing id")
	}

	descOut, err := h.KMSOps.Backend.DescribeKey(&kmsbackend.DescribeKeyInput{KeyID: keyID})
	if err != nil {
		h.Logger.Error("Failed to describe KMS key", "keyID", keyID, "error", err)
		return c.String(http.StatusNotFound, "Key not found")
	}

	var aliases []kmsbackend.Alias

	aliasOut, err := h.KMSOps.Backend.ListAliases(&kmsbackend.ListAliasesInput{KeyID: keyID})
	if err == nil {
		aliases = aliasOut.Aliases
	}

	var rotationEnabled bool

	rotOut, err := h.KMSOps.Backend.GetKeyRotationStatus(&kmsbackend.GetKeyRotationStatusInput{KeyID: keyID})
	if err == nil {
		rotationEnabled = rotOut.KeyRotationEnabled
	}

	data := kmsKeyDetailData{
		PageData: PageData{
			Title:     "KMS Key Detail",
			ActiveTab: "kms",
		},
		KeyID:       descOut.KeyMetadata.KeyID,
		Arn:         descOut.KeyMetadata.Arn,
		Description: descOut.KeyMetadata.Description,
		KeyState:    descOut.KeyMetadata.KeyState,
		KeyUsage:    descOut.KeyMetadata.KeyUsage,
		Aliases:     aliases,
		Rotation:    rotationEnabled,
	}

	h.renderTemplate(w, "kms/key_detail.html", data)

	return nil
}

// kmsEncrypt handles encrypting plaintext with a KMS key and returns an HTMX fragment.
func (h *DashboardHandler) kmsEncrypt(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	if h.KMSOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	keyID := r.URL.Query().Get("id")
	if keyID == "" {
		return c.String(http.StatusBadRequest, "Missing id")
	}

	if err := r.ParseForm(); err != nil {
		return c.String(http.StatusBadRequest, "Invalid request")
	}

	plaintext := r.FormValue("plaintext")

	out, err := h.KMSOps.Backend.Encrypt(&kmsbackend.EncryptInput{
		KeyID:     keyID,
		Plaintext: []byte(plaintext),
	})
	if err != nil {
		h.Logger.Error("Failed to encrypt with KMS key", "keyID", keyID, "error", err)
		return c.String(http.StatusInternalServerError, "Encryption failed: "+err.Error())
	}

	h.renderFragment(w, "kms_encrypt_result", kmsEncryptResultData{
		KeyID:      out.KeyID,
		Ciphertext: base64.StdEncoding.EncodeToString(out.CiphertextBlob),
	})

	return nil
}

// kmsDecrypt handles decrypting ciphertext with a KMS key and returns an HTMX fragment.
func (h *DashboardHandler) kmsDecrypt(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	if h.KMSOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := r.ParseForm(); err != nil {
		return c.String(http.StatusBadRequest, "Invalid request")
	}

	ciphertextB64 := r.FormValue("ciphertext")
	keyID := r.FormValue("key_id")

	decoded, err := base64.StdEncoding.DecodeString(ciphertextB64)
	if err != nil {
		return c.String(http.StatusBadRequest, "Invalid base64 ciphertext")
	}

	out, err := h.KMSOps.Backend.Decrypt(&kmsbackend.DecryptInput{
		KeyID:          keyID,
		CiphertextBlob: decoded,
	})
	if err != nil {
		h.Logger.Error("Failed to decrypt with KMS key", "error", err)
		return c.String(http.StatusInternalServerError, "Decryption failed: "+err.Error())
	}

	h.renderFragment(w, "kms_decrypt_result", kmsDecryptResultData{
		KeyID:     out.KeyID,
		Plaintext: string(out.Plaintext),
	})

	return nil
}
