package cloudfront

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// xmlEscape escapes a string for safe inclusion as XML character data.
func xmlEscape(s string) string {
	if s == "" {
		return ""
	}

	var buf bytes.Buffer
	if err := xml.EscapeText(&buf, []byte(s)); err != nil {
		return ""
	}

	return buf.String()
}

// --- Config-only ("/config") GET handlers ---
//
// AWS returns the *Config element as the document root for the Get*Config
// operations (GetKeyGroupConfig, GetPublicKeyConfig,
// GetFieldLevelEncryptionConfig, GetFieldLevelEncryptionProfileConfig), not the
// wrapping resource element. These handlers render that config-only shape.

func (h *Handler) handleGetFieldLevelEncryptionConfig(c *echo.Context, id string) error {
	fle, err := h.Backend.GetFieldLevelEncryption(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", fle.ETag)

	return xmlResp(c, http.StatusOK, fleConfigResponseXML(fle))
}

func (h *Handler) handleGetFieldLevelEncryptionProfileConfig(c *echo.Context, id string) error {
	p, err := h.Backend.GetFieldLevelEncryptionProfile(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", p.ETag)

	return xmlResp(c, http.StatusOK, fleProfileConfigResponseXML(p))
}

// publicKeyConfigResponseXML renders the config-only PublicKeyConfig root.
func publicKeyConfigResponseXML(pk *PublicKey) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<PublicKeyConfig xmlns="%s">`+
		`<CallerReference>%s</CallerReference>`+
		`<Name>%s</Name>`+
		`<EncodedKey>%s</EncodedKey>`+
		`<Comment>%s</Comment>`+
		`</PublicKeyConfig>`,
		cfNS, xmlEscape(pk.CallerReference), xmlEscape(pk.Name),
		xmlEscape(pk.EncodedKey), xmlEscape(pk.Comment))
}

func (h *Handler) handleGetPublicKeyConfig(c *echo.Context, id string) error {
	pk, err := h.Backend.GetPublicKey(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", pk.ETag)

	return xmlResp(c, http.StatusOK, publicKeyConfigResponseXML(pk))
}

// keyGroupConfigResponseXML renders the config-only KeyGroupConfig root.
func keyGroupConfigResponseXML(kg *KeyGroup) string {
	var items strings.Builder
	for _, id := range kg.Items {
		items.WriteString(`<PublicKey>`)
		items.WriteString(xmlEscape(id))
		items.WriteString(`</PublicKey>`)
	}

	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<KeyGroupConfig xmlns="%s">`+
		`<Name>%s</Name>`+
		`<Comment>%s</Comment>`+
		`<Items>%s</Items>`+
		`</KeyGroupConfig>`,
		cfNS, xmlEscape(kg.Name), xmlEscape(kg.Comment), items.String())
}

func (h *Handler) handleGetKeyGroupConfig(c *echo.Context, id string) error {
	kg, err := h.Backend.GetKeyGroup(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", kg.ETag)

	return xmlResp(c, http.StatusOK, keyGroupConfigResponseXML(kg))
}
