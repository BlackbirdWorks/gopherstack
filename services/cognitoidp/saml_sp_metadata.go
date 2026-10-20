package cognitoidp

import (
	"encoding/base64"
	"html"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"
)

const mimeSAMLMetadata = "application/samlmetadata+xml"

// poolSignsRequests reports whether any SAML provider of the pool signs the SP's messages.
func (b *InMemoryBackend) poolSignsRequests(poolID string) bool {
	for _, idp := range b.samlProviders(poolID) {
		if idp.ProviderDetails[detailRequestSigning] == signAlgRSASHA256 {
			return true
		}
	}

	return false
}

func spKeyDescriptor(use string, certDER []byte) string {
	return `<md:KeyDescriptor use="` + use + `"><ds:KeyInfo xmlns:ds="` + nsDSig + `"><ds:X509Data>` +
		`<ds:X509Certificate>` + base64.StdEncoding.EncodeToString(certDER) +
		`</ds:X509Certificate></ds:X509Data></ds:KeyInfo></md:KeyDescriptor>`
}

func sloService(binding, base string) string {
	return `<md:SingleLogoutService Binding="` + binding + `" Location="` +
		html.EscapeString(base+pathSAMLLogout) + `"/>`
}

// buildSPMetadata renders the pool's SP metadata: signing and encryption certificate, ACS and SLO endpoints.
func buildSPMetadata(poolID, base string, certDER []byte, signsRequests bool) string {
	return `<md:EntityDescriptor xmlns:md="` + nsSAMLMetadata + `" entityID="` + samlSPPrefix + poolID + `">` +
		`<md:SPSSODescriptor AuthnRequestsSigned="` + strconv.FormatBool(signsRequests) +
		`" WantAssertionsSigned="true" protocolSupportEnumeration="` + nsSAMLProtocol + `">` +
		spKeyDescriptor("signing", certDER) + spKeyDescriptor("encryption", certDER) +
		sloService(bindingRedirect, base) + sloService(bindingPost, base) +
		`<md:AssertionConsumerService Binding="` + bindingPost + `" Location="` +
		html.EscapeString(base+pathSAMLIDPResponse) + `" index="0" isDefault="true"/>` +
		`</md:SPSSODescriptor></md:EntityDescriptor>`
}

func (h *Handler) handleSAMLMetadata(c *echo.Context) error {
	poolID := poolPathPrefix(c.Request().URL.Path, pathSAMLMetadata)
	if poolID == "" {
		poolID = h.hostPool(c)
	}

	sp, err := h.Backend.spIdentity(poolID)
	if poolID == "" || err != nil {
		return renderErrorPage(c, http.StatusNotFound, errInvalidRequest, "user pool not found")
	}

	doc := buildSPMetadata(poolID, samlBaseURL(c.Request()), sp.certDER, h.Backend.poolSignsRequests(poolID))

	return c.Blob(http.StatusOK, mimeSAMLMetadata, []byte(doc))
}
