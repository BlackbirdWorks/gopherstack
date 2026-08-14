package route53

import (
	"encoding/xml"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

type xmlKSK struct {
	XMLName                  xml.Name `xml:"KeySigningKey"`
	Name                     string   `xml:"Name"`
	KMSArn                   string   `xml:"KmsArn,omitempty"`
	Status                   string   `xml:"Status"`
	SigningAlgorithmMnemonic string   `xml:"SigningAlgorithmMnemonic,omitempty"`
	DigestAlgorithmMnemonic  string   `xml:"DigestAlgorithmMnemonic,omitempty"`
	PublicKey                string   `xml:"PublicKey,omitempty"`
	DSRecord                 string   `xml:"DSRecord,omitempty"`
	DigestValue              string   `xml:"DigestValue,omitempty"`
	Flag                     int      `xml:"Flag,omitempty"`
	SigningAlgorithmType     int      `xml:"SigningAlgorithmType,omitempty"`
	DigestAlgorithmType      int      `xml:"DigestAlgorithmType,omitempty"`
	KeyTag                   int      `xml:"KeyTag,omitempty"`
}

// xmlKSKMember mirrors xmlKSK's fields but without a struct-level XMLName,
// for use as a list item under a parent-owned element name. xmlKSK's own
// XMLName tag would otherwise silently override the parent field's tag
// (route53@v1.65.6 deserializers.go: awsRestxml_deserializeDocumentKeySigningKeys
// reads each item as "member", not "KeySigningKey").
type xmlKSKMember struct {
	Name                     string `xml:"Name"`
	KMSArn                   string `xml:"KmsArn,omitempty"`
	Status                   string `xml:"Status"`
	SigningAlgorithmMnemonic string `xml:"SigningAlgorithmMnemonic,omitempty"`
	DigestAlgorithmMnemonic  string `xml:"DigestAlgorithmMnemonic,omitempty"`
	PublicKey                string `xml:"PublicKey,omitempty"`
	DSRecord                 string `xml:"DSRecord,omitempty"`
	DigestValue              string `xml:"DigestValue,omitempty"`
	Flag                     int    `xml:"Flag,omitempty"`
	SigningAlgorithmType     int    `xml:"SigningAlgorithmType,omitempty"`
	DigestAlgorithmType      int    `xml:"DigestAlgorithmType,omitempty"`
	KeyTag                   int    `xml:"KeyTag,omitempty"`
}

type xmlCreateKSKRequest struct {
	XMLName                 xml.Name `xml:"CreateKeySigningKeyRequest"`
	HostedZoneID            string   `xml:"HostedZoneId"`
	CallerReference         string   `xml:"CallerReference"`
	Name                    string   `xml:"Name"`
	KeyManagementServiceArn string   `xml:"KeyManagementServiceArn,omitempty"`
	Status                  string   `xml:"Status,omitempty"`
}

type xmlCreateKSKResponse struct {
	XMLName       xml.Name      `xml:"CreateKeySigningKeyResponse"`
	Xmlns         string        `xml:"xmlns,attr"`
	ChangeInfo    xmlChangeInfo `xml:"ChangeInfo"`
	KeySigningKey xmlKSK        `xml:"KeySigningKey"`
}

type xmlActivateKSKResponse struct {
	XMLName    xml.Name      `xml:"ActivateKeySigningKeyResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	ChangeInfo xmlChangeInfo `xml:"ChangeInfo"`
}

func (h *Handler) routeKSKRoot(c *echo.Context, method string) error {
	if method == http.MethodPost {
		return h.createKeySigningKey(c)
	}

	return xmlError(
		c,
		http.StatusNotFound,
		"NoSuchOperation",
		"unsupported method on /keysigningkey",
	)
}

func (h *Handler) routeKSK(c *echo.Context, path, method string) error {
	if strings.HasSuffix(path, route53ActivateSuffix) && method == http.MethodPost {
		return h.activateKeySigningKey(c, path)
	}

	if strings.HasSuffix(path, route53DeactivateSuffix) && method == http.MethodPost {
		return h.deactivateKeySigningKey(c, path)
	}

	if method == http.MethodDelete {
		return h.deleteKeySigningKey(c, path)
	}

	return xmlError(
		c,
		http.StatusNotFound,
		"NoSuchOperation",
		"unsupported operation on key signing key",
	)
}

func toXMLKSKMember(ksk *KeySigningKey) xmlKSKMember {
	return xmlKSKMember{
		Name:                     ksk.Name,
		KMSArn:                   ksk.KeyManagementServiceArn,
		Status:                   ksk.Status,
		Flag:                     ksk.Flag,
		SigningAlgorithmMnemonic: ksk.SigningAlgorithmMnemonic,
		SigningAlgorithmType:     ksk.SigningAlgorithmType,
		DigestAlgorithmMnemonic:  ksk.DigestAlgorithmMnemonic,
		DigestAlgorithmType:      ksk.DigestAlgorithmType,
		KeyTag:                   ksk.KeyTag,
		PublicKey:                ksk.PublicKey,
		DigestValue:              ksk.DigestValue,
		DSRecord:                 ksk.DSRecord,
	}
}

func toXMLKSK(ksk *KeySigningKey) xmlKSK {
	m := toXMLKSKMember(ksk)

	return xmlKSK{
		Name:                     m.Name,
		KMSArn:                   m.KMSArn,
		Status:                   m.Status,
		SigningAlgorithmMnemonic: m.SigningAlgorithmMnemonic,
		DigestAlgorithmMnemonic:  m.DigestAlgorithmMnemonic,
		PublicKey:                m.PublicKey,
		DSRecord:                 m.DSRecord,
		DigestValue:              m.DigestValue,
		Flag:                     m.Flag,
		SigningAlgorithmType:     m.SigningAlgorithmType,
		DigestAlgorithmType:      m.DigestAlgorithmType,
		KeyTag:                   m.KeyTag,
	}
}

func (h *Handler) createKeySigningKey(c *echo.Context) error {
	ctx := c.Request().Context()

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req xmlCreateKSKRequest
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(
			c,
			http.StatusBadRequest,
			"InvalidInput",
			"failed to parse XML: "+err.Error(),
		)
	}

	ksk, err := h.Backend.CreateKeySigningKey(
		req.HostedZoneID,
		req.CallerReference,
		req.Name,
		req.KeyManagementServiceArn,
		req.Status,
	)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 CreateKeySigningKey", "name", ksk.Name)

	resp := xmlCreateKSKResponse{
		Xmlns:         route53Namespace,
		KeySigningKey: toXMLKSK(ksk),
		ChangeInfo: xmlChangeInfo{
			ID:          "/change/C" + ksk.HostedZoneID,
			Status:      statusInsync,
			SubmittedAt: time.Now(),
		},
	}

	c.Response().
		Header().
		Set("Location", "/2013-04-01/keysigningkey/"+ksk.HostedZoneID+"/"+ksk.Name)

	return writeXML(c, http.StatusCreated, resp)
}

func (h *Handler) activateKeySigningKey(c *echo.Context, path string) error {
	ctx := c.Request().Context()
	// path is /2013-04-01/keysigningkey/{HostedZoneId}/{Name}/activate
	withoutSuffix := strings.TrimSuffix(path, route53ActivateSuffix)
	rest := strings.TrimPrefix(withoutSuffix, route53KSKPrefix)
	parts := strings.SplitN(rest, "/", zoneIDAndRest)

	if len(parts) < zoneIDAndRest {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "invalid key signing key path")
	}

	hostedZoneID := parts[0]
	name := parts[1]

	if _, err := h.Backend.ActivateKeySigningKey(hostedZoneID, name); err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).
		DebugContext(ctx, "Route53 ActivateKeySigningKey", "name", name, "zoneID", hostedZoneID)

	return writeXML(c, http.StatusOK, xmlActivateKSKResponse{
		Xmlns: route53Namespace,
		ChangeInfo: xmlChangeInfo{
			ID:          "/change/C" + hostedZoneID,
			Status:      statusInsync,
			SubmittedAt: time.Now(),
		},
	})
}

func (h *Handler) deactivateKeySigningKey(c *echo.Context, path string) error {
	ctx := c.Request().Context()

	// path: /2013-04-01/keysigningkey/{zoneId}/{name}/deactivate
	rest := strings.TrimPrefix(path, route53KSKPrefix)
	rest = strings.TrimSuffix(rest, route53DeactivateSuffix)
	parts := strings.SplitN(rest, "/", zoneIDAndRest)

	if len(parts) != zoneIDAndRest {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "invalid KSK deactivate path")
	}

	zoneID, name := parts[0], parts[1]

	if _, err := h.Backend.DeactivateKeySigningKey(zoneID, name); err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).
		DebugContext(ctx, "Route53 DeactivateKeySigningKey", "zoneID", zoneID, "name", name)

	resp := struct {
		XMLName    xml.Name      `xml:"DeactivateKeySigningKeyResponse"`
		Xmlns      string        `xml:"xmlns,attr"`
		ChangeInfo xmlChangeInfo `xml:"ChangeInfo"`
	}{
		Xmlns: route53Namespace,
		ChangeInfo: xmlChangeInfo{
			ID:          "/change/deactivate-ksk-" + zoneID + "-" + name,
			Status:      statusInsync,
			SubmittedAt: time.Now(),
		},
	}

	return writeXML(c, http.StatusOK, resp)
}

func (h *Handler) deleteKeySigningKey(c *echo.Context, path string) error {
	ctx := c.Request().Context()

	// path: /2013-04-01/keysigningkey/{zoneId}/{name}
	rest := strings.TrimPrefix(path, route53KSKPrefix)
	parts := strings.SplitN(rest, "/", zoneIDAndRest)

	if len(parts) != zoneIDAndRest {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "invalid KSK delete path")
	}

	zoneID, name := parts[0], parts[1]

	if err := h.Backend.DeleteKeySigningKey(zoneID, name); err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).
		DebugContext(ctx, "Route53 DeleteKeySigningKey", "zoneID", zoneID, "name", name)

	resp := struct {
		XMLName    xml.Name      `xml:"DeleteKeySigningKeyResponse"`
		Xmlns      string        `xml:"xmlns,attr"`
		ChangeInfo xmlChangeInfo `xml:"ChangeInfo"`
	}{
		Xmlns: route53Namespace,
		ChangeInfo: xmlChangeInfo{
			ID:          "/change/delete-ksk-" + zoneID + "-" + name,
			Status:      statusInsync,
			SubmittedAt: time.Now(),
		},
	}

	return writeXML(c, http.StatusOK, resp)
}
