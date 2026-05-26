package s3control

import (
	"encoding/xml"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// ---- StorageLensGroup Tagging handlers ----

type slgTagXML struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type putSLGTaggingRequestXML struct {
	XMLName xml.Name    `xml:"PutStorageLensGroupTaggingRequest"`
	Tags    []slgTagXML `xml:"Tags>Tag"`
}

type getSLGTaggingResultXML struct {
	XMLName xml.Name    `xml:"GetStorageLensGroupTaggingResult"`
	Tags    []slgTagXML `xml:"Tags>Tag"`
}

func (h *Handler) handleGetStorageLensGroupTagging(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathStorageLensGroupPrefix),
		"/tagging",
	)

	tags, err := h.Backend.GetStorageLensGroupTags(accountID, name)
	if err != nil {
		return handleBackendError(c, err)
	}

	items := make([]slgTagXML, 0, len(tags))
	for k, v := range tags {
		items = append(items, slgTagXML{Key: k, Value: v})
	}

	return writeXML(c, getSLGTaggingResultXML{Tags: items})
}

func (h *Handler) handlePutStorageLensGroupTagging(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathStorageLensGroupPrefix),
		"/tagging",
	)

	var body putSLGTaggingRequestXML
	if err := decodeXML(c, &body); err != nil {
		return c.String(http.StatusBadRequest, "invalid request body")
	}

	tags := make(TagSet, len(body.Tags))
	for _, t := range body.Tags {
		tags[t.Key] = t.Value
	}

	if err := h.Backend.PutStorageLensGroupTags(accountID, name, tags); err != nil {
		return handleBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteStorageLensGroupTagging(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathStorageLensGroupPrefix),
		"/tagging",
	)

	if err := h.Backend.DeleteStorageLensGroupTags(accountID, name); err != nil {
		return handleBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// ---- ListAccessGrantsInstances handler ----

type agiItemXML struct {
	AccessGrantsInstanceArn      string `xml:"AccessGrantsInstanceArn,omitempty"`
	AccessGrantsInstanceID       string `xml:"AccessGrantsInstanceId,omitempty"`
	IdentityCenterArn            string `xml:"IdentityCenterArn,omitempty"`
	IdentityCenterApplicationArn string `xml:"IdentityCenterApplicationArn,omitempty"`
	CreatedAt                    string `xml:"CreatedAt,omitempty"`
}

type listAGIResultXML struct {
	XMLName   xml.Name     `xml:"ListAccessGrantsInstancesResult"`
	Instances []agiItemXML `xml:"AccessGrantsInstancesList>AccessGrantsInstance"`
}

func (h *Handler) handleListAccessGrantsInstances(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	instances := h.Backend.ListAllAccessGrantsInstances(accountID)
	items := make([]agiItemXML, 0, len(instances))

	for _, inst := range instances {
		items = append(items, agiItemXML{
			AccessGrantsInstanceArn:      inst.AccessGrantsInstanceArn,
			AccessGrantsInstanceID:       inst.AccessGrantsInstanceID,
			IdentityCenterArn:            inst.IdentityCenterArn,
			IdentityCenterApplicationArn: inst.IdentityCenterApplicationArn,
			CreatedAt:                    inst.CreatedAt,
		})
	}

	return writeXML(c, listAGIResultXML{Instances: items})
}

// ---- GetDataAccess with credentials ----

type dataAccessCredentialsXML struct {
	AccessKeyID     string `xml:"AccessKeyId"`
	SecretAccessKey string `xml:"SecretAccessKey"`
	SessionToken    string `xml:"SessionToken"`
	Expiration      string `xml:"Expiration"`
}

type getDataAccessResultXML struct {
	XMLName            xml.Name                 `xml:"GetDataAccessResult"`
	Credentials        dataAccessCredentialsXML `xml:"Credentials"`
	MatchedGrantTarget string                   `xml:"MatchedGrantTarget,omitempty"`
}

func (h *Handler) handleGetDataAccessV2(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	target := c.Request().URL.Query().Get("target")
	permission := c.Request().URL.Query().Get("permission")

	keyID, secretKey, sessionToken, expiration, matched, err :=
		h.Backend.GetDataAccessCredentials(accountID, target, permission)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, getDataAccessResultXML{
		Credentials: dataAccessCredentialsXML{
			AccessKeyID:     keyID,
			SecretAccessKey: secretKey,
			SessionToken:    sessionToken,
			Expiration:      expiration,
		},
		MatchedGrantTarget: matched,
	})
}

// ---- MRAP PublicAccessBlock in response ----

type mrapPABResponseXML struct {
	BlockPublicAcls       bool `xml:"BlockPublicAcls"`
	IgnorePublicAcls      bool `xml:"IgnorePublicAcls"`
	BlockPublicPolicy     bool `xml:"BlockPublicPolicy"`
	RestrictPublicBuckets bool `xml:"RestrictPublicBuckets"`
}

// buildMRAPPABResponse converts a PublicAccessBlock to its XML response form.
func buildMRAPPABResponse(pab *PublicAccessBlock) *mrapPABResponseXML {
	if pab == nil {
		return nil
	}

	return &mrapPABResponseXML{
		BlockPublicAcls:       pab.BlockPublicAcls,
		IgnorePublicAcls:      pab.IgnorePublicAcls,
		BlockPublicPolicy:     pab.BlockPublicPolicy,
		RestrictPublicBuckets: pab.RestrictPublicBuckets,
	}
}
