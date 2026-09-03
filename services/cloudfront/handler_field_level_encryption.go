package cloudfront

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// fleConfigRequestXML parses a FieldLevelEncryptionConfig request body.
type fleConfigRequestXML struct {
	XMLName          xml.Name `xml:"FieldLevelEncryptionConfig"`
	CallerReference  string   `xml:"CallerReference"`
	Comment          string   `xml:"Comment"`
	QueryArgProfiles []struct {
		QueryArg  string `xml:"QueryArg"`
		ProfileID string `xml:"ProfileId"`
	} `xml:"QueryArgProfileConfig>QueryArgProfiles>Items>QueryArgProfile"`
	ForwardWhenQueryArgProfileIsUnknown bool `xml:"QueryArgProfileConfig>ForwardWhenQueryArgProfileIsUnknown"`
}

// toBackend converts the parsed request into backend query-arg profiles.
func (r fleConfigRequestXML) toBackend() []FLEQueryArgProfile {
	if len(r.QueryArgProfiles) == 0 {
		return nil
	}

	out := make([]FLEQueryArgProfile, 0, len(r.QueryArgProfiles))
	for _, p := range r.QueryArgProfiles {
		out = append(out, FLEQueryArgProfile{QueryArg: p.QueryArg, ProfileID: p.ProfileID})
	}

	return out
}

// fleConfigInnerXML renders the inner fields of a FieldLevelEncryptionConfig
// element (without the enclosing element itself), so it can be reused for both
// the wrapped and config-only responses.
func fleConfigInnerXML(fle *FieldLevelEncryption) string {
	var items strings.Builder
	for _, p := range fle.QueryArgProfiles {
		items.WriteString(`<QueryArgProfile><QueryArg>`)
		items.WriteString(xmlEscape(p.QueryArg))
		items.WriteString(`</QueryArg><ProfileId>`)
		items.WriteString(xmlEscape(p.ProfileID))
		items.WriteString(`</ProfileId></QueryArgProfile>`)
	}

	return fmt.Sprintf(`<CallerReference>%s</CallerReference>`+
		`<Comment>%s</Comment>`+
		`<QueryArgProfileConfig>`+
		`<ForwardWhenQueryArgProfileIsUnknown>%t</ForwardWhenQueryArgProfileIsUnknown>`+
		`<QueryArgProfiles><Quantity>%d</Quantity><Items>%s</Items></QueryArgProfiles>`+
		`</QueryArgProfileConfig>`,
		xmlEscape(fle.Name), xmlEscape(fle.Comment),
		fle.ForwardWhenQueryArgProfileIsUnknown, len(fle.QueryArgProfiles), items.String())
}

func fleResponseXML(fle *FieldLevelEncryption) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<FieldLevelEncryption xmlns="%s"><Id>%s</Id>`+
		`<FieldLevelEncryptionConfig>%s</FieldLevelEncryptionConfig>`+
		`</FieldLevelEncryption>`,
		cfNS, fle.ID, fleConfigInnerXML(fle))
}

// fleConfigResponseXML renders the config-only response (root =
// FieldLevelEncryptionConfig) returned by GetFieldLevelEncryptionConfig.
func fleConfigResponseXML(fle *FieldLevelEncryption) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<FieldLevelEncryptionConfig xmlns="%s">%s</FieldLevelEncryptionConfig>`,
		cfNS, fleConfigInnerXML(fle))
}

func (h *Handler) handleCreateFieldLevelEncryption(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req fleConfigRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid FieldLevelEncryptionConfig XML"),
			)
		}
	}

	name := req.CallerReference
	if name == "" {
		name = generateID()
	}

	fle, createErr := h.Backend.CreateFieldLevelEncryption(name, req.Comment, req.toBackend())
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("ETag", fle.ETag)
	c.Response().Header().Set("Location", cfPathPrefix+"field-level-encryption/"+fle.ID)

	return xmlResp(c, http.StatusCreated, fleResponseXML(fle))
}

func (h *Handler) handleGetFieldLevelEncryption(c *echo.Context, id string) error {
	fle, err := h.Backend.GetFieldLevelEncryption(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", fle.ETag)

	return xmlResp(c, http.StatusOK, fleResponseXML(fle))
}

// handleListFieldLevelEncryptions implements ListFieldLevelEncryptionConfigs, paginated via
// Marker/MaxItems (both query-bound, cloudfront@v1.67.4 serializers.go). Real
// FieldLevelEncryptionList has no IsTruncated field -- NextMarker's presence alone signals
// truncation (types/types.go:3054-3074).
func (h *Handler) handleListFieldLevelEncryptions(c *echo.Context) error {
	items := h.Backend.ListFieldLevelEncryptions()

	page, pageSize, _, nextMarker := paginateByMarkerID(
		c,
		items,
		func(fle *FieldLevelEncryption) string { return fle.ID },
	)

	type queryArgProfileXML struct {
		QueryArg  string `xml:"QueryArg"`
		ProfileID string `xml:"ProfileId"`
	}

	type queryArgProfileConfigXML struct {
		Items              []queryArgProfileXML `xml:"QueryArgProfiles>Items>QueryArgProfile"`
		Quantity           int                  `xml:"QueryArgProfiles>Quantity"`
		ForwardWhenUnknown bool                 `xml:"ForwardWhenQueryArgProfileIsUnknown"`
	}

	type fleSummaryXML struct {
		XMLName               xml.Name                 `xml:"FieldLevelEncryptionSummary"`
		ID                    string                   `xml:"Id"`
		Comment               string                   `xml:"Comment"`
		QueryArgProfileConfig queryArgProfileConfigXML `xml:"QueryArgProfileConfig"`
	}

	type fleListXML struct {
		XMLName    xml.Name        `xml:"FieldLevelEncryptionList"`
		XMLNS      string          `xml:"xmlns,attr"`
		NextMarker string          `xml:"NextMarker,omitempty"`
		Items      []fleSummaryXML `xml:"Items>FieldLevelEncryptionSummary"`
		MaxItems   int             `xml:"MaxItems"`
		Quantity   int             `xml:"Quantity"`
	}

	summaries := make([]fleSummaryXML, 0, len(page))
	for _, fle := range page {
		items := make([]queryArgProfileXML, 0, len(fle.QueryArgProfiles))
		for _, p := range fle.QueryArgProfiles {
			items = append(items, queryArgProfileXML(p))
		}
		summaries = append(summaries, fleSummaryXML{
			ID:      fle.ID,
			Comment: fle.Comment,
			QueryArgProfileConfig: queryArgProfileConfigXML{
				ForwardWhenUnknown: fle.ForwardWhenQueryArgProfileIsUnknown,
				Items:              items,
				Quantity:           len(items),
			},
		})
	}

	list := fleListXML{
		XMLNS:      cfNS,
		NextMarker: nextMarker,
		MaxItems:   pageSize,
		Quantity:   len(summaries),
		Items:      summaries,
	}

	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

//nolint:dupl // update FLE and FLE profile share structure but operate on distinct resource types
func (h *Handler) handleUpdateFieldLevelEncryption(c *echo.Context, id string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req fleConfigRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid FieldLevelEncryptionConfig XML"),
			)
		}
	}

	current, getErr := h.Backend.GetFieldLevelEncryption(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	name := req.CallerReference
	if name == "" {
		name = current.Name
	}

	fle, updateErr := h.Backend.UpdateFieldLevelEncryption(id, name, req.Comment, req.toBackend())
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", fle.ETag)

	return xmlResp(c, http.StatusOK, fleResponseXML(fle))
}

func (h *Handler) handleDeleteFieldLevelEncryption(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetFieldLevelEncryption(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed,
			cfErrorXML("PreconditionFailed", "If-Match ETag did not match the current FieldLevelEncryption ETag"))
	}

	if err := h.Backend.DeleteFieldLevelEncryption(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- Field Level Encryption Profile handlers ---

// fleProfileConfigRequestXML parses a FieldLevelEncryptionProfileConfig body.
type fleProfileConfigRequestXML struct {
	XMLName            xml.Name `xml:"FieldLevelEncryptionProfileConfig"`
	Name               string   `xml:"Name"`
	CallerReference    string   `xml:"CallerReference"`
	Comment            string   `xml:"Comment"`
	EncryptionEntities []struct {
		PublicKeyID   string   `xml:"PublicKeyId"`
		ProviderID    string   `xml:"ProviderId"`
		FieldPatterns []string `xml:"FieldPatterns>Items>FieldPattern"`
	} `xml:"EncryptionEntities>Items>EncryptionEntity"`
}

// toBackend converts the parsed request into backend encryption entities.
func (r fleProfileConfigRequestXML) toBackend() []EncryptionEntity {
	if len(r.EncryptionEntities) == 0 {
		return nil
	}

	out := make([]EncryptionEntity, 0, len(r.EncryptionEntities))
	for _, e := range r.EncryptionEntities {
		out = append(out, EncryptionEntity{
			PublicKeyID:   e.PublicKeyID,
			ProviderID:    e.ProviderID,
			FieldPatterns: append([]string(nil), e.FieldPatterns...),
		})
	}

	return out
}

// fleProfileConfigInnerXML renders the inner fields of a
// FieldLevelEncryptionProfileConfig element.
func fleProfileConfigInnerXML(p *FieldLevelEncryptionProfile) string {
	var entities strings.Builder
	for _, e := range p.EncryptionEntities {
		var patterns strings.Builder
		for _, fp := range e.FieldPatterns {
			patterns.WriteString(`<FieldPattern>`)
			patterns.WriteString(xmlEscape(fp))
			patterns.WriteString(`</FieldPattern>`)
		}

		fmt.Fprintf(&entities, `<EncryptionEntity>`+
			`<PublicKeyId>%s</PublicKeyId>`+
			`<ProviderId>%s</ProviderId>`+
			`<FieldPatterns><Quantity>%d</Quantity><Items>%s</Items></FieldPatterns>`+
			`</EncryptionEntity>`,
			xmlEscape(e.PublicKeyID), xmlEscape(e.ProviderID),
			len(e.FieldPatterns), patterns.String())
	}

	return fmt.Sprintf(`<Name>%s</Name>`+
		`<Comment>%s</Comment>`+
		`<EncryptionEntities><Quantity>%d</Quantity><Items>%s</Items></EncryptionEntities>`,
		xmlEscape(p.Name), xmlEscape(p.Comment),
		len(p.EncryptionEntities), entities.String())
}

func fleProfileResponseXML(p *FieldLevelEncryptionProfile) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<FieldLevelEncryptionProfile xmlns="%s"><Id>%s</Id>`+
		`<FieldLevelEncryptionProfileConfig>%s</FieldLevelEncryptionProfileConfig>`+
		`</FieldLevelEncryptionProfile>`,
		cfNS, p.ID, fleProfileConfigInnerXML(p))
}

// fleProfileConfigResponseXML renders the config-only response (root =
// FieldLevelEncryptionProfileConfig) for GetFieldLevelEncryptionProfileConfig.
func fleProfileConfigResponseXML(p *FieldLevelEncryptionProfile) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<FieldLevelEncryptionProfileConfig xmlns="%s">%s</FieldLevelEncryptionProfileConfig>`,
		cfNS, fleProfileConfigInnerXML(p))
}

func (h *Handler) handleCreateFieldLevelEncryptionProfile(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req fleProfileConfigRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid FieldLevelEncryptionProfileConfig XML"),
			)
		}
	}

	if req.Name == "" {
		req.Name = generateID()
	}

	p, createErr := h.Backend.CreateFieldLevelEncryptionProfile(req.Name, req.Comment, req.toBackend())
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("ETag", p.ETag)
	c.Response().Header().Set("Location", cfPathPrefix+"field-level-encryption-profile/"+p.ID)

	return xmlResp(c, http.StatusCreated, fleProfileResponseXML(p))
}

func (h *Handler) handleGetFieldLevelEncryptionProfile(c *echo.Context, id string) error {
	p, err := h.Backend.GetFieldLevelEncryptionProfile(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", p.ETag)

	return xmlResp(c, http.StatusOK, fleProfileResponseXML(p))
}

// handleListFieldLevelEncryptionProfiles paginates via Marker/MaxItems (both query-bound,
// cloudfront@v1.67.4 serializers.go). Real FieldLevelEncryptionProfileList has no IsTruncated
// field -- NextMarker's presence alone signals truncation (types/types.go:3129-3149).
func (h *Handler) handleListFieldLevelEncryptionProfiles(c *echo.Context) error {
	items := h.Backend.ListFieldLevelEncryptionProfiles()

	page, pageSize, _, nextMarker := paginateByMarkerID(
		c, items, func(p *FieldLevelEncryptionProfile) string { return p.ID },
	)

	type fieldPatternsXML struct {
		Items    []string `xml:"Items>FieldPattern"`
		Quantity int      `xml:"Quantity"`
	}

	type encryptionEntityXML struct {
		PublicKeyID   string           `xml:"PublicKeyId"`
		ProviderID    string           `xml:"ProviderId"`
		FieldPatterns fieldPatternsXML `xml:"FieldPatterns"`
	}

	type encryptionEntitiesXML struct {
		Items    []encryptionEntityXML `xml:"Items>EncryptionEntity"`
		Quantity int                   `xml:"Quantity"`
	}

	type flePSummaryXML struct {
		XMLName            xml.Name              `xml:"FieldLevelEncryptionProfileSummary"`
		ID                 string                `xml:"Id"`
		Name               string                `xml:"Name"`
		Comment            string                `xml:"Comment"`
		EncryptionEntities encryptionEntitiesXML `xml:"EncryptionEntities"`
	}

	type flePListXML struct {
		XMLName    xml.Name         `xml:"FieldLevelEncryptionProfileList"`
		XMLNS      string           `xml:"xmlns,attr"`
		NextMarker string           `xml:"NextMarker,omitempty"`
		Items      []flePSummaryXML `xml:"Items>FieldLevelEncryptionProfileSummary"`
		MaxItems   int              `xml:"MaxItems"`
		Quantity   int              `xml:"Quantity"`
	}

	summaries := make([]flePSummaryXML, 0, len(page))
	for _, p := range page {
		entities := make([]encryptionEntityXML, 0, len(p.EncryptionEntities))
		for _, e := range p.EncryptionEntities {
			entities = append(entities, encryptionEntityXML{
				PublicKeyID: e.PublicKeyID,
				ProviderID:  e.ProviderID,
				FieldPatterns: fieldPatternsXML{
					Items:    append([]string(nil), e.FieldPatterns...),
					Quantity: len(e.FieldPatterns),
				},
			})
		}
		summaries = append(summaries, flePSummaryXML{
			ID:                 p.ID,
			Name:               p.Name,
			Comment:            p.Comment,
			EncryptionEntities: encryptionEntitiesXML{Items: entities, Quantity: len(entities)},
		})
	}

	list := flePListXML{
		XMLNS:      cfNS,
		NextMarker: nextMarker,
		MaxItems:   pageSize,
		Quantity:   len(summaries),
		Items:      summaries,
	}

	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

//nolint:dupl // update FLE profile and update FLE share structure but operate on distinct resource types
func (h *Handler) handleUpdateFieldLevelEncryptionProfile(c *echo.Context, id string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req fleProfileConfigRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid FieldLevelEncryptionProfileConfig XML"),
			)
		}
	}

	current, getErr := h.Backend.GetFieldLevelEncryptionProfile(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	name := req.Name
	if name == "" {
		name = current.Name
	}

	p, updateErr := h.Backend.UpdateFieldLevelEncryptionProfile(id, name, req.Comment, req.toBackend())
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", p.ETag)

	return xmlResp(c, http.StatusOK, fleProfileResponseXML(p))
}

func (h *Handler) handleDeleteFieldLevelEncryptionProfile(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetFieldLevelEncryptionProfile(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(
			c,
			http.StatusPreconditionFailed,
			cfErrorXML(
				"PreconditionFailed",
				"If-Match ETag did not match the current FieldLevelEncryptionProfile ETag",
			),
		)
	}

	if err := h.Backend.DeleteFieldLevelEncryptionProfile(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- Public Key handlers ---

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
