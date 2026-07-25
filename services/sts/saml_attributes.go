package sts

import (
	"encoding/xml"
	"strings"
)

// samlAssertionData holds the identity attributes AWS derives from a SAML
// assertion for AssumeRoleWithSAML. None of RoleSessionName, SourceIdentity,
// or session tags are separate top-level request parameters in the real API
// (aws-sdk-go-v2/service/sts's AssumeRoleWithSAMLInput carries only
// PrincipalArn/RoleArn/SAMLAssertion/DurationSeconds/Policy/PolicyArns) — AWS
// documents that these values, along with Subject/SubjectType/Issuer/Audience
// in the response, are read out of the assertion's <NameID>, <Issuer>,
// <SubjectConfirmationData>, and <Attribute> elements server-side.
type samlAssertionData struct {
	NameID            string
	NameIDFormat      string
	Issuer            string
	Recipient         string
	RoleSessionName   string
	SourceIdentity    string
	TransitiveTagKeys []string
	Tags              []Tag
}

const (
	// samlAttrRoleSessionName is the SAML attribute AWS reads for the assumed
	// role's session name (see "Configuring SAML assertions for the
	// authentication response" in the IAM User Guide).
	samlAttrRoleSessionName = "https://aws.amazon.com/SAML/Attributes/RoleSessionName"

	// samlAttrSourceIdentity is the SAML attribute AWS reads for SourceIdentity.
	samlAttrSourceIdentity = "https://aws.amazon.com/SAML/Attributes/SourceIdentity"

	// samlAttrTransitiveTagKeys is the SAML attribute listing which
	// PrincipalTag: attributes are transitive across role chaining.
	samlAttrTransitiveTagKeys = "https://aws.amazon.com/SAML/Attributes/TransitiveTagKeys"

	// samlAttrPrincipalTagPrefix prefixes session-tag attributes; the tag key
	// is the remainder of the attribute Name after this prefix.
	samlAttrPrincipalTagPrefix = "https://aws.amazon.com/SAML/Attributes/PrincipalTag:"

	// samlNameIDFormatPrefix is stripped from NameID's Format attribute per
	// AWS's documented SubjectType derivation.
	samlNameIDFormatPrefix = "urn:oasis:names:tc:SAML:2.0:nameid-format:"
)

// extractSAMLAssertionData parses a base64-encoded SAML assertion for the
// identity attributes described above. It is permissive: assertions that are
// not well-formed XML, or that carry none of these elements, yield a
// zero-value result and the caller falls back to its own defaults — the
// emulator does not require a fully-formed SAML document (see
// validateSAMLAssertion).
func extractSAMLAssertionData(assertion string) samlAssertionData {
	var data samlAssertionData

	decoded, ok := decodeSAMLAssertion(assertion)
	if !ok {
		return data
	}

	dec := xml.NewDecoder(strings.NewReader(string(decoded)))

	var walk samlAssertionWalkState

	for {
		tok, err := dec.Token()
		if err != nil {
			break
		}

		walk.apply(&data, tok)
	}

	if data.NameIDFormat != "" {
		data.NameIDFormat = strings.TrimPrefix(data.NameIDFormat, samlNameIDFormatPrefix)
	}

	return data
}

// samlAssertionWalkState tracks which element the token stream is currently
// inside of, so CharData can be routed to the right samlAssertionData field.
type samlAssertionWalkState struct {
	curAttrName string
	inNameID    bool
	inIssuer    bool
	inAttrValue bool
}

func (w *samlAssertionWalkState) apply(data *samlAssertionData, tok xml.Token) {
	switch t := tok.(type) {
	case xml.StartElement:
		w.applyStart(data, t)
	case xml.EndElement:
		w.applyEnd(t)
	case xml.CharData:
		w.applyCharData(data, t)
	}
}

func (w *samlAssertionWalkState) applyStart(data *samlAssertionData, t xml.StartElement) {
	switch t.Name.Local {
	case "NameID":
		w.inNameID = true
		data.NameIDFormat = xmlAttrValue(t.Attr, "Format")
	case "Issuer":
		if data.Issuer == "" {
			w.inIssuer = true
		}
	case "SubjectConfirmationData":
		if data.Recipient == "" {
			data.Recipient = xmlAttrValue(t.Attr, "Recipient")
		}
	case "Attribute":
		w.curAttrName = xmlAttrValue(t.Attr, "Name")
	case "AttributeValue":
		w.inAttrValue = w.curAttrName != ""
	}
}

func (w *samlAssertionWalkState) applyEnd(t xml.EndElement) {
	switch t.Name.Local {
	case "NameID":
		w.inNameID = false
	case "Issuer":
		w.inIssuer = false
	case "Attribute":
		w.curAttrName = ""
	case "AttributeValue":
		w.inAttrValue = false
	}
}

func (w *samlAssertionWalkState) applyCharData(data *samlAssertionData, t xml.CharData) {
	text := strings.TrimSpace(string(t))
	if text == "" {
		return
	}

	if w.inNameID && data.NameID == "" {
		data.NameID = text
	}

	if w.inIssuer && data.Issuer == "" {
		data.Issuer = text
	}

	if w.inAttrValue {
		applySAMLAttributeValue(data, w.curAttrName, text)
	}
}

// applySAMLAttributeValue routes one <AttributeValue> text node to the
// samlAssertionData field matching its enclosing <Attribute Name="...">.
func applySAMLAttributeValue(data *samlAssertionData, attrName, value string) {
	switch {
	case attrName == samlAttrRoleSessionName:
		if data.RoleSessionName == "" {
			data.RoleSessionName = value
		}
	case attrName == samlAttrSourceIdentity:
		if data.SourceIdentity == "" {
			data.SourceIdentity = value
		}
	case attrName == samlAttrTransitiveTagKeys:
		if len(data.TransitiveTagKeys) == 0 {
			data.TransitiveTagKeys = splitSAMLTagKeyList(value)
		}
	case strings.HasPrefix(attrName, samlAttrPrincipalTagPrefix):
		key := strings.TrimPrefix(attrName, samlAttrPrincipalTagPrefix)
		data.Tags = append(data.Tags, Tag{Key: key, Value: value})
	}
}

// splitSAMLTagKeyList splits a comma-separated TransitiveTagKeys attribute value.
func splitSAMLTagKeyList(value string) []string {
	var keys []string

	for k := range strings.SplitSeq(value, ",") {
		if k = strings.TrimSpace(k); k != "" {
			keys = append(keys, k)
		}
	}

	return keys
}

// xmlAttrValue returns the value of the attribute with the given local name,
// ignoring namespace, or "" if absent.
func xmlAttrValue(attrs []xml.Attr, local string) string {
	for _, a := range attrs {
		if a.Name.Local == local {
			return a.Value
		}
	}

	return ""
}
