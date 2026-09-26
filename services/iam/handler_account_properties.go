package iam

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"sort"
)

// AccountPropertyEntryXML is one Namespace/PropertyName -> value pair in the
// awsquery map wire shape (<entry><key>...</key><value>...</value></entry>,
// confirmed against iam@v1.63.0 (de)serializers.go's
// AccountPropertiesMapType, which uses value.Map("key", "value") --
// lowercase, unlike this package's usual PascalCase members).
type AccountPropertyEntryXML struct {
	Key   string `xml:"key"`
	Value string `xml:"value"`
}

// GetAccountPropertiesResult wraps the account's property map.
type GetAccountPropertiesResult struct {
	Properties []AccountPropertyEntryXML `xml:"Properties>entry,omitempty"`
}

// GetAccountPropertiesResponse is the XML response for GetAccountProperties.
type GetAccountPropertiesResponse struct {
	XMLName                    xml.Name                   `xml:"GetAccountPropertiesResponse"`
	Xmlns                      string                     `xml:"xmlns,attr"`
	ResponseMetadata           ResponseMetadata           `xml:"ResponseMetadata"`
	GetAccountPropertiesResult GetAccountPropertiesResult `xml:"GetAccountPropertiesResult"`
}

// PutAccountPropertiesResult is an empty element real AWS still emits: the
// client's own deserializer (iam@v1.63.0 deserializers.go) unconditionally
// looks for a PutAccountPropertiesResult element even though
// PutAccountPropertiesOutput carries no members, and errors if it's absent.
type PutAccountPropertiesResult struct{}

// PutAccountPropertiesResponse is the XML response for PutAccountProperties.
type PutAccountPropertiesResponse struct {
	PutAccountPropertiesResult PutAccountPropertiesResult `xml:"PutAccountPropertiesResult"`
	XMLName                    xml.Name                   `xml:"PutAccountPropertiesResponse"`
	Xmlns                      string                     `xml:"xmlns,attr"`
	ResponseMetadata           ResponseMetadata           `xml:"ResponseMetadata"`
}

func accountPropertiesToXML(props map[string]string) []AccountPropertyEntryXML {
	if len(props) == 0 {
		return nil
	}

	keys := make([]string, 0, len(props))
	for k := range props {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	out := make([]AccountPropertyEntryXML, 0, len(keys))
	for _, k := range keys {
		out = append(out, AccountPropertyEntryXML{Key: k, Value: props[k]})
	}

	return out
}

// parseAccountPropertiesForm parses PutAccountProperties'
// Properties.entry.N.key / .value form values (the request-direction mirror
// of AccountPropertiesMapType).
func parseAccountPropertiesForm(vals url.Values) map[string]string {
	out := make(map[string]string)

	for i := 1; ; i++ {
		key := vals.Get(fmt.Sprintf("Properties.entry.%d.key", i))
		if key == "" {
			return out
		}

		out[key] = vals.Get(fmt.Sprintf("Properties.entry.%d.value", i))
	}
}

func (h *Handler) iamAccountPropertiesDispatchTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"GetAccountProperties": func(_ url.Values, reqID string) (any, error) {
			props := h.Backend.GetAccountProperties()

			return &GetAccountPropertiesResponse{
				Xmlns: iamXMLNS,
				GetAccountPropertiesResult: GetAccountPropertiesResult{
					Properties: accountPropertiesToXML(props),
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"PutAccountProperties": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.PutAccountProperties(parseAccountPropertiesForm(vals)); err != nil {
				return nil, err
			}

			return &PutAccountPropertiesResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}
