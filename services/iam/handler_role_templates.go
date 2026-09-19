package iam

import (
	"fmt"
	"net/url"
	"strconv"
)

// parseReplacementValues parses AcquireRole's
// ReplacementValues.entry.N.key / .value.Values.member.M form values (the
// real awsquery wire shape for map[string]types.ReplacementValueEntry --
// confirmed against iam@v1.63.0 serializers.go's
// awsAwsquery_serializeDocumentMapStringReplacementValueEntry /
// awsAwsquery_serializeDocumentReplacementValueEntry).
func parseReplacementValues(vals url.Values) map[string][]string {
	out := make(map[string][]string)

	for i := 1; ; i++ {
		key := vals.Get(fmt.Sprintf("ReplacementValues.entry.%d.key", i))
		if key == "" {
			return out
		}

		var values []string

		for j := 1; ; j++ {
			v := vals.Get(fmt.Sprintf("ReplacementValues.entry.%d.value.Values.member.%d", i, j))
			if v == "" {
				break
			}

			values = append(values, v)
		}

		out[key] = values
	}
}

// parseOptionalInt32 parses a form field as *int32, returning nil (not 0)
// when the field is absent -- AcquireRole/GetRoleTemplateVersion's minor
// version fields distinguish "use the template's default minor version"
// (absent) from an explicit minor version 0.
func parseOptionalInt32(vals url.Values, key string) (*int32, error) {
	s := vals.Get(key)
	if s == "" {
		return nil, nil //nolint:nilnil // absent-vs-zero distinction is the point; see doc comment
	}

	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("%w: %s must be an integer", ErrInvalidInput, key)
	}

	n32 := int32(n)

	return &n32, nil
}

func (h *Handler) iamRoleTemplateDispatchTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"AcquireRole": func(vals url.Values, reqID string) (any, error) {
			minorVersion, err := parseOptionalInt32(vals, "TemplateMinorVersion")
			if err != nil {
				return nil, err
			}

			r, err := h.Backend.AcquireRole(
				vals.Get("TemplateArn"), minorVersion, parseReplacementValues(vals),
			)
			if err != nil {
				return nil, err
			}

			return &AcquireRoleResponse{
				Xmlns:             iamXMLNS,
				AcquireRoleResult: AcquireRoleResult{Role: toRoleXML(r)},
				ResponseMetadata:  ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"GetRoleTemplateVersion": func(vals url.Values, reqID string) (any, error) {
			minorVersion, err := parseOptionalInt32(vals, "MinorVersion")
			if err != nil {
				return nil, err
			}

			v, err := h.Backend.GetRoleTemplateVersion(vals.Get("TemplateArn"), minorVersion)
			if err != nil {
				return nil, err
			}

			return &GetRoleTemplateVersionResponse{
				Xmlns: iamXMLNS,
				GetRoleTemplateVersionResult: GetRoleTemplateVersionResult{
					RoleTemplateVersion: toRoleTemplateVersionXML(v),
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}
