package iam

import (
	"net/url"
	"time"
)

func (h *Handler) iamSAMLProviderDispatchTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"CreateSAMLProvider": func(vals url.Values, reqID string) (any, error) {
			p, err := h.Backend.CreateSAMLProvider(vals.Get("Name"), vals.Get("SAMLMetadataDocument"))
			if err != nil {
				return nil, err
			}

			return &CreateSAMLProviderResponse{
				Xmlns:                    iamXMLNS,
				CreateSAMLProviderResult: CreateSAMLProviderResult{SAMLProviderArn: p.Arn},
				ResponseMetadata:         ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UpdateSAMLProvider": func(vals url.Values, reqID string) (any, error) {
			p, err := h.Backend.UpdateSAMLProvider(vals.Get("SAMLProviderArn"), vals.Get("SAMLMetadataDocument"))
			if err != nil {
				return nil, err
			}

			return &UpdateSAMLProviderResponse{
				Xmlns:                    iamXMLNS,
				UpdateSAMLProviderResult: UpdateSAMLProviderResult{SAMLProviderArn: p.Arn},
				ResponseMetadata:         ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteSAMLProvider": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteSAMLProvider(vals.Get("SAMLProviderArn")); err != nil {
				return nil, err
			}

			return &DeleteSAMLProviderResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"GetSAMLProvider": func(vals url.Values, reqID string) (any, error) {
			p, err := h.Backend.GetSAMLProvider(vals.Get("SAMLProviderArn"))
			if err != nil {
				return nil, err
			}

			var validUntil string
			if !p.ValidUntil.IsZero() {
				validUntil = isoTime(p.ValidUntil)
			}

			return &GetSAMLProviderResponse{
				Xmlns: iamXMLNS,
				GetSAMLProviderResult: GetSAMLProviderResult{
					SAMLMetadataDocument: p.SAMLMetadataDocument,
					CreateDate:           isoTime(p.CreateDate),
					ValidUntil:           validUntil,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"ListSAMLProviders": func(_ url.Values, reqID string) (any, error) {
			providers, err := h.Backend.ListSAMLProviders()
			if err != nil {
				return nil, err
			}

			entries := make([]SAMLProviderListEntryXML, 0, len(providers))
			for _, p := range providers {
				var validUntil string
				if !p.ValidUntil.IsZero() {
					validUntil = isoTime(p.ValidUntil)
				}

				entries = append(entries, SAMLProviderListEntryXML{
					Arn:        p.Arn,
					CreateDate: isoTime(p.CreateDate),
					ValidUntil: validUntil,
				})
			}

			return &ListSAMLProvidersResponse{
				Xmlns:                   iamXMLNS,
				ListSAMLProvidersResult: ListSAMLProvidersResult{SAMLProviderList: entries},
				ResponseMetadata:        ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

func (h *Handler) iamOIDCProviderDispatchTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"CreateOpenIDConnectProvider": func(vals url.Values, reqID string) (any, error) {
			clientIDs := parseIndexedValues(vals, "ClientIDList.member.")
			thumbprints := parseIndexedValues(vals, "ThumbprintList.member.")

			p, err := h.Backend.CreateOpenIDConnectProvider(vals.Get("Url"), clientIDs, thumbprints)
			if err != nil {
				return nil, err
			}

			return &CreateOpenIDConnectProviderResponse{
				Xmlns: iamXMLNS,
				CreateOpenIDConnectProviderResult: CreateOpenIDConnectProviderResult{
					OpenIDConnectProviderArn: p.Arn,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UpdateOpenIDConnectProviderThumbprint": func(vals url.Values, reqID string) (any, error) {
			thumbprints := parseIndexedValues(vals, "ThumbprintList.member.")

			if err := h.Backend.UpdateOpenIDConnectProviderThumbprint(
				vals.Get("OpenIDConnectProviderArn"), thumbprints,
			); err != nil {
				return nil, err
			}

			return &UpdateOpenIDConnectProviderThumbprintResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteOpenIDConnectProvider": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteOpenIDConnectProvider(vals.Get("OpenIDConnectProviderArn")); err != nil {
				return nil, err
			}

			return &DeleteOpenIDConnectProviderResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"GetOpenIDConnectProvider": func(vals url.Values, reqID string) (any, error) {
			p, err := h.Backend.GetOpenIDConnectProvider(vals.Get("OpenIDConnectProviderArn"))
			if err != nil {
				return nil, err
			}

			return &GetOpenIDConnectProviderResponse{
				Xmlns: iamXMLNS,
				GetOpenIDConnectProviderResult: GetOpenIDConnectProviderResult{
					URL:            p.URL,
					ClientIDList:   p.ClientIDList,
					ThumbprintList: p.ThumbprintList,
					CreateDate:     isoTime(p.CreateDate),
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"ListOpenIDConnectProviders": func(_ url.Values, reqID string) (any, error) {
			providers, err := h.Backend.ListOpenIDConnectProviders()
			if err != nil {
				return nil, err
			}

			entries := make([]OIDCProviderListEntryXML, 0, len(providers))
			for _, p := range providers {
				entries = append(entries, OIDCProviderListEntryXML{Arn: p.Arn})
			}

			return &ListOpenIDConnectProvidersResponse{
				Xmlns: iamXMLNS,
				ListOpenIDConnectProvidersResult: ListOpenIDConnectProvidersResult{
					OpenIDConnectProviderList: entries,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

func (h *Handler) iamLoginProfileDispatchTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"CreateLoginProfile": func(vals url.Values, reqID string) (any, error) {
			lp, err := h.Backend.CreateLoginProfile(
				vals.Get("UserName"),
				vals.Get("Password"),
				vals.Get("PasswordResetRequired") == "true",
			)
			if err != nil {
				return nil, err
			}

			return &CreateLoginProfileResponse{
				Xmlns: iamXMLNS,
				CreateLoginProfileResult: CreateLoginProfileResult{
					LoginProfile: toLoginProfileXML(lp),
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UpdateLoginProfile": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.UpdateLoginProfile(
				vals.Get("UserName"),
				vals.Get("Password"),
				vals.Get("PasswordResetRequired") == "true",
			); err != nil {
				return nil, err
			}

			return &UpdateLoginProfileResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteLoginProfile": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteLoginProfile(vals.Get("UserName")); err != nil {
				return nil, err
			}

			return &DeleteLoginProfileResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"GetLoginProfile": func(vals url.Values, reqID string) (any, error) {
			lp, err := h.Backend.GetLoginProfile(vals.Get("UserName"))
			if err != nil {
				return nil, err
			}

			return &GetLoginProfileResponse{
				Xmlns:                 iamXMLNS,
				GetLoginProfileResult: GetLoginProfileResult{LoginProfile: toLoginProfileXML(lp)},
				ResponseMetadata:      ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

func (h *Handler) iamMiscDispatchTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"GetServiceLastAccessedDetails": func(_ url.Values, reqID string) (any, error) {
			now := isoTime(time.Now().UTC())

			return &GetServiceLastAccessedDetailsResponse{
				Xmlns: iamXMLNS,
				GetServiceLastAccessedDetailsResult: GetServiceLastAccessedDetailsResult{
					JobStatus:         "COMPLETED",
					JobCreationDate:   now,
					JobCompletionDate: now,
					IsTruncated:       false,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"SetSecurityTokenServicePreferences": func(_ url.Values, reqID string) (any, error) {
			return &SetSecurityTokenServicePreferencesResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// toLoginProfileXML converts a LoginProfile to its XML representation.
func toLoginProfileXML(lp *LoginProfile) LoginProfileXML {
	return LoginProfileXML{
		UserName:              lp.UserName,
		CreateDate:            isoTime(lp.CreateDate),
		PasswordResetRequired: lp.PasswordResetRequired,
	}
}
