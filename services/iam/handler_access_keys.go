package iam

import "net/url"

func (h *Handler) iamAccessKeyDispatchTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"CreateAccessKey": func(vals url.Values, reqID string) (any, error) {
			ak, err := h.Backend.CreateAccessKey(vals.Get("UserName"))
			if err != nil {
				return nil, err
			}

			return &CreateAccessKeyResponse{
				Xmlns:                 iamXMLNS,
				CreateAccessKeyResult: CreateAccessKeyResult{AccessKey: toAccessKeyXML(ak)},
				ResponseMetadata:      ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteAccessKey": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteAccessKey(vals.Get("UserName"), vals.Get("AccessKeyId")); err != nil {
				return nil, err
			}

			return &DeleteAccessKeyResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil
		},
		"ListAccessKeys": func(vals url.Values, reqID string) (any, error) {
			p, err := h.Backend.ListAccessKeys(
				vals.Get("UserName"),
				vals.Get("Marker"),
				parseMaxItems(vals.Get("MaxItems")),
			)
			if err != nil {
				return nil, err
			}

			xmlKeys := make([]AccessKeyMetadataXML, 0, len(p.Data))
			for i := range p.Data {
				xmlKeys = append(xmlKeys, toAccessKeyMetadataXML(&p.Data[i]))
			}

			return &ListAccessKeysResponse{
				Xmlns: iamXMLNS,
				ListAccessKeysResult: ListAccessKeysResult{
					AccessKeyMetadata: xmlKeys,
					IsTruncated:       p.Next != "",
					Marker:            p.Next,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

func toAccessKeyXML(ak *AccessKey) AccessKeyXML {
	return AccessKeyXML{
		AccessKeyID:     ak.AccessKeyID,
		SecretAccessKey: ak.SecretAccessKey,
		UserName:        ak.UserName,
		Status:          ak.Status,
		CreateDate:      isoTime(ak.CreateDate),
	}
}

func toAccessKeyMetadataXML(ak *AccessKey) AccessKeyMetadataXML {
	return AccessKeyMetadataXML{
		AccessKeyID: ak.AccessKeyID,
		UserName:    ak.UserName,
		Status:      ak.Status,
		CreateDate:  isoTime(ak.CreateDate),
	}
}

// iamAccessKeyRefinementDispatch adds UpdateAccessKey and GetAccessKeyLastUsed.
func (h *Handler) iamAccessKeyRefinementDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"UpdateAccessKey": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.UpdateAccessKey(
				vals.Get("UserName"), vals.Get("AccessKeyId"), vals.Get("Status"),
			); err != nil {
				return nil, err
			}

			return &UpdateAccessKeyResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"GetAccessKeyLastUsed": func(vals url.Values, reqID string) (any, error) {
			info, err := h.Backend.GetAccessKeyLastUsed(vals.Get("AccessKeyId"))
			if err != nil {
				return nil, err
			}

			return &GetAccessKeyLastUsedResponse{
				Xmlns: iamXMLNS,
				GetAccessKeyLastUsedResult: GetAccessKeyLastUsedResult{
					UserName: info.UserName,
					AccessKeyLastUsed: AccessKeyLastUsedXML{
						LastUsedDate: info.LastUsedDate,
						ServiceName:  info.ServiceName,
						Region:       info.Region,
					},
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}
