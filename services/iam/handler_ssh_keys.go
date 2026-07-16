package iam

import (
	"encoding/xml"
	"net/url"
)

// iamSSHKeyUploadGetDispatch wires UploadSSHPublicKey and GetSSHPublicKey with real storage.
func (h *Handler) iamSSHKeyUploadGetDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		opUploadSSHPublicKey: func(vals url.Values, reqID string) (any, error) {
			key, err := h.Backend.UploadSSHPublicKey(
				vals.Get("UserName"),
				vals.Get("SSHPublicKeyBody"),
			)
			if err != nil {
				return nil, err
			}

			return &uploadSSHPublicKeyResponse{
				XMLName: xmlLocalName("UploadSSHPublicKeyResponse"),
				Xmlns:   iamXMLNS,
				UploadSSHPublicKeyResult: uploadSSHPublicKeyResult{
					SSHPublicKey: sshPublicKeyXML{
						UserName:         key.UserName,
						SSHPublicKeyID:   key.SSHPublicKeyID,
						Fingerprint:      key.Fingerprint,
						SSHPublicKeyBody: key.SSHPublicKeyBody,
						Status:           key.Status,
						UploadDate:       isoTime(key.UploadDate),
					},
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},

		opGetSSHPublicKey: func(vals url.Values, reqID string) (any, error) {
			key, err := h.Backend.GetSSHPublicKey(
				vals.Get("UserName"),
				vals.Get("SSHPublicKeyId"),
			)
			if err != nil {
				return nil, err
			}

			return &getSSHPublicKeyResponse{
				XMLName: xmlLocalName("GetSSHPublicKeyResponse"),
				Xmlns:   iamXMLNS,
				GetSSHPublicKeyResult: getSSHPublicKeyResult{
					SSHPublicKey: sshPublicKeyXML{
						UserName:         key.UserName,
						SSHPublicKeyID:   key.SSHPublicKeyID,
						Fingerprint:      key.Fingerprint,
						SSHPublicKeyBody: key.SSHPublicKeyBody,
						Status:           key.Status,
						UploadDate:       isoTime(key.UploadDate),
					},
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamSSHKeyListDeleteDispatch wires ListSSHPublicKeys, UpdateSSHPublicKey, DeleteSSHPublicKey.
func (h *Handler) iamSSHKeyListDeleteDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		opListSSHPublicKeys: func(vals url.Values, reqID string) (any, error) {
			userName := vals.Get("UserName")

			p, err := h.Backend.ListSSHPublicKeys(
				userName,
				vals.Get("Marker"),
				parseMaxItems(vals.Get("MaxItems")),
			)
			if err != nil {
				return nil, err
			}

			members := make([]sshPublicKeyMetaXML, 0, len(p.Data))
			for _, k := range p.Data {
				members = append(members, sshPublicKeyMetaXML{
					UserName:       k.UserName,
					SSHPublicKeyID: k.SSHPublicKeyID,
					Status:         k.Status,
					UploadDate:     isoTime(k.UploadDate),
				})
			}

			return &listSSHPublicKeysResponse{
				XMLName: xmlLocalName("ListSSHPublicKeysResponse"),
				Xmlns:   iamXMLNS,
				ListSSHPublicKeysResult: listSSHPublicKeysResult{
					SSHPublicKeys: members,
					IsTruncated:   p.Next != "",
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},

		opUpdateSSHPublicKey: func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.UpdateSSHPublicKey(
				vals.Get("UserName"),
				vals.Get("SSHPublicKeyId"),
				vals.Get("Status"),
			); err != nil {
				return nil, err
			}

			return &iamSimpleTagResponse{
				XMLName:          xmlLocalName("UpdateSSHPublicKeyResponse"),
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},

		opDeleteSSHPublicKey: func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteSSHPublicKey(
				vals.Get("UserName"),
				vals.Get("SSHPublicKeyId"),
			); err != nil {
				return nil, err
			}

			return &iamSimpleTagResponse{
				XMLName:          xmlLocalName("DeleteSSHPublicKeyResponse"),
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamSSHKeyCompletenessDispatch returns SSH public key dispatch entries added
// in the completeness pass. These duplicate (and are shadowed by, since
// buildDispatchTable merges iamComprehensiveDispatchTable after
// iamCompletenessDispatchTable) the real implementations in
// iamSSHKeyUploadGetDispatch / iamSSHKeyListDeleteDispatch; kept verbatim as
// part of a pure reorganization (no behavior change).
func (h *Handler) iamSSHKeyCompletenessDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"ListSSHPublicKeys": func(vals url.Values, reqID string) (any, error) {
			p, err := h.Backend.ListSSHPublicKeys(vals.Get("UserName"), vals.Get("Marker"), iamDefaultMaxItems)
			if err != nil {
				return nil, err
			}

			members := make([]sshPublicKeyMetaXML, 0, len(p.Data))
			for _, k := range p.Data {
				members = append(members, sshPublicKeyMetaXML{
					UserName:       k.UserName,
					SSHPublicKeyID: k.SSHPublicKeyID,
					Status:         k.Status,
					UploadDate:     isoTime(k.UploadDate),
				})
			}

			return &listSSHPublicKeysResponse{
				XMLName: xml.Name{Local: "ListSSHPublicKeysResponse"},
				Xmlns:   iamXMLNS,
				ListSSHPublicKeysResult: listSSHPublicKeysResult{
					SSHPublicKeys: members,
					IsTruncated:   p.Next != "",
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"GetSSHPublicKey": func(vals url.Values, reqID string) (any, error) {
			key, err := h.Backend.GetSSHPublicKey(vals.Get("UserName"), vals.Get("SSHPublicKeyId"))
			if err != nil {
				return nil, err
			}

			return &getSSHPublicKeyResponse{
				XMLName: xml.Name{Local: "GetSSHPublicKeyResponse"},
				Xmlns:   iamXMLNS,
				GetSSHPublicKeyResult: getSSHPublicKeyResult{
					SSHPublicKey: sshPublicKeyXML{
						UserName:         key.UserName,
						SSHPublicKeyID:   key.SSHPublicKeyID,
						Fingerprint:      key.Fingerprint,
						SSHPublicKeyBody: key.SSHPublicKeyBody,
						Status:           key.Status,
						UploadDate:       isoTime(key.UploadDate),
					},
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteSSHPublicKey": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteSSHPublicKey(vals.Get("UserName"), vals.Get("SSHPublicKeyId")); err != nil {
				return nil, err
			}

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "DeleteSSHPublicKeyResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UpdateSSHPublicKey": func(vals url.Values, reqID string) (any, error) {
			userName := vals.Get("UserName")
			keyID := vals.Get("SSHPublicKeyId")
			status := vals.Get("Status")

			if err := h.Backend.UpdateSSHPublicKey(userName, keyID, status); err != nil {
				return nil, err
			}

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "UpdateSSHPublicKeyResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UploadSSHPublicKey": func(vals url.Values, reqID string) (any, error) {
			key, err := h.Backend.UploadSSHPublicKey(vals.Get("UserName"), vals.Get("SSHPublicKeyBody"))
			if err != nil {
				return nil, err
			}

			return &uploadSSHPublicKeyResponse{
				XMLName: xml.Name{Local: "UploadSSHPublicKeyResponse"},
				Xmlns:   iamXMLNS,
				UploadSSHPublicKeyResult: uploadSSHPublicKeyResult{
					SSHPublicKey: sshPublicKeyXML{
						UserName:         key.UserName,
						SSHPublicKeyID:   key.SSHPublicKeyID,
						Fingerprint:      key.Fingerprint,
						SSHPublicKeyBody: key.SSHPublicKeyBody,
						Status:           key.Status,
						UploadDate:       isoTime(key.UploadDate),
					},
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}
