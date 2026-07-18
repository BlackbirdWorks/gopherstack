package sts

import (
	"encoding/base64"
	"net/http"

	"github.com/google/uuid"
)

// dispatchDecodeAuthorizationMessage handles the DecodeAuthorizationMessage action.
// Messages issued by IssueEncodedAuthorizationMessage on this backend are verified and
// their plaintext returned. As an emulator we also accept any other valid base64 blob
// (real clients pass encoded messages this server never issued) and return its decoded
// bytes, so the operation stays usable; only a non-base64 or empty input is rejected.
func (h *Handler) dispatchDecodeAuthorizationMessage(
	r *http.Request,
) (*DecodeAuthorizationMessageResponse, error) {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.cntDecodeAuthorizationMsg.Add(1)
	}

	encoded := r.FormValue("EncodedMessage")

	if encoded == "" {
		return nil, ErrMissingEncodedMessage
	}

	decoded, err := h.Backend.VerifyEncodedAuthorizationMessage(encoded)
	if err != nil {
		// Not a self-issued message; fall back to a plain base64 decode.
		raw, derr := base64.StdEncoding.DecodeString(encoded)
		if derr != nil {
			if raw, derr = base64.URLEncoding.DecodeString(encoded); derr != nil {
				return nil, err
			}
		}

		decoded = string(raw)
	}

	return &DecodeAuthorizationMessageResponse{
		Xmlns: STSNamespace,
		DecodeAuthorizationMessageResult: DecodeAuthorizationMessageResult{
			DecodedMessage: decoded,
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	}, nil
}
