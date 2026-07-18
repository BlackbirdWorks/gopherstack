package sts

import (
	"fmt"
	"net/http"

	"github.com/google/uuid"
)

// dispatchGetAccessKeyInfo handles the GetAccessKeyInfo action.
func (h *Handler) dispatchGetAccessKeyInfo(r *http.Request) (*GetAccessKeyInfoResponse, error) {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.cntGetAccessKeyInfo.Add(1)
	}

	accessKeyID := r.FormValue("AccessKeyId")

	if accessKeyID == "" {
		return nil, ErrEmptyAccessKeyID
	}

	// Look up the key in the session store.
	b, ok := h.Backend.(*InMemoryBackend)
	if ok {
		b.mu.RLock("GetAccessKeyInfo")
		session, found := b.sessions.Get(accessKeyID)
		b.mu.RUnlock()

		if found {
			return &GetAccessKeyInfoResponse{
				Xmlns: STSNamespace,
				GetAccessKeyInfoResult: GetAccessKeyInfoResult{
					Account: session.AccountID,
				},
				ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
			}, nil
		}
	}

	// Key not in session store. If the key is well-formed (known prefix + 16 alphanumeric chars),
	// return the backend account ID — AWS derives the account from the key prefix encoding.
	// Only completely malformed keys return ErrUnknownAccessKeyID → InvalidClientTokenId.
	if isWellFormedAccessKey(accessKeyID) {
		account := MockAccountID
		if b != nil {
			account = b.AccountID()
		}

		return &GetAccessKeyInfoResponse{
			Xmlns: STSNamespace,
			GetAccessKeyInfoResult: GetAccessKeyInfoResult{
				Account: account,
			},
			ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
		}, nil
	}

	// Malformed key format — ValidationError per AWS.
	return nil, fmt.Errorf(
		"%w: AccessKeyId %q does not match expected format",
		ErrEmptyAccessKeyID,
		accessKeyID,
	)
}
