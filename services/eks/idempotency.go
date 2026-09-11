package eks

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// gopherstack-wf8f item 3: ClientRequestToken idempotency dedup.
//
// Every eks@v1.98.0 op that declares ClientRequestToken documents the same
// behavior (e.g. api_op_CreateCluster.go's CreateClusterInput.ClientRequestToken
// doc comment): "A unique, case-sensitive identifier that you provide to
// ensure the idempotency of the request ... If you retry a request with the
// same client request token and the same parameters after the original
// request has completed successfully, the result of the original request is
// returned."
//
// No op in this pinned SDK declares a distinct error for a token reused with
// DIFFERENT parameters (grepped every eks@v1.98.0 api_op_*.go and
// types/errors.go for "IdempotentParameterMismatch" or similar -- zero
// matches, unlike e.g. IAM's IdempotentParameterMismatchException); the EKS
// API reference's own Errors tables list nothing token-specific either. Per
// parity-principles (do not invent a code), a same-token/different-params
// replay is answered with InvalidParameterException (ErrValidation), the
// same general request-shape-fault code these ops already use for every
// other client-side request defect.
//
// The 24-hour token validity window api_op_CreateCluster.go's doc comment
// mentions ("This token is valid for 24 hours after creation.") is not
// enforced: tokens remain valid for the lifetime of the backend. This is a
// conservative simplification (it can only cause an over-eager replay of a
// token a real server would have already expired, never an incorrect new
// resource) and is disclosed in PARITY.md rather than silently added.

type idempotencyRecord struct {
	Op          string          `json:"op"`
	Token       string          `json:"token"`
	Fingerprint string          `json:"fingerprint"`
	Body        json.RawMessage `json:"body"`
	StatusCode  int             `json:"statusCode"`
}

func idempotencyKey(op, token string) string { return op + "\x00" + token }

func idempotencyKeyFn(v *idempotencyRecord) string { return idempotencyKey(v.Op, v.Token) }

// idempotencyFingerprint canonicalizes reqBody (decode into a generic value,
// drop clientRequestToken, re-encode) so two requests that are the same
// "parameters" per the doc language above fingerprint identically regardless
// of incidental JSON key ordering -- encoding/json sorts map keys at every
// nesting level, so two semantically-equal bodies always re-marshal
// byte-identically.
func idempotencyFingerprint(reqBody []byte) string {
	var v map[string]any
	if err := json.Unmarshal(reqBody, &v); err != nil {
		sum := sha256.Sum256(reqBody)

		return hex.EncodeToString(sum[:])
	}

	delete(v, "clientRequestToken")

	canon, err := json.Marshal(v)
	if err != nil {
		canon = reqBody
	}

	sum := sha256.Sum256(canon)

	return hex.EncodeToString(sum[:])
}

// LookupIdempotency returns the stored record for (op, token), if any.
func (b *InMemoryBackend) LookupIdempotency(op, token string) (idempotencyRecord, bool) {
	b.mu.RLock("LookupIdempotency")
	defer b.mu.RUnlock()

	rec, ok := b.idempotency.Get(idempotencyKey(op, token))
	if !ok {
		return idempotencyRecord{}, false
	}

	return *rec, true
}

// StoreIdempotency records a successful (2xx) response for later replay.
func (b *InMemoryBackend) StoreIdempotency(op, token, fingerprint string, statusCode int, body json.RawMessage) {
	b.mu.Lock("StoreIdempotency")
	defer b.mu.Unlock()

	b.idempotency.Put(&idempotencyRecord{
		Op: op, Token: token, Fingerprint: fingerprint, StatusCode: statusCode, Body: body,
	})
}

// withIdempotency wraps a Create/Update handler's real work (fn). When token
// is empty (ClientRequestToken not supplied -- optional on every op that
// carries it), fn always runs. Otherwise: a prior record for (op, token)
// with a matching fingerprint short-circuits fn entirely and replays the
// original response verbatim (never re-invoking the backend, so a replay can
// never itself trip a duplicate-resource ResourceInUseException/
// ResourceLimitExceededException); a mismatched fingerprint is rejected
// without calling fn; no record calls fn and stores its result iff it
// succeeds with a 2xx status.
//
// fn returns the response to write (status, body) or an error, which is
// passed to h.handleError -- matching every existing handler's error path,
// and consistent with the doc language above ("after the original request
// has completed successfully"): a failed attempt is never stored, so a
// retry with the same token after a failure runs fn again for real, exactly
// as real AWS would.
func (h *Handler) withIdempotency(
	c *echo.Context, op, token string, reqBody []byte, fn func() (int, any, error),
) error {
	if token == "" {
		status, body, err := fn()
		if err != nil {
			return h.handleError(c, err)
		}

		return c.JSON(status, body)
	}

	fp := idempotencyFingerprint(reqBody)

	if rec, ok := h.Backend.LookupIdempotency(op, token); ok {
		if rec.Fingerprint != fp {
			return c.JSON(http.StatusBadRequest, errResp(
				"InvalidParameterException",
				"clientRequestToken "+token+" was already used with different parameters",
			))
		}

		var body any
		if err := json.Unmarshal(rec.Body, &body); err != nil {
			return c.JSON(http.StatusInternalServerError, errResp("InternalFailure", err.Error()))
		}

		return c.JSON(rec.StatusCode, body)
	}

	status, body, err := fn()
	if err != nil {
		return h.handleError(c, err)
	}

	if status >= http.StatusOK && status < http.StatusMultipleChoices {
		if raw, mErr := json.Marshal(body); mErr == nil {
			h.Backend.StoreIdempotency(op, token, fp, status, raw)
		}
	}

	return c.JSON(status, body)
}
