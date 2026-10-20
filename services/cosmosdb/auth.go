package cosmosdb

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/azureauth"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// maxAuthClockSkew is the x-ms-date skew real Cosmos tolerates.
const maxAuthClockSkew = 15 * time.Minute

var (
	// ErrMissingAuthorization is returned when validation is on and a request carries no Authorization header.
	ErrMissingAuthorization = errors.New("cosmosdb: missing Authorization header")
	// ErrMissingDate is returned when neither x-ms-date nor Date is present.
	ErrMissingDate = errors.New("cosmosdb: missing x-ms-date header")
	// ErrDateSkew is returned when the request date is outside maxAuthClockSkew of server time.
	ErrDateSkew = errors.New("cosmosdb: x-ms-date outside the allowed clock skew")
	// ErrBadSignature is returned when the signature does not match.
	ErrBadSignature = errors.New("cosmosdb: signature mismatch")
)

// authorize enforces the master-key scheme for every route when ValidateAuth is set; it is a no-op otherwise.
func (h *Handler) authorize(r *http.Request) error {
	header := r.Header.Get("Authorization")

	if !h.ValidateAuth {
		if _, ok := parseMasterKeyAuthorization(header); header != "" && !ok {
			logger.Load(r.Context()).DebugContext(r.Context(), "cosmosdb: malformed Authorization header accepted")
		}

		return nil
	}

	if header == "" {
		return ErrMissingAuthorization
	}

	if err := h.checkDate(r); err != nil {
		return err
	}

	var (
		ok  bool
		err error
	)

	if _, shared := azureauth.ParseAuthorizationHeader(header); shared && isTableAPIPath(r.URL.Path) {
		ok, err = h.verifyAnyKey(r, azureauth.VerifySharedKey)
	} else {
		ok, err = h.verifyAnyKey(r, VerifyMasterKey)
	}

	if err != nil {
		return fmt.Errorf("%w: %w", ErrBadSignature, err)
	}

	if !ok {
		return ErrBadSignature
	}

	return nil
}

func (h *Handler) checkDate(r *http.Request) error {
	raw := r.Header.Get("X-Ms-Date")
	if raw == "" {
		raw = r.Header.Get("Date")
	}

	if strings.TrimSpace(raw) == "" {
		return ErrMissingDate
	}

	ts, err := http.ParseTime(raw)
	if err != nil {
		return fmt.Errorf("%w: unparsable date %q", ErrDateSkew, raw)
	}

	if d := h.now().Sub(ts); d > maxAuthClockSkew || d < -maxAuthClockSkew {
		return ErrDateSkew
	}

	return nil
}

func (h *Handler) now() time.Time {
	if h.clock != nil {
		return h.clock()
	}

	return time.Now()
}

// SignRequest adds x-ms-date and a master-key Authorization header to r using the handler's key. It lets
// in-process callers (the dashboard proxy) satisfy ValidateAuth.
func (h *Handler) SignRequest(r *http.Request) error {
	date := h.now().UTC().Format(http.TimeFormat)
	r.Header.Set("X-Ms-Date", date)

	resourceType, resourceID := resourceTypeAndIDFor(r.Method, r.URL.Path)

	sig, err := signMasterKey(h.MasterKey, masterKeyStringToSign(r.Method, resourceType, resourceID, date, ""))
	if err != nil {
		return err
	}

	r.Header.Set("Authorization", encodeMasterKeyAuthorization(sig))

	return nil
}

// authErrorMessage is the 401 body text for err.
func authErrorMessage(err error) string {
	switch {
	case errors.Is(err, ErrMissingAuthorization):
		return "Authorization header is missing. The input authorization token can't serve the request."
	case errors.Is(err, ErrMissingDate):
		return "The input authorization token can't serve the request: required header x-ms-date is missing."
	case errors.Is(err, ErrDateSkew):
		return "The input authorization token can't serve the request: x-ms-date is more than 15 minutes from server time."
	default:
		return "The input authorization token can't serve the request. The wrong key is being used or the " +
			"expected payload is not built as per the protocol."
	}
}
