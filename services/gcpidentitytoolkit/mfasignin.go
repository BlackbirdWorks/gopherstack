package gcpidentitytoolkit

import (
	"context"
	"errors"
	"slices"
	"strings"

	idtkv2 "google.golang.org/api/identitytoolkit/v2"
)

// mfaPendingLocked turns a first-factor success into a pending credential when the account has second factors.
func (b *InMemoryBackend) mfaPendingLocked(sc *scope, u *userRec, provider string) (*tokenResponse, bool) {
	if len(u.MFA) == 0 || !mfaConfigured(sc) || provider == phoneProv {
		return nil, false
	}

	key := b.newSessionLocked(sc, &oobRec{RequestType: reqMfaPending, UID: u.UID, Provider: provider}, mfaPendingTTL)

	return &tokenResponse{
		LocalID: u.UID, Email: u.Email, DisplayName: u.DisplayName,
		MfaPendingCredential: key, MfaInfo: mfaWires(u.MFA, true),
	}, true
}

func (b *InMemoryBackend) pendingLocked(sc *scope, key string) (*oobRec, *userRec, error) {
	if key == "" {
		return nil, nil, idtk("MISSING_MFA_PENDING_CREDENTIAL")
	}

	rec, err := b.sessionLocked(sc, key, reqMfaPending)
	if err != nil {
		return nil, nil, idtk("INVALID_MFA_PENDING_CREDENTIAL")
	}

	u, ok := sc.users[rec.UID]
	if !ok {
		return nil, nil, idtk("USER_NOT_FOUND")
	}

	return rec, u, nil
}

func enrollmentFor(u *userRec, id string) (*mfaRec, error) {
	if id == "" && len(u.MFA) == 1 {
		return &u.MFA[0], nil
	}

	idx := slices.IndexFunc(u.MFA, func(m mfaRec) bool { return m.ID == id })
	if idx < 0 {
		return nil, idtk("MFA_ENROLLMENT_NOT_FOUND")
	}

	return &u.MFA[idx], nil
}

// MfaSignInStart sends the SMS code of a phone second factor.
func (b *InMemoryBackend) MfaSignInStart(ctx context.Context, c call, r *startSignInReq) (*startSignInResp, error) {
	b.mu.Lock("MfaSignInStart")
	defer b.mu.Unlock()

	tenant := c.tenant
	if tenant == "" {
		tenant = r.TenantId
	}

	sc, err := b.scopeLocked(c.project, tenant)
	if err != nil {
		return nil, err
	}

	_, u, err := b.pendingLocked(sc, r.MfaPendingCredential)
	if err != nil {
		return nil, err
	}

	m, err := enrollmentFor(u, r.MfaEnrollmentId)
	if err != nil {
		return nil, err
	}

	if m.Kind != kindPhone || r.PhoneSignInInfo == nil {
		return nil, idtk("INVALID_MFA_ENROLLMENT", "The enrollment is not an SMS factor.")
	}

	info := *r.PhoneSignInInfo
	info.PhoneNumber = m.Phone

	if err = phoneRequestOK(&info); err != nil {
		return nil, err
	}

	key, err := b.sendMfaSMSLocked(ctx, sc, reqMfaPhone, m.Phone, u.UID)
	if err != nil {
		return nil, err
	}

	return &startSignInResp{PhoneResponseInfo: &idtkv2.GoogleCloudIdentitytoolkitV2StartMfaPhoneResponseInfo{
		SessionInfo: key,
	}}, nil
}

// MfaSignInFinalize verifies the second factor and issues the session.
func (b *InMemoryBackend) MfaSignInFinalize(c call, r *finalizeSignInReq) (*finalizeSignInRes, error) {
	b.mu.Lock("MfaSignInFinalize")
	defer b.mu.Unlock()

	tenant := c.tenant
	if tenant == "" {
		tenant = r.TenantId
	}

	sc, err := b.scopeLocked(c.project, tenant)
	if err != nil {
		return nil, err
	}

	pending, u, err := b.pendingLocked(sc, r.MfaPendingCredential)
	if err != nil {
		return nil, err
	}

	m, err := enrollmentFor(u, r.MfaEnrollmentId)
	if err != nil {
		return nil, err
	}

	out := &finalizeSignInRes{}
	if err = b.verifySecondFactorLocked(sc, u, m, pending, r, out); err != nil {
		return nil, err
	}

	delete(sc.proj.Codes, r.MfaPendingCredential)
	b.touchLoginLocked(u)

	s, err := b.issueLocked(sc, u, pending.Provider, map[string]any{
		"sign_in_second_factor": m.Kind, "second_factor_identifier": m.ID,
	})
	if err != nil {
		return nil, err
	}

	out.IdToken, out.RefreshToken = s.IDToken, s.RefreshToken

	return out, nil
}

func (b *InMemoryBackend) verifySecondFactorLocked(
	sc *scope, u *userRec, m *mfaRec, pending *oobRec, r *finalizeSignInReq, out *finalizeSignInRes,
) error {
	key := r.MfaPendingCredential

	switch {
	case m.Kind == kindTotp && r.TotpVerificationInfo != nil:
		secret, err := decodeB32(m.Secret)
		if err != nil {
			return idtk("INVALID_CODE")
		}

		step, ok := totpMatch(secret, r.TotpVerificationInfo.VerificationCode, b.now(), sc.adjacentIntervals())
		if !ok || step <= m.lastStep {
			return wrongGuessLocked(sc, key, pending)
		}

		m.lastStep = step
	case m.Kind == kindPhone && r.PhoneVerificationInfo != nil:
		info := r.PhoneVerificationInfo

		sess, err := b.phoneSessionCheckLocked(sc, reqMfaPhone, info.SessionInfo, info.Code)
		if err != nil {
			if isCodeFailure(err) {
				_ = wrongGuessLocked(sc, key, pending)
			}

			return err
		}

		if sess.UID != u.UID || sess.Phone != m.Phone {
			return idtk("INVALID_SESSION_INFO")
		}

		delete(sc.proj.Codes, info.SessionInfo)
		out.PhoneAuthInfo = &idtkv2.GoogleCloudIdentitytoolkitV2FinalizeMfaPhoneResponseInfo{PhoneNumber: m.Phone}
	default:
		return idtk("MISSING_MFA_ENROLLMENT_INFO")
	}

	return nil
}

func isCodeFailure(err error) bool {
	var ae *APIError

	return errors.As(err, &ae) &&
		(strings.HasPrefix(ae.Message, "INVALID_CODE") || strings.HasPrefix(ae.Message, "TOO_MANY_ATTEMPTS"))
}
