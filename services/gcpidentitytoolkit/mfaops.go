package gcpidentitytoolkit

import (
	"cmp"
	"context"
	"crypto/subtle"
	"slices"
	"time"

	"github.com/labstack/echo/v5"
	idtkv2 "google.golang.org/api/identitytoolkit/v2"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

type (
	startEnrollReq    = idtkv2.GoogleCloudIdentitytoolkitV2StartMfaEnrollmentRequest
	startEnrollResp   = idtkv2.GoogleCloudIdentitytoolkitV2StartMfaEnrollmentResponse
	finalizeEnrollReq = idtkv2.GoogleCloudIdentitytoolkitV2FinalizeMfaEnrollmentRequest
	finalizeEnrollRes = idtkv2.GoogleCloudIdentitytoolkitV2FinalizeMfaEnrollmentResponse
	withdrawReq       = idtkv2.GoogleCloudIdentitytoolkitV2WithdrawMfaRequest
	withdrawResp      = idtkv2.GoogleCloudIdentitytoolkitV2WithdrawMfaResponse
	startSignInReq    = idtkv2.GoogleCloudIdentitytoolkitV2StartMfaSignInRequest
	startSignInResp   = idtkv2.GoogleCloudIdentitytoolkitV2StartMfaSignInResponse
	finalizeSignInReq = idtkv2.GoogleCloudIdentitytoolkitV2FinalizeMfaSignInRequest
	finalizeSignInRes = idtkv2.GoogleCloudIdentitytoolkitV2FinalizeMfaSignInResponse
	phoneStartInfo    = idtkv2.GoogleCloudIdentitytoolkitV2StartMfaPhoneRequestInfo
)

func (b *InMemoryBackend) sessionLocked(sc *scope, key, kind string) (*oobRec, error) {
	rec, ok := sc.proj.Codes[key]
	if !ok || rec.RequestType != kind || rec.TenantID != sc.tenant {
		return nil, idtk("INVALID_SESSION_INFO")
	}

	if b.now().UnixMilli() > rec.Expires {
		delete(sc.proj.Codes, key)

		return nil, idtk("SESSION_EXPIRED")
	}

	return rec, nil
}

// wrongGuessLocked counts a failed code against rec and drops it once maxCodeAttempts is reached.
func wrongGuessLocked(sc *scope, key string, rec *oobRec) error {
	rec.attempts++
	if rec.attempts >= maxCodeAttempts {
		delete(sc.proj.Codes, key)

		return idtk("TOO_MANY_ATTEMPTS_TRY_LATER")
	}

	return idtk("INVALID_CODE")
}

// pruneSessionsLocked drops expired records and the oldest live ones of the same kind and user beyond the cap.
func (b *InMemoryBackend) pruneSessionsLocked(sc *scope, rec *oobRec) {
	nowMs := b.now().UnixMilli()

	var same []string

	for k, r := range sc.proj.Codes {
		if nowMs > r.Expires {
			delete(sc.proj.Codes, k)

			continue
		}

		if rec.UID != "" && r.UID == rec.UID && r.RequestType == rec.RequestType && r.TenantID == sc.tenant {
			same = append(same, k)
		}
	}

	if len(same) < maxLiveSessions {
		return
	}

	slices.SortFunc(same, func(a, c string) int {
		return cmp.Compare(sc.proj.Codes[a].Created, sc.proj.Codes[c].Created)
	})

	for _, k := range same[:len(same)-maxLiveSessions+1] {
		delete(sc.proj.Codes, k)
	}
}

func (b *InMemoryBackend) newSessionLocked(sc *scope, rec *oobRec, ttl time.Duration) string {
	b.pruneSessionsLocked(sc, rec)

	key := randomToken(oobBytes)
	now := b.now()
	rec.TenantID, rec.Created, rec.Expires = sc.tenant, now.UnixMilli(), now.Add(ttl).UnixMilli()
	sc.proj.Codes[key] = rec

	return key
}

func phoneRequestOK(info *phoneStartInfo) error {
	if info.PhoneNumber == "" {
		return idtk("MISSING_PHONE_NUMBER")
	}

	if !e164.MatchString(info.PhoneNumber) {
		return idtk("INVALID_PHONE_NUMBER", "Invalid format.")
	}

	if info.RecaptchaToken == "" && info.CaptchaResponse == "" && info.PlayIntegrityToken == "" &&
		info.SafetyNetToken == "" && (info.IosReceipt == "" || info.IosSecret == "") {
		return idtk("MISSING_CLIENT_IDENTIFIER")
	}

	return nil
}

func (b *InMemoryBackend) sendMfaSMSLocked(ctx context.Context, sc *scope, kind, phone, uid string) (string, error) {
	if err := b.smsAllowedLocked(sc, phone); err != nil {
		return "", err
	}

	code, ok := sc.testPhones[phone]
	if !ok {
		code = randomDigits()
	}

	key := b.newSessionLocked(sc, &oobRec{RequestType: kind, Phone: phone, VerifyCode: code, UID: uid}, mfaSessionTTL)

	logger.Load(ctx).InfoContext(ctx, "gcpidentitytoolkit: would send SMS (recorded, not delivered)",
		"to", phone, "project", sc.project, "purpose", "mfa")

	return key, nil
}

// MfaEnrollmentStart begins TOTP or SMS second-factor enrollment for a signed-in user.
func (b *InMemoryBackend) MfaEnrollmentStart(ctx context.Context, c call, r *startEnrollReq) (*startEnrollResp, error) {
	b.mu.Lock("MfaEnrollmentStart")
	defer b.mu.Unlock()

	sc, u, err := b.mfaUserLocked(c, r.TenantId, r.IdToken)
	if err != nil {
		return nil, err
	}

	if err = enrollmentAllowed(u); err != nil {
		return nil, err
	}

	switch {
	case r.TotpEnrollmentInfo != nil:
		return b.startTotpLocked(sc, u)
	case r.PhoneEnrollmentInfo != nil:
		if !sc.smsEnabled() {
			return nil, idtk("OPERATION_NOT_ALLOWED", "SMS second factor is not enabled.")
		}

		if err = phoneRequestOK(r.PhoneEnrollmentInfo); err != nil {
			return nil, err
		}

		if slices.ContainsFunc(u.MFA, func(m mfaRec) bool { return m.Phone == r.PhoneEnrollmentInfo.PhoneNumber }) {
			return nil, idtk("SECOND_FACTOR_EXISTS")
		}

		key, serr := b.sendMfaSMSLocked(ctx, sc, reqMfaPhone, r.PhoneEnrollmentInfo.PhoneNumber, u.UID)
		if serr != nil {
			return nil, serr
		}

		return &startEnrollResp{PhoneSessionInfo: &idtkv2.GoogleCloudIdentitytoolkitV2StartMfaPhoneResponseInfo{
			SessionInfo: key,
		}}, nil
	}

	return nil, idtk("MISSING_MFA_ENROLLMENT_INFO")
}

func (b *InMemoryBackend) mfaUserLocked(c call, tenantID, idToken string) (*scope, *userRec, error) {
	tenant := c.tenant
	if tenant == "" {
		tenant = tenantID
	}

	sc, err := b.scopeLocked(c.project, tenant)
	if err != nil {
		return nil, nil, err
	}

	if !mfaConfigured(sc) {
		return nil, nil, idtk("OPERATION_NOT_ALLOWED", "Multi-factor authentication is not enabled.")
	}

	if idToken == "" {
		return nil, nil, idtk("MISSING_ID_TOKEN")
	}

	u, _, err := b.userForTokenLocked(sc, idToken)
	if err != nil {
		return nil, nil, err
	}

	return sc, u, nil
}

func enrollmentAllowed(u *userRec) error {
	if u.Email != "" && !u.EmailVerified {
		return idtk("UNVERIFIED_EMAIL")
	}

	if len(u.MFA) >= maxEnrollments {
		return idtk("SECOND_FACTOR_LIMIT_EXCEEDED")
	}

	return nil
}

func (b *InMemoryBackend) startTotpLocked(sc *scope, u *userRec) (*startEnrollResp, error) {
	if sc.totpConfig() == nil {
		return nil, idtk("OPERATION_NOT_ALLOWED", "TOTP second factor is not enabled.")
	}

	secret := newTotpSecret()
	key := b.newSessionLocked(sc, &oobRec{RequestType: reqMfaTotp, UID: u.UID, Secret: b32(secret)}, mfaSessionTTL)
	deadline := b.now().Add(mfaSessionTTL).UTC().Format(timeLayoutRFC)

	return &startEnrollResp{TotpSessionInfo: &idtkv2.GoogleCloudIdentitytoolkitV2StartMfaTotpEnrollmentResponseInfo{
		SharedSecretKey: b32(secret), HashingAlgorithm: "SHA1", VerificationCodeLength: totpDigits,
		PeriodSec: totpPeriod, SessionInfo: key, FinalizeEnrollmentTime: deadline,
	}}, nil
}

func (b *InMemoryBackend) provider(vt *verifiedToken) string {
	if fb, ok := vt.claims["firebase"].(map[string]any); ok {
		if p, isStr := fb["sign_in_provider"].(string); isStr {
			return p
		}
	}

	return passwordProv
}

// MfaEnrollmentFinalize verifies the first code and records the enrollment.
func (b *InMemoryBackend) MfaEnrollmentFinalize(c call, r *finalizeEnrollReq) (*finalizeEnrollRes, error) {
	b.mu.Lock("MfaEnrollmentFinalize")
	defer b.mu.Unlock()

	sc, u, err := b.mfaUserLocked(c, r.TenantId, r.IdToken)
	if err != nil {
		return nil, err
	}

	if err = enrollmentAllowed(u); err != nil {
		return nil, err
	}

	rec := mfaRec{ID: newEnrollmentID(), DisplayName: r.DisplayName, EnrolledAt: b.nowMillis()}
	out := &finalizeEnrollRes{}

	switch {
	case r.TotpVerificationInfo != nil:
		if err = b.finalizeTotpLocked(sc, u, r.TotpVerificationInfo, &rec); err != nil {
			return nil, err
		}

		out.TotpAuthInfo = &idtkv2.GoogleCloudIdentitytoolkitV2FinalizeMfaTotpEnrollmentResponseInfo{}
	case r.PhoneVerificationInfo != nil:
		if err = b.finalizePhoneLocked(sc, u, r.PhoneVerificationInfo, &rec); err != nil {
			return nil, err
		}

		out.PhoneAuthInfo = &idtkv2.GoogleCloudIdentitytoolkitV2FinalizeMfaPhoneResponseInfo{PhoneNumber: rec.Phone}
	default:
		return nil, idtk("MISSING_MFA_ENROLLMENT_INFO")
	}

	u.MFA = append(u.MFA, rec)

	vt, _ := b.verifyJWTLocked(r.IdToken, sc.project, securetokenIssuer)
	s, err := b.issueLocked(sc, u, b.provider(vt), nil)
	if err != nil {
		return nil, err
	}

	out.IdToken, out.RefreshToken = s.IDToken, s.RefreshToken

	return out, nil
}

func (b *InMemoryBackend) finalizeTotpLocked(
	sc *scope, u *userRec, info *idtkv2.GoogleCloudIdentitytoolkitV2FinalizeMfaTotpEnrollmentRequestInfo, rec *mfaRec,
) error {
	if info.SessionInfo == "" {
		return idtk("MISSING_SESSION_INFO")
	}

	sess, err := b.sessionLocked(sc, info.SessionInfo, reqMfaTotp)
	if err != nil || sess.UID != u.UID {
		if err == nil {
			err = idtk("INVALID_SESSION_INFO")
		}

		return err
	}

	secret, derr := decodeB32(sess.Secret)
	if derr != nil {
		return idtk("INVALID_SESSION_INFO")
	}

	if info.VerificationCode == "" {
		return idtk("MISSING_CODE")
	}

	step, ok := totpMatch(secret, info.VerificationCode, b.now(), sc.adjacentIntervals())
	if !ok {
		return wrongGuessLocked(sc, info.SessionInfo, sess)
	}

	delete(sc.proj.Codes, info.SessionInfo)

	rec.Kind, rec.Secret, rec.lastStep = kindTotp, sess.Secret, step

	return nil
}

func (b *InMemoryBackend) finalizePhoneLocked(
	sc *scope, u *userRec, info *idtkv2.GoogleCloudIdentitytoolkitV2FinalizeMfaPhoneRequestInfo, rec *mfaRec,
) error {
	sess, err := b.phoneSessionCheckLocked(sc, reqMfaPhone, info.SessionInfo, info.Code)
	if err != nil {
		return err
	}

	if sess.UID != u.UID {
		return idtk("INVALID_SESSION_INFO")
	}

	delete(sc.proj.Codes, info.SessionInfo)

	rec.Kind, rec.Phone = kindPhone, sess.Phone

	return nil
}

func (b *InMemoryBackend) phoneSessionCheckLocked(sc *scope, kind, key, code string) (*oobRec, error) {
	if key == "" {
		return nil, idtk("MISSING_SESSION_INFO")
	}

	sess, err := b.sessionLocked(sc, key, kind)
	if err != nil {
		return nil, err
	}

	if code == "" {
		return nil, idtk("MISSING_CODE")
	}

	if subtle.ConstantTimeCompare([]byte(sess.VerifyCode), []byte(code)) != 1 {
		return nil, wrongGuessLocked(sc, key, sess)
	}

	return sess, nil
}

// MfaEnrollmentWithdraw removes one second factor.
func (b *InMemoryBackend) MfaEnrollmentWithdraw(c call, r *withdrawReq) (*withdrawResp, error) {
	b.mu.Lock("MfaEnrollmentWithdraw")
	defer b.mu.Unlock()

	sc, u, err := b.mfaUserLocked(c, r.TenantId, r.IdToken)
	if err != nil {
		return nil, err
	}

	idx := slices.IndexFunc(u.MFA, func(m mfaRec) bool { return m.ID == r.MfaEnrollmentId })
	if idx < 0 {
		return nil, idtk("MFA_ENROLLMENT_NOT_FOUND")
	}

	u.MFA = slices.Delete(u.MFA, idx, idx+1)

	vt, _ := b.verifyJWTLocked(r.IdToken, sc.project, securetokenIssuer)
	s, err := b.issueLocked(sc, u, b.provider(vt), nil)
	if err != nil {
		return nil, err
	}

	return &withdrawResp{IdToken: s.IDToken, RefreshToken: s.RefreshToken}, nil
}

func mfaDecode[T any](c *echo.Context, fn func(*T) (any, error)) (any, error) {
	req, _, err := readJSON[T](c)
	if err != nil {
		return nil, err
	}

	return fn(req)
}

func (h *Handler) mfaOp(c *echo.Context, op string, cl call) (any, error) {
	b, ctx := h.Backend, c.Request().Context()

	switch op {
	case opMfaEnrollStart:
		return mfaDecode(c, func(r *startEnrollReq) (any, error) { return b.MfaEnrollmentStart(ctx, cl, r) })
	case opMfaEnrollFinalize:
		return mfaDecode(c, func(r *finalizeEnrollReq) (any, error) { return b.MfaEnrollmentFinalize(cl, r) })
	case opMfaEnrollWithdraw:
		return mfaDecode(c, func(r *withdrawReq) (any, error) { return b.MfaEnrollmentWithdraw(cl, r) })
	case opMfaSignInStart:
		return mfaDecode(c, func(r *startSignInReq) (any, error) { return b.MfaSignInStart(ctx, cl, r) })
	}

	return mfaDecode(c, func(r *finalizeSignInReq) (any, error) { return b.MfaSignInFinalize(cl, r) })
}
