package gcpidentitytoolkit

import (
	"crypto/rand"
	"crypto/subtle"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"time"

	idtkv2 "google.golang.org/api/identitytoolkit/v2"
)

const (
	reqPhoneSession = "PHONE_SESSION"
	reqPhoneProof   = "PHONE_PROOF"
	phoneTTL        = 5 * time.Minute
	phoneCodeDigits = 1_000_000
)

// PhoneSignInResponse is the signInWithPhoneNumber response.
type PhoneSignInResponse struct {
	LocalID                 string `json:"localId,omitempty"`
	IDToken                 string `json:"idToken,omitempty"`
	RefreshToken            string `json:"refreshToken,omitempty"`
	ExpiresIn               string `json:"expiresIn,omitempty"`
	PhoneNumber             string `json:"phoneNumber"`
	TemporaryProof          string `json:"temporaryProof,omitempty"`
	TemporaryProofExpiresIn string `json:"temporaryProofExpiresIn,omitempty"`
	IsNewUser               bool   `json:"isNewUser,omitempty"`
}

// SessionResponse is the sendVerificationCode response.
type SessionResponse struct {
	SessionInfo string `json:"sessionInfo"`
}

// VerificationCode is one live phone verification code, as listed by the emulator endpoint.
type VerificationCode struct {
	PhoneNumber string `json:"phoneNumber"`
	Code        string `json:"code"`
	SessionInfo string `json:"sessionInfo"`
}

func phoneTestNumbers(in *idtkv2.GoogleCloudIdentitytoolkitAdminV2SignInConfig) map[string]string {
	if in == nil || in.PhoneNumber == nil {
		return nil
	}

	return in.PhoneNumber.TestPhoneNumbers
}

func randomDigits() string {
	n, err := rand.Int(rand.Reader, big.NewInt(phoneCodeDigits))
	if err != nil {
		return "000000"
	}

	return fmt.Sprintf("%06d", n.Int64())
}

func hasClientIdentifier(r *authRequest) bool {
	return r.RecaptchaToken != "" || r.CaptchaResponse != "" || r.PlayIntegrity != "" || r.SafetyNetToken != "" ||
		(r.IOSReceipt != "" && r.IOSSecret != "")
}

// SendVerificationCode starts phone sign-in; no SMS is sent and the code is read from the emulator endpoint.
func (b *InMemoryBackend) SendVerificationCode(c call, r *authRequest) (*SessionResponse, error) {
	b.mu.Lock("SendVerificationCode")
	defer b.mu.Unlock()

	sc, err := b.scopeLocked(c.project, effectiveTenant(c, r))
	if err != nil {
		return nil, err
	}

	if !sc.phone {
		return nil, idtk("OPERATION_NOT_ALLOWED", "Phone number sign-in is disabled for this project.")
	}

	phone := r.PhoneNumber.first()
	if phone == "" {
		return nil, idtk("MISSING_PHONE_NUMBER")
	}

	if !e164.MatchString(phone) {
		return nil, idtk("INVALID_PHONE_NUMBER", "Invalid format.")
	}

	if !hasClientIdentifier(r) {
		return nil, idtk("MISSING_CLIENT_IDENTIFIER")
	}

	code, ok := sc.testPhones[phone]
	if !ok {
		code = randomDigits()
	}

	session := randomToken(oobBytes)
	now := b.now()

	sc.proj.Codes[session] = &oobRec{
		RequestType: reqPhoneSession, TenantID: sc.tenant, Phone: phone, VerifyCode: code,
		Created: now.UnixMilli(), Expires: now.Add(phoneTTL).UnixMilli(),
	}

	return &SessionResponse{SessionInfo: session}, nil
}

// phoneLocked resolves the verified phone number of a sign-in request from its session code or temporary proof.
func (b *InMemoryBackend) phoneLocked(sc *scope, r *authRequest) (string, error) {
	if r.SessionInfo == "" && r.TemporaryProof == "" {
		return "", idtk("MISSING_SESSION_INFO")
	}

	if r.SessionInfo != "" {
		rec, err := b.phoneRecLocked(sc, r.SessionInfo, reqPhoneSession, "INVALID_SESSION_INFO", "SESSION_EXPIRED")
		if err != nil {
			return "", err
		}

		if r.Code == "" {
			return "", idtk("MISSING_CODE")
		}

		if subtle.ConstantTimeCompare([]byte(rec.VerifyCode), []byte(r.Code)) != 1 {
			return "", idtk("INVALID_CODE")
		}

		delete(sc.proj.Codes, r.SessionInfo)

		return rec.Phone, nil
	}

	rec, err := b.phoneRecLocked(
		sc,
		r.TemporaryProof,
		reqPhoneProof,
		"INVALID_TEMPORARY_PROOF",
		"TEMPORARY_PROOF_EXPIRED",
	)
	if err != nil {
		return "", err
	}

	if p := r.PhoneNumber.first(); p == "" || p != rec.Phone {
		return "", idtk("INVALID_PHONE_NUMBER", "The phone number does not match the temporary proof.")
	}

	return rec.Phone, nil
}

func (b *InMemoryBackend) phoneRecLocked(sc *scope, key, kind, invalid, expired string) (*oobRec, error) {
	rec, ok := sc.proj.Codes[key]
	if !ok || rec.RequestType != kind || rec.TenantID != sc.tenant {
		return nil, idtk(invalid)
	}

	if b.now().UnixMilli() > rec.Expires {
		delete(sc.proj.Codes, key)

		return nil, idtk(expired)
	}

	return rec, nil
}

// SignInWithPhoneNumber verifies a code (or temporary proof), then signs in, creates or links the account.
func (b *InMemoryBackend) SignInWithPhoneNumber(c call, r *authRequest) (*PhoneSignInResponse, error) {
	b.mu.Lock("SignInWithPhoneNumber")
	defer b.mu.Unlock()

	sc, err := b.scopeLocked(c.project, effectiveTenant(c, r))
	if err != nil {
		return nil, err
	}

	if !sc.phone {
		return nil, idtk("OPERATION_NOT_ALLOWED", "Phone number sign-in is disabled for this project.")
	}

	phone, err := b.phoneLocked(sc, r)
	if err != nil {
		return nil, err
	}

	var (
		u       *userRec
		created bool
	)

	if r.IDToken != "" {
		u, _, err = b.userForTokenLocked(sc, r.IDToken)
		if err != nil {
			return nil, err
		}

		if o := sc.byPhone(phone); o != nil && o.UID != u.UID {
			return b.temporaryProofLocked(sc, phone), nil
		}

		u.Phone = phone
		syncProviders(u)
	} else if u = sc.byPhone(phone); u == nil {
		u, err = b.createUserLocked(sc, newUser{phone: phone}, false)
		if err != nil {
			return nil, err
		}

		created = true
	}

	if u.Disabled {
		return nil, idtk("USER_DISABLED", "The user account has been disabled by an administrator.")
	}

	tr, err := b.finishSignInLocked(sc, u, phoneProv, "")
	if err != nil {
		return nil, err
	}

	return &PhoneSignInResponse{
		LocalID: u.UID, IDToken: tr.IDToken, RefreshToken: tr.RefreshToken, ExpiresIn: tr.ExpiresIn,
		PhoneNumber: phone, IsNewUser: created,
	}, nil
}

func (b *InMemoryBackend) temporaryProofLocked(sc *scope, phone string) *PhoneSignInResponse {
	proof := randomToken(oobBytes)
	now := b.now()

	sc.proj.Codes[proof] = &oobRec{
		RequestType: reqPhoneProof, TenantID: sc.tenant, Phone: phone,
		Created: now.UnixMilli(), Expires: now.Add(phoneTTL).UnixMilli(),
	}

	return &PhoneSignInResponse{
		PhoneNumber: phone, TemporaryProof: proof, TemporaryProofExpiresIn: strconv.Itoa(int(phoneTTL.Seconds())),
	}
}

// VerificationCodes lists the live phone verification codes of a project, oldest first.
func (b *InMemoryBackend) VerificationCodes(project, tenant string) ([]VerificationCode, error) {
	b.mu.Lock("VerificationCodes")
	defer b.mu.Unlock()

	sc, err := b.scopeLocked(project, tenant)
	if err != nil {
		return nil, err
	}

	type entry struct {
		rec     *oobRec
		session string
	}

	var list []entry

	for session, rec := range sc.proj.Codes {
		if rec.RequestType == reqPhoneSession && rec.TenantID == tenant {
			list = append(list, entry{rec, session})
		}
	}

	sort.Slice(list, func(i, j int) bool {
		if list[i].rec.Created != list[j].rec.Created {
			return list[i].rec.Created < list[j].rec.Created
		}

		return list[i].session < list[j].session
	})

	out := make([]VerificationCode, 0, len(list))
	for _, e := range list {
		out = append(out, VerificationCode{PhoneNumber: e.rec.Phone, Code: e.rec.VerifyCode, SessionInfo: e.session})
	}

	return out, nil
}
