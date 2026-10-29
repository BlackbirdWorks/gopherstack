package gcpidentitytoolkit

import (
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	reqPasswordReset = "PASSWORD_RESET"
	reqVerifyEmail   = "VERIFY_EMAIL"
	reqEmailSignin   = "EMAIL_SIGNIN"
	reqVerifyChange  = "VERIFY_AND_CHANGE_EMAIL"
	oobBytes         = 16
	resetTTL         = time.Hour
	signinTTL        = 6 * time.Hour
	verifyTTL        = 3 * 24 * time.Hour
)

//nolint:gochecknoglobals // immutable lookup tables
var (
	oobModes = map[string]string{
		reqPasswordReset: opResetPassword, reqVerifyEmail: "verifyEmail",
		reqEmailSignin: "signIn", reqVerifyChange: "verifyAndChangeEmail",
	}
	oobTTL = map[string]time.Duration{
		reqPasswordReset: resetTTL, reqVerifyEmail: verifyTTL,
		reqEmailSignin: signinTTL, reqVerifyChange: verifyTTL,
	}
)

func (b *InMemoryBackend) revokeLocked(u *userRec) { u.ValidSince = b.now().Unix() }

// Update modifies an account: self-service with an ID token, by oobCode, or by uid for admins.
func (b *InMemoryBackend) Update(c call, r *authRequest) (*setAccountResponse, error) {
	b.mu.Lock("Update")
	defer b.mu.Unlock()

	sc, err := b.scopeLocked(c.project, effectiveTenant(c, r))
	if err != nil {
		return nil, err
	}

	if r.OobCode != "" {
		return b.applyOobUpdateLocked(sc, r.OobCode)
	}

	u, err := b.updateTargetLocked(sc, c, r)
	if err != nil {
		return nil, err
	}

	selfService := r.IDToken != ""

	stale, err := b.applyUpdateLocked(sc, u, r, c.admin && !selfService)
	if err != nil {
		return nil, err
	}

	syncProviders(u)

	out := updateResponse(u)

	if selfService && stale && r.ReturnSecureToken != nil && *r.ReturnSecureToken {
		s, iErr := b.issueLocked(sc, u, passwordProv, nil)
		if iErr != nil {
			return nil, iErr
		}

		out.IDToken, out.RefreshToken, out.ExpiresIn = s.IDToken, s.RefreshToken, expiresInSecs
	}

	return out, nil
}

func (b *InMemoryBackend) updateTargetLocked(sc *scope, c call, r *authRequest) (*userRec, error) {
	if r.IDToken != "" {
		u, _, err := b.userForTokenLocked(sc, r.IDToken)

		return u, err
	}

	if !c.admin {
		return nil, idtk("MISSING_ID_TOKEN")
	}

	if r.LocalID.first() == "" {
		return nil, idtk("MISSING_LOCAL_ID")
	}

	u, ok := sc.users[r.LocalID.first()]
	if !ok {
		return nil, idtk("USER_NOT_FOUND")
	}

	return u, nil
}

func updateResponse(u *userRec) *setAccountResponse {
	out := &setAccountResponse{
		Kind: "identitytoolkit#SetAccountInfoResponse", LocalID: u.UID, Email: u.Email,
		DisplayName: u.DisplayName, PhotoURL: u.PhotoURL, EmailVerified: u.EmailVerified,
	}

	if len(u.PasswordHash) > 0 {
		out.PasswordHash = redactedHash
	}

	for _, p := range u.Providers {
		out.ProviderUserInfo = append(out.ProviderUserInfo, providerWire(p))
	}

	return out
}

// applyUpdateLocked validates every change before mutating, so a rejected update leaves the account untouched.
func (b *InMemoryBackend) applyUpdateLocked(sc *scope, u *userRec, r *authRequest, admin bool) (bool, error) {
	claims, err := b.validateUpdateLocked(sc, u, r, admin)
	if err != nil {
		return false, err
	}

	stale := false

	if newEmail := strs.Fold(r.Email.first()); newEmail != "" && newEmail != u.Email {
		u.Email, u.EmailVerified, stale = newEmail, false, true
	}

	if admin && r.EmailVerified != nil {
		u.EmailVerified = *r.EmailVerified
	}

	if r.Password != "" {
		if err = b.setPasswordLocked(sc, u, r.Password); err != nil {
			return false, err
		}

		b.revokeLocked(u)

		stale, u.Anonymous = true, false
	}

	if r.DisplayName != nil {
		u.DisplayName = *r.DisplayName
	}

	if r.PhotoURL != nil {
		u.PhotoURL = *r.PhotoURL
	}

	b.deleteAttributes(u, r.DeleteAttribute, r.DeleteProvider)

	if admin {
		b.applyAdminFieldsLocked(u, r, r.PhoneNumber.first(), claims)
	}

	return stale, nil
}

// validateUpdateLocked checks an update without mutating; it returns the parsed custom claims.
func (b *InMemoryBackend) validateUpdateLocked(sc *scope, u *userRec, r *authRequest, admin bool) (string, error) {
	if err := validateSelfServiceUpdate(sc, u, r); err != nil {
		return "", err
	}

	if !admin {
		return "", nil
	}

	if err := validateAdminUpdate(sc, u, r); err != nil {
		return "", err
	}

	if r.CustomAttributes != nil {
		return parseClaims(*r.CustomAttributes)
	}

	return "", nil
}

func validateSelfServiceUpdate(sc *scope, u *userRec, r *authRequest) error {
	newEmail := strs.Fold(r.Email.first())

	if newEmail != "" && newEmail != u.Email {
		if !validEmail(newEmail) {
			return idtk("INVALID_EMAIL")
		}

		if o := sc.byEmail(newEmail); o != nil && o.UID != u.UID {
			return idtk("EMAIL_EXISTS")
		}
	}

	if r.Password == "" {
		return nil
	}

	if err := checkPassword(sc, r.Password); err != nil {
		return err
	}

	if u.Email == "" && newEmail == "" {
		return idtk("MISSING_EMAIL")
	}

	return nil
}

func validateAdminUpdate(sc *scope, u *userRec, r *authRequest) error {
	phone := r.PhoneNumber.first()
	if phone == "" {
		return nil
	}

	if !e164.MatchString(phone) {
		return idtk("INVALID_PHONE_NUMBER", "Invalid format.")
	}

	if o := sc.byPhone(phone); o != nil && o.UID != u.UID {
		return idtk("PHONE_NUMBER_EXISTS")
	}

	return nil
}

func (b *InMemoryBackend) deleteAttributes(u *userRec, attrs, providers []string) {
	for _, a := range attrs {
		switch a {
		case "DISPLAY_NAME":
			u.DisplayName = ""
		case "PHOTO_URL":
			u.PhotoURL = ""
		case "PASSWORD":
			u.PasswordHash, u.Salt, u.Hash = nil, nil, nil
		case "EMAIL":
			u.Email, u.EmailVerified = "", false
			u.PasswordHash, u.Salt, u.Hash = nil, nil, nil
		}
	}

	for _, p := range providers {
		switch p {
		case phoneProv:
			u.Phone = ""
		case passwordProv:
			u.PasswordHash, u.Salt, u.Hash = nil, nil, nil
		default:
			u.Providers = slices.DeleteFunc(u.Providers, func(x providerRec) bool { return x.ProviderID == p })
		}
	}
}

func (b *InMemoryBackend) applyAdminFieldsLocked(u *userRec, r *authRequest, phone, claims string) {
	if phone != "" {
		u.Phone = phone
	}

	if r.DisableUser != nil {
		u.Disabled = *r.DisableUser
	}

	if r.CustomAttributes != nil {
		u.CustomAttrs = claims
	}

	if r.ValidSince > 0 {
		u.ValidSince = int64(r.ValidSince)
	}

	if lp := r.LinkProvider; lp != nil && lp.ProviderID != "" && lp.RawID != "" {
		u.Providers = slices.DeleteFunc(u.Providers, func(x providerRec) bool { return x.ProviderID == lp.ProviderID })
		u.Providers = append(u.Providers, providerRec(*lp))
	}
}

func (b *InMemoryBackend) applyOobUpdateLocked(sc *scope, code string) (*setAccountResponse, error) {
	rec, err := b.oobLocked(sc, code)
	if err != nil {
		return nil, err
	}

	if rec.RequestType != reqVerifyEmail && rec.RequestType != reqVerifyChange {
		return nil, idtk("INVALID_OOB_CODE")
	}

	u := sc.byEmail(rec.Email)
	if u == nil {
		return nil, idtk("EMAIL_NOT_FOUND")
	}

	out := &setAccountResponse{Kind: "identitytoolkit#SetAccountInfoResponse", LocalID: u.UID}

	if rec.RequestType == reqVerifyChange {
		n := strs.Fold(rec.NewEmail)
		if o := sc.byEmail(n); o != nil && o.UID != u.UID {
			return nil, idtk("EMAIL_EXISTS")
		}

		u.Email = n
		b.revokeLocked(u)
		syncProviders(u)
	}

	u.EmailVerified = true
	out.Email, out.EmailVerified = u.Email, true

	delete(sc.proj.Codes, code)

	return out, nil
}

// oobLocked looks up a live out-of-band code within the scope.
func (b *InMemoryBackend) oobLocked(sc *scope, code string) (*oobRec, error) {
	rec, ok := sc.proj.Codes[code]
	if !ok || rec.TenantID != sc.tenant {
		return nil, idtk("INVALID_OOB_CODE")
	}

	if b.now().UnixMilli() > rec.Expires {
		delete(sc.proj.Codes, code)

		return nil, idtk("EXPIRED_OOB_CODE")
	}

	return rec, nil
}

// SendOobCode records a reset, verification or sign-in code; no email is sent.
func (b *InMemoryBackend) SendOobCode(c call, r *authRequest) (*oobResponse, error) {
	if _, ok := oobModes[r.RequestType]; !ok {
		return nil, idtk("INVALID_REQ_TYPE", "Unsupported request type.")
	}

	b.mu.Lock("SendOobCode")
	defer b.mu.Unlock()

	sc, err := b.scopeLocked(c.project, effectiveTenant(c, r))
	if err != nil {
		return nil, err
	}

	email, silent, err := b.oobTargetLocked(sc, c, r)
	if err != nil {
		return nil, err
	}

	out := &oobResponse{Kind: "identitytoolkit#GetOobConfirmationCodeResponse", Email: email}
	if silent {
		return out, nil
	}

	code := randomToken(oobBytes)
	link := oobLink(c.host, oobModes[r.RequestType], code, r.ContinueURL, sc.tenant)
	now := b.now()

	sc.proj.Codes[code] = &oobRec{
		Email: email, NewEmail: strs.Fold(r.NewEmail), RequestType: r.RequestType, TenantID: sc.tenant,
		ContinueURL: r.ContinueURL, Link: link, Created: now.UnixMilli(),
		Expires: now.Add(oobTTL[r.RequestType]).UnixMilli(),
	}

	if r.ReturnOobLink && c.admin {
		out.OobCode, out.OobLink = code, link
	}

	return out, nil
}

// oobTargetLocked resolves the address a code is for; silent means enumeration protection swallowed the request.
func (b *InMemoryBackend) oobTargetLocked(sc *scope, c call, r *authRequest) (string, bool, error) {
	email := strs.Fold(r.Email.first())

	switch r.RequestType {
	case reqPasswordReset:
		if !sc.password {
			return "", false, idtk("OPERATION_NOT_ALLOWED", "Password sign-in is disabled for this project.")
		}

		if email == "" {
			return "", false, idtk("MISSING_EMAIL")
		}

		if !validEmail(email) {
			return "", false, idtk("INVALID_EMAIL")
		}

		if sc.byEmail(email) == nil {
			if sc.privacy && !c.admin {
				return email, true, nil
			}

			return "", false, idtk("EMAIL_NOT_FOUND")
		}
	case reqEmailSignin:
		if !sc.link {
			return "", false, idtk("OPERATION_NOT_ALLOWED", "Email link sign-in is disabled for this project.")
		}

		if email == "" {
			return "", false, idtk("MISSING_EMAIL")
		}

		if !validEmail(email) {
			return "", false, idtk("INVALID_EMAIL")
		}
	default:
		return b.verifyTargetLocked(sc, c, r, email)
	}

	return email, false, nil
}

func (b *InMemoryBackend) verifyTargetLocked(sc *scope, c call, r *authRequest, email string) (string, bool, error) {
	if r.IDToken != "" {
		u, _, err := b.userForTokenLocked(sc, r.IDToken)
		if err != nil {
			return "", false, err
		}

		email = u.Email
	} else if !c.admin {
		return "", false, idtk("MISSING_ID_TOKEN")
	}

	if email == "" {
		return "", false, idtk("MISSING_EMAIL")
	}

	if sc.byEmail(email) == nil {
		return "", false, idtk("EMAIL_NOT_FOUND")
	}

	if r.RequestType == reqVerifyChange {
		n := strs.Fold(r.NewEmail)
		if n == "" || !validEmail(n) {
			return "", false, idtk("INVALID_NEW_EMAIL")
		}

		if sc.byEmail(n) != nil {
			return "", false, idtk("EMAIL_EXISTS")
		}
	}

	return email, false, nil
}

// ResetPassword checks (no newPassword) or applies (newPassword) a password reset code.
func (b *InMemoryBackend) ResetPassword(c call, r *authRequest) (map[string]any, error) {
	b.mu.Lock("ResetPassword")
	defer b.mu.Unlock()

	sc, err := b.scopeLocked(c.project, effectiveTenant(c, r))
	if err != nil {
		return nil, err
	}

	if r.OobCode == "" {
		return nil, idtk("MISSING_OOB_CODE")
	}

	rec, err := b.oobLocked(sc, r.OobCode)
	if err != nil {
		return nil, err
	}

	out := map[string]any{
		keyKind: "identitytoolkit#ResetPasswordResponse", "email": rec.Email, "requestType": rec.RequestType,
	}

	if rec.RequestType == reqVerifyChange {
		out["newEmail"], out["email"] = rec.NewEmail, rec.Email
	}

	if r.NewPassword == "" {
		return out, nil
	}

	if rec.RequestType != reqPasswordReset {
		return nil, idtk("INVALID_OOB_CODE")
	}

	u := sc.byEmail(rec.Email)
	if u == nil {
		return nil, idtk("EMAIL_NOT_FOUND")
	}

	if err = checkPassword(sc, r.NewPassword); err != nil {
		return nil, err
	}

	if err = b.setPasswordLocked(sc, u, r.NewPassword); err != nil {
		return nil, err
	}

	u.EmailVerified = true

	b.revokeLocked(u)
	syncProviders(u)
	delete(sc.proj.Codes, r.OobCode)

	return out, nil
}

// SignInWithEmailLink completes passwordless sign-in, creating the account on first use.
func (b *InMemoryBackend) SignInWithEmailLink(c call, r *authRequest) (*tokenResponse, error) {
	b.mu.Lock("SignInWithEmailLink")
	defer b.mu.Unlock()

	sc, err := b.scopeLocked(c.project, effectiveTenant(c, r))
	if err != nil {
		return nil, err
	}

	if !sc.link {
		return nil, idtk("OPERATION_NOT_ALLOWED", "Email link sign-in is disabled for this project.")
	}

	if r.OobCode == "" {
		return nil, idtk("MISSING_OOB_CODE")
	}

	email := strs.Fold(r.Email.first())
	if email == "" {
		return nil, idtk("MISSING_EMAIL")
	}

	rec, err := b.oobLocked(sc, r.OobCode)
	if err != nil {
		return nil, err
	}

	if rec.RequestType != reqEmailSignin {
		return nil, idtk("INVALID_OOB_CODE")
	}

	if rec.Email != email {
		return nil, idtk("INVALID_EMAIL")
	}

	existing := sc.byEmail(email)
	created := existing == nil

	if created {
		existing, err = b.createUserLocked(sc, newUser{email: email, emailVerified: true}, false)
		if err != nil {
			return nil, err
		}
	} else {
		existing.EmailVerified = true
	}

	if existing.Disabled {
		return nil, idtk("USER_DISABLED", "The user account has been disabled by an administrator.")
	}

	delete(sc.proj.Codes, r.OobCode)

	resp, err := b.finishSignInLocked(sc, existing, passwordProv, "identitytoolkit#EmailLinkSigninResponse")
	if err != nil {
		return nil, err
	}

	resp.IsNewUser = created

	return resp, nil
}
