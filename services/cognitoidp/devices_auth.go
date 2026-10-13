package cognitoidp

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math/big"
	"time"

	"github.com/google/uuid"
)

const (
	challengeDeviceSRPAuth         = "DEVICE_SRP_AUTH"
	challengeDevicePasswordVerifer = "DEVICE_PASSWORD_VERIFIER"
	deviceKeyParam                 = "DEVICE_KEY"
	deviceGroupKeyLen              = 9
)

// NewDeviceMetadata is the AuthenticationResult.NewDeviceMetadata of a sign-in from an unrecognized device.
type NewDeviceMetadata struct {
	DeviceGroupKey string
	DeviceKey      string
}

// deviceGroupKey derives the stable per-user device group key.
func deviceGroupKey(poolID, sub string) string {
	sum := sha256.Sum256([]byte(poolID + ":" + sub))

	return "-" + base64.RawURLEncoding.EncodeToString(sum[:])[:deviceGroupKeyLen]
}

func deviceTrackingEnabled(pool *UserPool) bool { return pool.Settings.DeviceConfiguration != nil }

func (b *InMemoryBackend) userDeviceLocked(poolID, username, deviceKey string) *Device {
	if deviceKey == "" {
		return nil
	}

	return b.devices[userStateKey(poolID, username)][deviceKey]
}

// NewDeviceMetadataFor returns fresh device metadata when the pool remembers devices and the
// signed-in user did not present a registered device key; nil otherwise.
func (b *InMemoryBackend) NewDeviceMetadataFor(poolID, username, deviceKey string) *NewDeviceMetadata {
	b.mu.RLock("NewDeviceMetadataFor")
	defer b.mu.RUnlock()

	pool, ok := b.pools.Get(poolID)
	if !ok || !deviceTrackingEnabled(pool) || b.userDeviceLocked(poolID, username, deviceKey) != nil {
		return nil
	}

	user, ok := b.users.Get(userKey(poolID, username))
	if !ok {
		return nil
	}

	return &NewDeviceMetadata{
		DeviceGroupKey: deviceGroupKey(poolID, user.Sub),
		DeviceKey:      b.region + "_" + uuid.NewString(),
	}
}

// ConfirmDeviceWithVerifier is ConfirmDevice that also registers the device's SRP verifier.
func (b *InMemoryBackend) ConfirmDeviceWithVerifier(
	accessToken, deviceKey, deviceName string, verifierConfig map[string]string,
) (string, bool, error) {
	verifier, salt := verifierConfig["PasswordVerifier"], verifierConfig["Salt"]

	for _, v := range []string{verifier, salt} {
		if v == "" {
			continue
		}

		if _, err := base64.StdEncoding.DecodeString(v); err != nil {
			return "", false, fmt.Errorf("%w: DeviceSecretVerifierConfig must be base64", ErrInvalidParameter)
		}
	}

	key, necessary, err := b.ConfirmDevice(accessToken, deviceKey, deviceName)
	if err != nil || (verifier == "" && salt == "") {
		return key, necessary, err
	}

	b.mu.Lock("ConfirmDeviceVerifier")
	defer b.mu.Unlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return "", false, err
	}

	if dev := b.userDeviceLocked(user.UserPoolID, user.Username, key); dev != nil {
		dev.PasswordVerifier, dev.Salt = verifier, salt
	}

	return key, necessary, nil
}

// deviceChallengeLocked returns a DEVICE_SRP_AUTH challenge when the sign-in named a
// registered device of a device-remembering pool; nil otherwise.
func (b *InMemoryBackend) deviceChallengeLocked(
	pool *UserPool, clientID string, user *User, deviceKey string,
) *AuthResult {
	dev := b.userDeviceLocked(pool.ID, user.Username, deviceKey)
	if dev == nil || dev.PasswordVerifier == "" || !deviceTrackingEnabled(pool) {
		return nil
	}

	session := randomAlphanumeric(mfaSessionLen)
	b.mfaSessions[session] = &mfaSessionEntry{
		PoolID: pool.ID, ClientID: clientID, Username: user.Username,
		ChallengeType: challengeDeviceSRPAuth, DeviceKey: deviceKey,
		ExpiresAt: time.Now().Add(mfaSessionTTL),
	}

	return &AuthResult{MFASession: session, ChallengeName: challengeDeviceSRPAuth}
}

func (b *InMemoryBackend) liveDeviceSessionLocked(
	session, clientID, challenge string,
) (*mfaSessionEntry, error) {
	entry, ok := b.mfaSessions[session]
	if !ok || (!entry.ExpiresAt.IsZero() && time.Now().After(entry.ExpiresAt)) {
		delete(b.mfaSessions, session)

		return nil, fmt.Errorf("%w: device session not found or expired", ErrNotAuthorized)
	}

	if entry.ChallengeType != challenge || entry.ClientID != clientID {
		return nil, fmt.Errorf("%w: session is not a %s challenge for this client", ErrNotAuthorized, challenge)
	}

	return entry, nil
}

// RespondToDeviceSRPChallenge answers DEVICE_SRP_AUTH (SRP_A) with a DEVICE_PASSWORD_VERIFIER challenge.
func (b *InMemoryBackend) RespondToDeviceSRPChallenge(
	clientID, session string, responses map[string]string,
) (*AuthResult, error) {
	b.mu.Lock("RespondToDeviceSRPChallenge")
	defer b.mu.Unlock()

	entry, err := b.liveDeviceSessionLocked(session, clientID, challengeDeviceSRPAuth)
	if err != nil {
		return nil, err
	}

	if responses[deviceKeyParam] != entry.DeviceKey {
		return nil, fmt.Errorf("%w: DEVICE_KEY does not match the sign-in", ErrNotAuthorized)
	}

	dev := b.userDeviceLocked(entry.PoolID, entry.Username, entry.DeviceKey)
	if dev == nil {
		return nil, fmt.Errorf("%w: device %q not found", ErrNotAuthorized, entry.DeviceKey)
	}

	aPub, ok := new(big.Int).SetString(responses["SRP_A"], hexBase)
	if !ok || aPub.Sign() <= 0 || new(big.Int).Mod(aPub, srpN()).Sign() == 0 {
		return nil, fmt.Errorf("%w: invalid SRP_A", ErrInvalidParameter)
	}

	verifierBytes, vErr := base64.StdEncoding.DecodeString(dev.PasswordVerifier)
	saltBytes, sErr := base64.StdEncoding.DecodeString(dev.Salt)

	if vErr != nil || sErr != nil {
		return nil, fmt.Errorf("%w: corrupt device verifier", ErrNotAuthorized)
	}

	bPriv, err := srpRandomExponent()
	if err != nil {
		return nil, err
	}

	bPub := srpServerB(new(big.Int).SetBytes(verifierBytes), bPriv)

	secretBlock := make([]byte, srpSecretBlockLen)
	if _, readErr := rand.Read(secretBlock); readErr != nil {
		return nil, fmt.Errorf("generating SRP secret block: %w", readErr)
	}

	delete(b.mfaSessions, session)

	next := randomAlphanumeric(mfaSessionLen)
	verifierEntry := &mfaSessionEntry{
		PoolID: entry.PoolID, ClientID: clientID, Username: entry.Username,
		ChallengeType: challengeDevicePasswordVerifer, DeviceKey: entry.DeviceKey,
		ExpiresAt:      time.Now().Add(mfaSessionTTL),
		SRPA:           hex.EncodeToString(srpPadHex(aPub)),
		SRPb:           hex.EncodeToString(srpPadHex(bPriv)),
		SRPB:           hex.EncodeToString(srpPadHex(bPub)),
		SRPSecretBlock: base64.StdEncoding.EncodeToString(secretBlock),
	}
	b.mfaSessions[next] = verifierEntry

	return &AuthResult{
		MFASession:    next,
		ChallengeName: challengeDevicePasswordVerifer,
		ChallengeParameters: map[string]string{
			"SALT":         hex.EncodeToString(saltBytes),
			"SRP_B":        verifierEntry.SRPB,
			"SECRET_BLOCK": verifierEntry.SRPSecretBlock,
			"USERNAME":     entry.Username,
		},
	}, nil
}

// RespondToDevicePasswordVerifier verifies the device's password claim and completes sign-in.
func (b *InMemoryBackend) RespondToDevicePasswordVerifier(
	clientID, session string, responses map[string]string, meta ...ClientMetadata,
) (*AuthResult, error) {
	b.mu.Lock("RespondToDevicePasswordVerifier")
	defer b.mu.Unlock()

	entry, err := b.liveDeviceSessionLocked(session, clientID, challengeDevicePasswordVerifer)
	if err != nil {
		return nil, err
	}

	pool, ok := b.pools.Get(entry.PoolID)
	if !ok {
		return nil, fmt.Errorf("%w: user pool %q not found", ErrUserPoolNotFound, entry.PoolID)
	}

	user, ok := b.users.Get(userKey(entry.PoolID, entry.Username))
	if !ok {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, entry.Username)
	}

	dev := b.userDeviceLocked(entry.PoolID, entry.Username, entry.DeviceKey)
	if dev == nil {
		return nil, fmt.Errorf("%w: device %q not found", ErrNotAuthorized, entry.DeviceKey)
	}

	verifierBytes, vErr := base64.StdEncoding.DecodeString(dev.PasswordVerifier)
	if vErr != nil {
		return nil, fmt.Errorf("%w: corrupt device verifier", ErrNotAuthorized)
	}

	verifierHex := new(big.Int).SetBytes(verifierBytes).Text(hexBase)

	if err = verifySRPClaim(
		entry,
		verifierHex,
		deviceGroupKey(pool.ID, user.Sub),
		entry.DeviceKey,
		responses,
	); err != nil {
		return nil, err
	}

	delete(b.mfaSessions, session)

	now := time.Now()
	dev.LastAuthenticatedAt, dev.LastModifiedAt = now, now

	cfg := pool.Settings.DeviceConfiguration
	challengeNew, _ := cfg["ChallengeRequiredOnNewDevice"].(bool)
	skipMFA := challengeNew && dev.Status == DeviceStatusRemembered

	return b.finishFirstFactorLocked(pool, clientID, user, firstMetadata(meta), skipMFA)
}
