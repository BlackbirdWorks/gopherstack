package lambda

import (
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

var (
	// ErrKMSNotFound maps to KMSNotFoundException.
	ErrKMSNotFound = errors.New("KMSNotFoundException")
	// ErrKMSDisabled maps to KMSDisabledException.
	ErrKMSDisabled = errors.New("KMSDisabledException")
	// ErrKMSInvalidState maps to KMSInvalidStateException.
	ErrKMSInvalidState = errors.New("KMSInvalidStateException")
	// ErrKMSAccessDenied maps to KMSAccessDeniedException.
	ErrKMSAccessDenied = errors.New("KMSAccessDeniedException")
)

const encContextFunctionArn = "aws:lambda:FunctionArn"

// KMSResolver is the in-process KMS used for function environment-variable encryption.
// Implementations return the ErrKMS* sentinels for unusable keys.
type KMSResolver interface {
	// CheckKey verifies keyARN names an enabled symmetric encrypt/decrypt key.
	CheckKey(keyARN string) error
	Encrypt(keyARN string, plaintext []byte, encContext map[string]string) ([]byte, error)
	Decrypt(keyARN string, ciphertext []byte, encContext map[string]string) ([]byte, error)
}

// SetKMSResolver wires environment-variable encryption.
func (b *InMemoryBackend) SetKMSResolver(r KMSResolver) {
	b.mu.Lock("SetKMSResolver")
	defer b.mu.Unlock()

	b.kmsResolver = r
}

func (b *InMemoryBackend) kmsResolverSnapshot() KMSResolver {
	b.mu.RLock("kmsResolver")
	defer b.mu.RUnlock()

	return b.kmsResolver
}

// kmsInvokeError reports the Lambda error code for a KMS failure, or "" for any other error.
func kmsInvokeError(err error) string {
	for _, s := range []error{ErrKMSNotFound, ErrKMSDisabled, ErrKMSInvalidState, ErrKMSAccessDenied} {
		if errors.Is(err, s) {
			return s.Error()
		}
	}

	return ""
}

// envCiphertextKey names the ciphertext slot for $LATEST (version "") or a published version.
func envCiphertextKey(name, version string) string {
	if version == "" || version == versionLatest {
		return name
	}

	return name + ":" + version
}

func functionEncContext(name string, b *InMemoryBackend) map[string]string {
	return map[string]string{encContextFunctionArn: buildARN(b.region, b.accountID, name)}
}

// sealEnvironment validates fn.KMSKeyArn and encrypts fn's env vars under it. It returns nil
// ciphertext when no resolver is wired or the function has no customer managed key.
func (b *InMemoryBackend) sealEnvironment(r KMSResolver, fn *FunctionConfiguration) (map[string]string, error) {
	if r == nil || fn.KMSKeyArn == "" {
		return nil, nil //nolint:nilnil // no ciphertext is a valid outcome
	}

	if !strings.HasPrefix(fn.KMSKeyArn, "arn:") {
		return nil, fmt.Errorf("%w: KMSKeyArn must be a KMS key ARN", ErrInvalidParameterValue)
	}

	if err := r.CheckKey(fn.KMSKeyArn); err != nil {
		return nil, kmsConfigError(err)
	}

	if fn.Environment == nil || len(fn.Environment.Variables) == 0 {
		return nil, nil //nolint:nilnil // nothing to encrypt
	}

	encCtx := functionEncContext(fn.FunctionName, b)
	ct := make(map[string]string, len(fn.Environment.Variables))

	for k, v := range fn.Environment.Variables {
		if v == "" {
			ct[k] = ""

			continue
		}

		blob, err := r.Encrypt(fn.KMSKeyArn, []byte(v), encCtx)
		if err != nil {
			return nil, kmsConfigError(err)
		}

		ct[k] = base64.StdEncoding.EncodeToString(blob)
	}

	return ct, nil
}

func kmsConfigError(err error) error {
	if code := kmsInvokeError(err); code != "" {
		return fmt.Errorf("%w: Lambda was unable to configure access to your environment variables "+
			"because the KMS key is not usable (%s): %w", ErrInvalidParameterValue, code, err)
	}

	return fmt.Errorf("%w: %w", ErrInvalidParameterValue, err)
}

// openEnvironment decrypts ct under keyARN.
func (b *InMemoryBackend) openEnvironment(
	r KMSResolver, fnName, keyARN string, ct map[string]string,
) (map[string]string, error) {
	if r == nil {
		return nil, fmt.Errorf("%w: KMS integration is not available", ErrLambdaUnavailable)
	}

	encCtx := functionEncContext(fnName, b)
	out := make(map[string]string, len(ct))

	for k, enc := range ct {
		if enc == "" {
			out[k] = ""

			continue
		}

		blob, err := base64.StdEncoding.DecodeString(enc)
		if err != nil {
			return nil, fmt.Errorf("%w: corrupt environment ciphertext: %w", ErrKMSAccessDenied, err)
		}

		pt, err := r.Decrypt(keyARN, blob, encCtx)
		if err != nil {
			return nil, err
		}

		out[k] = string(pt)
	}

	return out, nil
}

func (b *InMemoryBackend) ciphertextFor(name, version string) map[string]string {
	b.mu.RLock("ciphertextFor")
	defer b.mu.RUnlock()

	return b.envCiphertext[envCiphertextKey(name, version)]
}

// decryptInvokeEnv returns the plaintext environment for fn, decrypting under its CMK. It returns
// (nil, nil) when fn has no ciphertext (no CMK, or no resolver wired when it was written).
func (b *InMemoryBackend) decryptInvokeEnv(fn *FunctionConfiguration) (map[string]string, error) {
	ct := b.ciphertextFor(fn.FunctionName, fn.Version)
	if len(ct) == 0 || fn.KMSKeyArn == "" {
		return nil, nil //nolint:nilnil // nothing encrypted
	}

	return b.openEnvironment(b.kmsResolverSnapshot(), fn.FunctionName, fn.KMSKeyArn, ct)
}

// invokeEnvError returns the KMS error that must fail an invoke of fn, or nil.
func (b *InMemoryBackend) invokeEnvError(fn *FunctionConfiguration) error {
	_, err := b.decryptInvokeEnv(fn)

	return err
}

// invokeStatusForKMS is the HTTP status of the KMS* Invoke errors.
const invokeStatusForKMS = http.StatusBadGateway

// hydrateEnvironment (caller holds b.mu) fills clone's Environment from ciphertext when a restore left it empty.
// Decrypt failure is reported on Environment.Error, as GetFunctionConfiguration does.
func (b *InMemoryBackend) hydrateEnvironment(fn *FunctionConfiguration, version string) {
	if fn.KMSKeyArn == "" || (fn.Environment != nil && len(fn.Environment.Variables) > 0) {
		return
	}

	ct := b.envCiphertext[envCiphertextKey(fn.FunctionName, version)]
	if len(ct) == 0 {
		return
	}

	vars, err := b.openEnvironment(b.kmsResolver, fn.FunctionName, fn.KMSKeyArn, ct)
	if err != nil {
		code := kmsInvokeError(err)
		if code == "" {
			code = "ServiceException"
		}

		fn.Environment = &EnvironmentConfig{Error: &EnvironmentError{ErrorCode: code, Message: err.Error()}}

		return
	}

	fn.Environment = &EnvironmentConfig{Variables: vars}
}

// storeCiphertextLocked records ct for the function's $LATEST slot; nil ct clears it.
func (b *InMemoryBackend) storeCiphertextLocked(name string, ct map[string]string) {
	if len(ct) == 0 {
		delete(b.envCiphertext, name)

		return
	}

	b.envCiphertext[name] = ct
}

func (b *InMemoryBackend) copyCiphertextToVersionLocked(name, version string) {
	if ct, ok := b.envCiphertext[name]; ok {
		b.envCiphertext[envCiphertextKey(name, version)] = maps.Clone(ct)
	} else {
		delete(b.envCiphertext, envCiphertextKey(name, version))
	}
}

func (b *InMemoryBackend) deleteCiphertextLocked(name string) {
	maps.DeleteFunc(b.envCiphertext, func(k string, _ map[string]string) bool {
		return k == name || strings.HasPrefix(k, name+":")
	})
}

// envChanged reports whether the env vars or key differ between old and next.
func envChanged(old, next *FunctionConfiguration) bool {
	if old.KMSKeyArn != next.KMSKeyArn {
		return true
	}

	var ov, nv map[string]string
	if old.Environment != nil {
		ov = old.Environment.Variables
	}

	if next.Environment != nil {
		nv = next.Environment.Variables
	}

	return !maps.Equal(ov, nv)
}

// restoreEnvironmentsLocked hydrates plaintext after a restore; failures are left to lazy hydration.
func (b *InMemoryBackend) restoreEnvironmentsLocked(
	ct map[string]map[string]string, snapVersions map[string][]*FunctionVersion,
) {
	b.envCiphertext = nonNilCiphertext(ct)

	if b.kmsResolver == nil {
		return
	}

	for _, fn := range b.functions.All() {
		cp := cloneFunctionConfig(fn)
		cp.Environment = nil
		b.hydrateEnvironment(cp, "")

		if cp.Environment != nil && cp.Environment.Error == nil {
			fn.Environment = cp.Environment
		}
	}

	for name, vs := range snapVersions {
		for _, v := range vs {
			ct := b.envCiphertext[envCiphertextKey(name, v.Version)]
			if len(ct) == 0 || v.KMSKeyArn == "" {
				continue
			}

			vars, err := b.openEnvironment(b.kmsResolver, name, v.KMSKeyArn, ct)
			if err != nil {
				logger.Load(b.ctx).Debug("lambda: deferring version env decrypt", "function", name, "error", err)

				continue
			}

			v.Environment = &EnvironmentConfig{Variables: vars}
		}
	}
}
