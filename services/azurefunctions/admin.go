package azurefunctions

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

const (
	authKeySetting = "WEBSITE_AUTH_ENCRYPTION_KEY"
	adminTokenTTL  = 10 * time.Minute
)

// adminSigningKey is the HS256 key the host validates admin JWTs with: WEBSITE_AUTH_ENCRYPTION_KEY, which the
// emulator derives from the app's master key unless the app sets its own hex value.
func adminSigningKey(a *appState) []byte {
	if raw, err := hex.DecodeString(a.Settings[authKeySetting]); err == nil && len(raw) > 0 {
		return raw
	}

	sum := sha256.Sum256([]byte("gopherstack-azfn-admin|" + a.MasterKey))

	return sum[:]
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

// AdminToken returns a JWT the app's Functions host accepts as a bearer token on its /admin APIs.
func (b *InMemoryBackend) AdminToken(name string) (string, error) {
	b.mu.RLock("AdminToken")
	defer b.mu.RUnlock()

	a, err := b.appLocked(name)
	if err != nil {
		return "", err
	}

	if a.isWeb() {
		return "", ErrFunctionAppOnly
	}

	now := b.now().UTC()
	site := a.Name
	header, _ := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT"})
	claims, _ := json.Marshal(map[string]any{
		"iat": now.Unix(), "nbf": now.Unix(), "exp": now.Add(adminTokenTTL).Unix(),
		"iss": "https://" + site + ".scm.azurewebsites.net",
		"aud": "https://" + site + ".azurewebsites.net/azurefunctions",
	})

	signing := b64(header) + "." + b64(claims)
	mac := hmac.New(sha256.New, adminSigningKey(&a.appState))
	_, _ = mac.Write([]byte(signing))

	return signing + "." + b64(mac.Sum(nil)), nil
}

// SyncTriggers refreshes the app's trigger metadata: it starts the host if needed and reads its function list.
func (b *InMemoryBackend) SyncTriggers(ctx context.Context, name string) error {
	_, err := b.Functions(ctx, name)

	return err
}

// removeFunctionFiles returns the zip without the function's directory, and whether it had a function.json.
func removeFunctionFiles(data []byte, fn string) ([]byte, bool, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, false, fmt.Errorf("%w: %w", ErrInvalidPackage, err)
	}

	var (
		buf   bytes.Buffer
		found bool
	)

	zw := zip.NewWriter(&buf)
	prefix := fn + "/"

	for _, f := range zr.File {
		if strings.HasPrefix(f.Name, prefix) {
			found = found || f.Name == prefix+"function.json"

			continue
		}

		if err = copyEntry(zw, f); err != nil {
			return nil, false, err
		}
	}

	if err = zw.Close(); err != nil {
		return nil, false, fmt.Errorf("write archive: %w", err)
	}

	return buf.Bytes(), found, nil
}

func copyEntry(zw *zip.Writer, f *zip.File) error {
	w, err := zw.CreateHeader(&zip.FileHeader{Name: f.Name, Method: zip.Deflate, Modified: f.Modified})
	if err != nil {
		return fmt.Errorf("write %s: %w", f.Name, err)
	}

	rc, err := f.Open()
	if err != nil {
		return fmt.Errorf("read %s: %w", f.Name, err)
	}
	defer rc.Close()

	if _, err = io.Copy(w, io.LimitReader(rc, maxUnpacked)); err != nil {
		return fmt.Errorf("copy %s: %w", f.Name, err)
	}

	return nil
}

// DeleteFunction removes a function.json function's directory from the deployed package and restarts the host.
func (b *InMemoryBackend) DeleteFunction(ctx context.Context, name, fn string) error {
	if _, err := b.Function(ctx, name, fn); err != nil {
		return err
	}

	b.mu.Lock("DeleteFunction")

	a, err := b.appLocked(name)
	if err != nil {
		b.mu.Unlock()

		return err
	}

	next, found, err := removeFunctionFiles(a.Zip, fn)
	if err != nil || !found {
		b.mu.Unlock()

		if err == nil {
			err = ErrFunctionInCode
		}

		return err
	}

	a.Zip = next
	delete(a.FunctionKeys, fn)

	old := b.takeBoxLocked(a)
	b.mu.Unlock()

	b.teardown(old)
	b.autoStart(name)

	return nil
}
