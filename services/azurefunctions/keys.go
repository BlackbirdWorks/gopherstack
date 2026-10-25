package azurefunctions

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

const (
	// KeyTypeFunction and KeyTypeSystem are the host key collections ARM addresses as host/default/{keyType}.
	KeyTypeFunction = "functionKeys"
	// KeyTypeSystem is the host key collection extensions use.
	KeyTypeSystem = "systemKeys"

	defaultKeyName = "default"
	keyBytes       = 40
	secretsFileMod = 0o666
	secretsDirMod  = 0o777
)

// newKey returns a key shaped like the ones the Functions host generates.
func newKey() string {
	buf := make([]byte, keyBytes)
	_, _ = rand.Read(buf)

	return base64.RawURLEncoding.EncodeToString(buf) + "=="
}

type secretKey struct {
	Name      string `json:"name"`
	Value     string `json:"value"`
	Encrypted bool   `json:"encrypted"`
}

type hostSecretsFile struct {
	Source          string      `json:"source"`
	DecryptionKeyID string      `json:"decryptionKeyId"`
	MasterKey       secretKey   `json:"masterKey"`
	FunctionKeys    []secretKey `json:"functionKeys"`
	SystemKeys      []secretKey `json:"systemKeys"`
}

type functionSecretsFile struct {
	Source          string      `json:"source"`
	DecryptionKeyID string      `json:"decryptionKeyId"`
	Keys            []secretKey `json:"keys"`
}

func sortedKeys(m map[string]string) []secretKey {
	names := make([]string, 0, len(m))
	for n := range m {
		names = append(names, n)
	}

	slices.Sort(names)

	out := make([]secretKey, 0, len(names))
	for _, n := range names {
		out = append(out, secretKey{Name: n, Value: m[n]})
	}

	return out
}

func writeAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(
		tmp,
		data,
		secretsFileMod,
	); err != nil {
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}

	if err := os.Chmod(tmp, secretsFileMod); err != nil {
		return fmt.Errorf("chmod %s: %w", filepath.Base(path), err)
	}

	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename %s: %w", filepath.Base(path), err)
	}

	return nil
}

// writeSecrets lays the app's keys out the way the host's file secret store reads them. The host watches the
// directory, so a rewrite takes effect without restarting the container.
func writeSecrets(dir string, st *appState) error {
	host := hostSecretsFile{
		MasterKey:    secretKey{Name: "master", Value: st.MasterKey},
		FunctionKeys: sortedKeys(st.HostKeys),
		SystemKeys:   sortedKeys(st.SystemKeys),
		Source:       "runtime",
	}

	raw, err := json.Marshal(host)
	if err != nil {
		return fmt.Errorf("encode host secrets: %w", err)
	}

	if err = writeAtomic(filepath.Join(dir, "host.json"), raw); err != nil {
		return err
	}

	want := map[string]bool{"host.json": true}

	for fn, keys := range st.FunctionKeys {
		raw, err = json.Marshal(functionSecretsFile{Source: "runtime", Keys: sortedKeys(keys)})
		if err != nil {
			return fmt.Errorf("encode function secrets: %w", err)
		}

		file := strings.ToLower(fn) + ".json"
		want[file] = true

		if err = writeAtomic(filepath.Join(dir, file), raw); err != nil {
			return err
		}
	}

	return pruneSecrets(dir, want)
}

// pruneSecrets removes function secret files the app no longer has keys for.
func pruneSecrets(dir string, keep map[string]bool) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read secrets dir: %w", err)
	}

	for _, e := range entries {
		if e.IsDir() || keep[e.Name()] || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}

		if rmErr := os.Remove(filepath.Join(dir, e.Name())); rmErr != nil && !os.IsNotExist(rmErr) {
			return fmt.Errorf("remove %s: %w", e.Name(), rmErr)
		}
	}

	return nil
}
