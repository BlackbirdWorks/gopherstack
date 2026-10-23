// Command azure-keyvault-app reads its config secret from Azure Key Vault,
// signs a payload with a Key Vault key and verifies the signature.
package main

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azkeys"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
)

const (
	secretName = "app-config"
	keyName    = "app-signing-key"
)

type appConfig struct {
	Service string `json:"service"`
	Payload string `json:"payload"`
}

// devCredential hands out a fixed token; the emulator does not validate it.
type devCredential struct{}

func (devCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: "dev-token", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

func clientOptions() (azcore.ClientOptions, error) {
	path := os.Getenv("KEYVAULT_CA_FILE")
	if path == "" {
		return azcore.ClientOptions{}, errors.New("KEYVAULT_CA_FILE must point to the PEM certificate the Key Vault listener presents (demo.sh fetches it)")
	}

	pemBytes, err := os.ReadFile(path)
	if err != nil {
		return azcore.ClientOptions{}, fmt.Errorf("read KEYVAULT_CA_FILE: %w", err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return azcore.ClientOptions{}, fmt.Errorf("no PEM certificates found in %s", path)
	}

	tr := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}

	return azcore.ClientOptions{Transport: &http.Client{Transport: tr}}, nil
}

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}

	return def
}

func seedSecret(ctx context.Context, c *azsecrets.Client) error {
	seed := appConfig{Service: "billing", Payload: "invoice-42:total=199.00"}

	raw, err := json.Marshal(seed)
	if err != nil {
		return err
	}

	_, err = c.SetSecret(ctx, secretName, azsecrets.SetSecretParameters{Value: new(string(raw))}, nil)

	return err
}

func readConfig(ctx context.Context, c *azsecrets.Client) (appConfig, error) {
	var cfg appConfig

	got, err := c.GetSecret(ctx, secretName, "", nil)
	if err != nil {
		return cfg, err
	}

	if got.Value == nil {
		return cfg, errors.New("secret has no value")
	}

	return cfg, json.Unmarshal([]byte(*got.Value), &cfg)
}

func createKey(ctx context.Context, c *azkeys.Client) (*azkeys.JSONWebKey, error) {
	resp, err := c.CreateKey(ctx, keyName, azkeys.CreateKeyParameters{
		Kty:     new(azkeys.KeyTypeRSA),
		KeySize: new(int32(2048)),
	}, nil)
	if err != nil {
		return nil, err
	}

	return resp.Key, nil
}

func sign(ctx context.Context, c *azkeys.Client, version string, digest []byte) ([]byte, error) {
	alg := azkeys.SignatureAlgorithmRS256

	resp, err := c.Sign(ctx, keyName, version, azkeys.SignParameters{Algorithm: &alg, Value: digest}, nil)
	if err != nil {
		return nil, err
	}

	return resp.Result, nil
}

func verifyRemote(ctx context.Context, c *azkeys.Client, version string, digest, sig []byte) (bool, error) {
	alg := azkeys.SignatureAlgorithmRS256

	resp, err := c.Verify(ctx, keyName, version, azkeys.VerifyParameters{
		Algorithm: &alg, Digest: digest, Signature: sig,
	}, nil)
	if err != nil || resp.Value == nil {
		return false, err
	}

	return *resp.Value, nil
}

func verifyLocal(k *azkeys.JSONWebKey, digest, sig []byte) error {
	pub := &rsa.PublicKey{N: new(big.Int).SetBytes(k.N), E: int(new(big.Int).SetBytes(k.E).Int64())}

	return rsa.VerifyPKCS1v15(pub, crypto.SHA256, digest, sig)
}

func run(ctx context.Context) error {
	vaultURL := env("KEYVAULT_URL", "https://localhost:10004/vaults/demo-vault")

	opts, err := clientOptions()
	if err != nil {
		return err
	}

	secrets, err := azsecrets.NewClient(vaultURL, devCredential{}, &azsecrets.ClientOptions{
		ClientOptions: opts, DisableChallengeResourceVerification: true,
	})
	if err != nil {
		return err
	}

	keys, err := azkeys.NewClient(vaultURL, devCredential{}, &azkeys.ClientOptions{
		ClientOptions: opts, DisableChallengeResourceVerification: true,
	})
	if err != nil {
		return err
	}

	return exercise(ctx, secrets, keys)
}

func exercise(ctx context.Context, secrets *azsecrets.Client, keys *azkeys.Client) error {
	if err := seedSecret(ctx, secrets); err != nil {
		return fmt.Errorf("seed secret: %w", err)
	}

	cfg, err := readConfig(ctx, secrets)
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}

	fmt.Printf("config secret: service=%s payload=%q\n", cfg.Service, cfg.Payload)

	key, err := createKey(ctx, keys)
	if err != nil {
		return fmt.Errorf("create key: %w", err)
	}

	version := key.KID.Version()
	digest := sha256.Sum256([]byte(cfg.Payload))

	sig, err := sign(ctx, keys, version, digest[:])
	if err != nil {
		return fmt.Errorf("sign: %w", err)
	}

	fmt.Printf("signed %d-byte digest with key %s (%d-byte signature)\n", len(digest), keyName, len(sig))

	return verifyAll(ctx, keys, key, version, digest[:], sig)
}

func verifyAll(ctx context.Context, keys *azkeys.Client, key *azkeys.JSONWebKey, version string, digest, sig []byte) error {
	ok, err := verifyRemote(ctx, keys, version, digest, sig)
	if err != nil || !ok {
		return fmt.Errorf("vault verify failed (ok=%v): %w", ok, err)
	}

	if err = verifyLocal(key, digest, sig); err != nil {
		return fmt.Errorf("local verify: %w", err)
	}

	tampered := sha256.Sum256([]byte("invoice-42:total=1.00"))
	if bad, _ := verifyRemote(ctx, keys, version, tampered[:], sig); bad {
		return errors.New("tampered payload verified")
	}

	fmt.Println("signature verified by Key Vault and locally; tampered payload rejected")

	return nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
