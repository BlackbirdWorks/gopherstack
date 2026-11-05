package ocivault

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"strings"
)

const (
	jsonNull      = "null"
	rsa2048       = 2048
	rsa3072       = 3072
	rsa4096       = 4096
	genPassphrase = "PASSPHRASE"
	genSSHKey     = "SSH_KEY"
	genBytes      = "BYTES"

	tmplSecretsDefault = "SECRETS_DEFAULT_PASSWORD"
	tmplDBDefault      = "DBAAS_DEFAULT_PASSWORD"

	phPassphrase = "%GENERATED_PASSPHRASE%"
	phBytes      = "%GENERATED_BYTES%"
	phPublic     = "%GENERATED_PUBLIC_KEY%"
	phPrivate    = "%GENERATED_PRIVATE_KEY%"

	defaultPhraseLen = 16
	minSecretsLen    = 8
	maxSecretsLen    = 128
	minDBLen         = 12
	maxDBLen         = 30

	upperChars   = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	lowerChars   = "abcdefghijklmnopqrstuvwxyz"
	digitChars   = "0123456789"
	secretsExtra = "!@#$%^&*()-_=+"
	dbExtra      = "#_"
)

//nolint:gochecknoglobals // read-only template table
var rsaBits = map[string]int{"RSA_2048": rsa2048, "RSA_3072": rsa3072, "RSA_4096": rsa4096}

// GenerationContext is the polymorphic secretGenerationContext body.
type GenerationContext struct {
	SecretTemplate     string `json:"secretTemplate,omitempty"`
	GenerationType     string `json:"generationType"`
	GenerationTemplate string `json:"generationTemplate"`
	PassphraseLength   int    `json:"passphraseLength,omitempty"`
}

func parseGeneration(raw json.RawMessage) (*GenerationContext, *APIError) {
	if len(raw) == 0 || string(raw) == jsonNull {
		return nil, nil
	}

	var g GenerationContext
	if err := json.Unmarshal(raw, &g); err != nil {
		return nil, errParam("secretGenerationContext is not valid")
	}

	return &g, g.validate()
}

func (g *GenerationContext) validate() *APIError {
	need := map[string][]string{
		genPassphrase: {phPassphrase},
		genBytes:      {phBytes},
		genSSHKey:     {phPublic, phPrivate},
	}[g.GenerationType]

	switch g.GenerationType {
	case genPassphrase:
		if aerr := g.validatePassphrase(); aerr != nil {
			return aerr
		}
	case genSSHKey:
		if g.GenerationTemplate != "RSA_2048" && g.GenerationTemplate != "RSA_3072" &&
			g.GenerationTemplate != "RSA_4096" {
			return errParam("SSH_KEY generationTemplate must be RSA_2048, RSA_3072 or RSA_4096")
		}
	case genBytes:
		if g.GenerationTemplate != "BYTES_512" && g.GenerationTemplate != "BYTES_1024" {
			return errParam("BYTES generationTemplate must be BYTES_512 or BYTES_1024")
		}
	default:
		return errParam("secretGenerationContext.generationType must be PASSPHRASE, SSH_KEY or BYTES")
	}

	if g.SecretTemplate != "" {
		for _, ph := range need {
			if !strings.Contains(g.SecretTemplate, ph) {
				return errParam("secretTemplate must contain " + ph)
			}
		}
	}

	return nil
}

func (g *GenerationContext) validatePassphrase() *APIError {
	lo, hi := minSecretsLen, maxSecretsLen

	switch g.GenerationTemplate {
	case tmplSecretsDefault:
	case tmplDBDefault:
		lo, hi = minDBLen, maxDBLen
	default:
		return errParam("PASSPHRASE generationTemplate must be SECRETS_DEFAULT_PASSWORD or DBAAS_DEFAULT_PASSWORD")
	}

	if g.PassphraseLength == 0 {
		g.PassphraseLength = defaultPhraseLen
	}

	if g.PassphraseLength < lo || g.PassphraseLength > hi {
		return errParam("passphraseLength must be between " + itoa(int64(lo)) + " and " + itoa(int64(hi)) +
			" for " + g.GenerationTemplate)
	}

	return nil
}

func randIndex(n int) (int, error) {
	v, err := rand.Int(rand.Reader, big.NewInt(int64(n)))
	if err != nil {
		return 0, err //nolint:wrapcheck // crypto/rand failure
	}

	return int(v.Int64()), nil
}

// passphrase returns a password with at least one upper, lower and digit character.
func passphrase(template string, n int) (string, error) {
	extra := secretsExtra
	if template == tmplDBDefault {
		extra = dbExtra
	}

	pools := []string{upperChars, lowerChars, digitChars}
	all := upperChars + lowerChars + digitChars + extra
	out := make([]byte, n)

	for i := range out {
		pool := all
		if i < len(pools) {
			pool = pools[i]
		}

		j, err := randIndex(len(pool))
		if err != nil {
			return "", err
		}

		out[i] = pool[j]
	}

	for i := len(out) - 1; i > 0; i-- {
		j, err := randIndex(i + 1)
		if err != nil {
			return "", err
		}

		out[i], out[j] = out[j], out[i]
	}

	return string(out), nil
}

// generate renders the secret content (before base64 storage encoding) for a context.
func (g *GenerationContext) generate() ([]byte, error) {
	switch g.GenerationType {
	case genPassphrase:
		p, err := passphrase(g.GenerationTemplate, g.PassphraseLength)
		if err != nil {
			return nil, err
		}

		return []byte(g.render(map[string]string{phPassphrase: p}, phPassphrase)), nil
	case genBytes:
		n := 512
		if g.GenerationTemplate == "BYTES_1024" {
			n = 1024
		}

		raw := make([]byte, n)
		if _, err := rand.Read(raw); err != nil {
			return nil, err //nolint:wrapcheck // crypto/rand failure
		}

		if g.SecretTemplate == "" {
			return raw, nil
		}

		return []byte(g.render(map[string]string{phBytes: base64.StdEncoding.EncodeToString(raw)}, phBytes)), nil
	default:
		return g.generateSSH()
	}
}

func (g *GenerationContext) generateSSH() ([]byte, error) {
	bits := rsaBits[g.GenerationTemplate]

	k, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		return nil, err //nolint:wrapcheck // crypto failure
	}

	pub, err := x509.MarshalPKIXPublicKey(&k.PublicKey)
	if err != nil {
		return nil, err //nolint:wrapcheck // crypto failure
	}

	priv, err := x509.MarshalPKCS8PrivateKey(k)
	if err != nil {
		return nil, err //nolint:wrapcheck // crypto failure
	}

	vals := map[string]string{
		phPublic:  string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pub})),
		phPrivate: string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: priv})),
	}

	if g.SecretTemplate == "" {
		out, merr := json.Marshal(map[string]string{"publicKey": vals[phPublic], "privateKey": vals[phPrivate]})

		return out, merr //nolint:wrapcheck // static shape
	}

	return []byte(g.render(vals, "")), nil
}

func (g *GenerationContext) render(vals map[string]string, dflt string) string {
	if g.SecretTemplate == "" {
		return vals[dflt]
	}

	out := g.SecretTemplate
	for ph, v := range vals {
		out = strings.ReplaceAll(out, ph, v)
	}

	return out
}
