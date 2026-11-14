package azurearm

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
	"golang.org/x/crypto/ssh"
)

const (
	sshKeyBits        = 3072
	sshMinRSABits     = 2048
	sshEncRSA         = "RSA"
	sshEncEd25519     = "Ed25519"
	sshFieldPublicKey = "publicKey"
	sshActionGenerate = "generateKeyPair"
)

func validSSHPublicKey(key string) error {
	parsed, _, _, _, err := ssh.ParseAuthorizedKey([]byte(key))
	if err != nil {
		return cmpInvalid("SSH public key is not valid; it must be in ssh-rsa format and at least 2048 bits.")
	}

	if parsed.Type() == ssh.KeyAlgoRSA {
		if pub, ok := rsaKeyOf(parsed); ok && pub.N.BitLen() >= sshMinRSABits {
			return nil
		}

		return cmpInvalid("SSH public key is not valid; an ssh-rsa key must be at least 2048 bits.")
	}

	if parsed.Type() == ssh.KeyAlgoED25519 {
		return nil
	}

	return cmpInvalid("SSH public key is not valid; it must be in ssh-rsa format and at least 2048 bits.")
}

func rsaKeyOf(k ssh.PublicKey) (*rsa.PublicKey, bool) {
	cp, ok := k.(ssh.CryptoPublicKey)
	if !ok {
		return nil, false
	}

	pub, ok := cp.CryptoPublicKey().(*rsa.PublicKey)

	return pub, ok
}

func (p *ComputeProvider) putSSHKey(id ResourceID, body map[string]any) (*cmpRes, error) {
	if err := validCmpName(id.Names[0], cmpMaxNameLen); err != nil {
		return nil, err
	}

	r, old, err := p.upsert(cmpSSHKeys, id, body)
	if err != nil {
		return nil, err
	}

	props := map[string]any{}

	key := strings.TrimSpace(strOf(propsOf(body), sshFieldPublicKey))

	switch {
	case key != "":
		if err = validSSHPublicKey(key); err != nil {
			return nil, err
		}

		if old != nil && strOf(old.Props, sshFieldPublicKey) != "" && strOf(old.Props, sshFieldPublicKey) != key {
			return nil, cmpChanged("properties.publicKey")
		}

		props[sshFieldPublicKey] = key
	case old != nil:
		if prev := strOf(old.Props, sshFieldPublicKey); prev != "" {
			props[sshFieldPublicKey] = prev
		}
	}

	r.Props = props
	p.items[cmpKey(cmpSSHKeys, r.RG, r.Name)] = r

	return r, nil
}

func newSSHKeyPair(encryption string) (string, string, error) {
	if encryption == sshEncEd25519 {
		pub, priv, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return "", "", err
		}

		sshPub, err := ssh.NewPublicKey(pub)
		if err != nil {
			return "", "", err
		}

		block, err := ssh.MarshalPrivateKey(priv, "")
		if err != nil {
			return "", "", err
		}

		return string(pem.EncodeToMemory(block)), strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sshPub))), nil
	}

	priv, err := rsa.GenerateKey(rand.Reader, sshKeyBits)
	if err != nil {
		return "", "", err
	}

	sshPub, err := ssh.NewPublicKey(&priv.PublicKey)
	if err != nil {
		return "", "", err
	}

	privPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})

	return string(privPEM), strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sshPub))), nil
}

// GenerateSSHKeyPair fills an SSH public key resource that has no key yet and returns the key pair.
func (p *ComputeProvider) GenerateSSHKeyPair(id ResourceID, encryption string) (map[string]any, error) {
	enc := sshEncRSA

	if encryption != "" {
		canon, ok := canonicalIn([]string{sshEncRSA, sshEncEd25519}, encryption)
		if !ok {
			return nil, cmpInvalid(
				"The encryptionType '%s' is not valid; allowed values are RSA and Ed25519.",
				encryption,
			)
		}

		enc = canon
	}

	p.mu.Lock("GenerateSSHKeyPair")
	defer p.mu.Unlock()

	r, ok := p.top(cmpSSHKeys, id.ResourceGroup, id.Names[0])
	if !ok {
		return nil, ErrResourceNotFound
	}

	if strOf(r.Props, sshFieldPublicKey) != "" {
		return nil, cmpNotAllowed("The SSH public key %s already has a public key; generateKeyPair can only be "+
			"performed once per SSH public key resource.", r.Name)
	}

	priv, pub, err := newSSHKeyPair(enc)
	if err != nil {
		return nil, err
	}

	r.Props = map[string]any{sshFieldPublicKey: pub}

	return map[string]any{"privateKey": priv, sshFieldPublicKey: pub, cmpFieldID: r.armID()}, nil
}

func (h *Handler) computeSSHAction(c *echo.Context, cp *ComputeProvider, id ResourceID) error {
	var (
		body map[string]any
		err  error
	)

	if c.Request().ContentLength != 0 {
		if body, err = decodeJSONBody(c.Request()); err != nil {
			return h.writeAPIError(c, err)
		}
	}

	out, err := cp.GenerateSSHKeyPair(id, strOf(body, "encryptionType"))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, out)
}
