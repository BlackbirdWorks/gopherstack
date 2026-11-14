package gcpartifactregistry

import (
	"bytes"
	"crypto"
	"io"

	"github.com/ProtonMail/go-crypto/openpgp"
	"github.com/ProtonMail/go-crypto/openpgp/armor"
	"github.com/ProtonMail/go-crypto/openpgp/clearsign"
	"github.com/ProtonMail/go-crypto/openpgp/packet"
)

const signingRSABits = 3072

// repoSigner signs repository metadata with the repository's OpenPGP key.
type repoSigner struct {
	entity *openpgp.Entity
	cfg    *packet.Config
}

// signerFor returns the repository's signing key, generating and keeping it on first use.
func (b *InMemoryBackend) signerFor(ref repoRef) (*repoSigner, error) {
	name := ref.full()

	b.mu.RLock("signerFor")
	raw, ok := b.signKeys[name]
	now := b.now
	b.mu.RUnlock()

	cfg := &packet.Config{
		Time: now, DefaultHash: crypto.SHA256, RSABits: signingRSABits,
	}

	if ok {
		return loadSigner(raw, cfg)
	}

	ent, err := openpgp.NewEntity(
		"Artifact Registry "+ref.id,
		"gopherstack signing key",
		ref.id+"@gopherstack.invalid",
		cfg,
	)
	if err != nil {
		return nil, errInternal(err)
	}

	var buf bytes.Buffer
	if err = ent.SerializePrivate(&buf, cfg); err != nil {
		return nil, errInternal(err)
	}

	b.mu.Lock("signerFor")
	defer b.mu.Unlock()

	if existing, kept := b.signKeys[name]; kept {
		return loadSigner(existing, cfg)
	}

	if _, live := b.repos[name]; !live {
		return &repoSigner{entity: ent, cfg: cfg}, nil
	}

	b.signKeys[name] = buf.Bytes()

	return &repoSigner{entity: ent, cfg: cfg}, nil
}

func loadSigner(raw []byte, cfg *packet.Config) (*repoSigner, error) {
	list, err := openpgp.ReadKeyRing(bytes.NewReader(raw))
	if err != nil || len(list) == 0 {
		return nil, errInternal(io.ErrUnexpectedEOF)
	}

	return &repoSigner{entity: list[0], cfg: cfg}, nil
}

// detached returns an ASCII-armored detached signature of msg.
func (s *repoSigner) detached(msg []byte) ([]byte, error) {
	var out bytes.Buffer
	if err := openpgp.ArmoredDetachSign(&out, s.entity, bytes.NewReader(msg), s.cfg); err != nil {
		return nil, errInternal(err)
	}

	return withNewline(out.Bytes()), nil
}

func withNewline(b []byte) []byte {
	if len(b) > 0 && b[len(b)-1] != '\n' {
		return append(b, '\n')
	}

	return b
}

// clearSigned returns msg as a cleartext-signed document.
func (s *repoSigner) clearSigned(msg []byte) ([]byte, error) {
	var out bytes.Buffer

	w, err := clearsign.Encode(&out, s.entity.PrivateKey, s.cfg)
	if err != nil {
		return nil, errInternal(err)
	}

	if _, err = w.Write(msg); err != nil {
		return nil, errInternal(err)
	}

	if err = w.Close(); err != nil {
		return nil, errInternal(err)
	}

	return withNewline(out.Bytes()), nil
}

// publicKey returns the public key, ASCII-armored or as binary packets.
func (s *repoSigner) publicKey(armored bool) ([]byte, error) {
	var out bytes.Buffer

	if !armored {
		if err := s.entity.Serialize(&out); err != nil {
			return nil, errInternal(err)
		}

		return out.Bytes(), nil
	}

	w, err := armor.Encode(&out, openpgp.PublicKeyType, nil)
	if err != nil {
		return nil, errInternal(err)
	}

	if err = s.entity.Serialize(w); err != nil {
		return nil, errInternal(err)
	}

	if err = w.Close(); err != nil {
		return nil, errInternal(err)
	}

	return withNewline(out.Bytes()), nil
}
