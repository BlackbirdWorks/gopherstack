package azurekeyvault

import (
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/x509"
	"math/big"
	"net/http"
	"sort"
)

const (
	defaultRotationExpiry = "P30D"
	rsaPrivateParts       = 4
)

// KeyVersion is one stored version of a key.
type KeyVersion struct {
	Tags    map[string]string `json:"tags,omitempty"`
	ID      string            `json:"id"`
	Kty     string            `json:"kty"`
	Crv     string            `json:"crv,omitempty"`
	Attrs   Attributes        `json:"attrs"`
	KeyOps  []string          `json:"keyOps"`
	Priv    []byte            `json:"priv"`
	KeySize int               `json:"keySize,omitempty"`
	Managed bool              `json:"managed,omitempty"`
}

// VersionID implements Versioned.
func (k *KeyVersion) VersionID() string { return k.ID }

// Attr implements Versioned.
func (k *KeyVersion) Attr() *Attributes { return &k.Attrs }

// JWK is the JSON web key wire/import shape.
type JWK struct {
	KID    string   `json:"kid,omitempty"`
	Kty    string   `json:"kty,omitempty"`
	Crv    string   `json:"crv,omitempty"`
	N      string   `json:"n,omitempty"`
	E      string   `json:"e,omitempty"`
	D      string   `json:"d,omitempty"`
	P      string   `json:"p,omitempty"`
	Q      string   `json:"q,omitempty"`
	DP     string   `json:"dp,omitempty"`
	DQ     string   `json:"dq,omitempty"`
	QI     string   `json:"qi,omitempty"`
	X      string   `json:"x,omitempty"`
	Y      string   `json:"y,omitempty"`
	K      string   `json:"k,omitempty"`
	KeyOps []string `json:"key_ops,omitempty"`
}

// KeyWire is the key bundle/item/deleted-bundle wire shape.
type KeyWire struct {
	Key                *JWK              `json:"key,omitempty"`
	Attributes         *Attributes       `json:"attributes,omitempty"`
	Tags               map[string]string `json:"tags,omitempty"`
	DeletedDate        *int64            `json:"deletedDate,omitempty"`
	ScheduledPurgeDate *int64            `json:"scheduledPurgeDate,omitempty"`
	KID                string            `json:"kid,omitempty"`
	RecoveryID         string            `json:"recoveryId,omitempty"`
	Managed            bool              `json:"managed,omitempty"`
}

// CreateKeyRequest is the POST /keys/{name}/create body.
type CreateKeyRequest struct {
	Attributes *Attributes       `json:"attributes"`
	Tags       map[string]string `json:"tags"`
	Kty        string            `json:"kty"`
	Crv        string            `json:"crv"`
	KeyOps     []string          `json:"key_ops"`
	KeySize    int               `json:"key_size"`
}

// ImportKeyRequest is the PUT /keys/{name} body.
type ImportKeyRequest struct {
	Key        *JWK              `json:"key"`
	Attributes *Attributes       `json:"attributes"`
	Tags       map[string]string `json:"tags"`
}

// UpdateKeyRequest is the PATCH /keys/{name}/{version} body.
type UpdateKeyRequest struct {
	Attributes *Attributes       `json:"attributes"`
	Tags       map[string]string `json:"tags"`
	KeyOps     []string          `json:"key_ops"`
}

// RotationTrigger is a rotation lifetime-action trigger.
type RotationTrigger struct {
	TimeAfterCreate  string `json:"timeAfterCreate,omitempty"`
	TimeBeforeExpiry string `json:"timeBeforeExpiry,omitempty"`
}

// RotationAction is a rotation lifetime-action action.
type RotationAction struct {
	Type string `json:"type"`
}

// RotationLifetimeAction pairs a trigger with an action.
type RotationLifetimeAction struct {
	Trigger RotationTrigger `json:"trigger"`
	Action  RotationAction  `json:"action"`
}

// RotationAttributes carries the policy's expiry time and timestamps.
type RotationAttributes struct {
	Created    *int64 `json:"created,omitempty"`
	Updated    *int64 `json:"updated,omitempty"`
	ExpiryTime string `json:"expiryTime,omitempty"`
}

// RotationPolicy is a key's rotation policy.
type RotationPolicy struct {
	Attributes      RotationAttributes       `json:"attributes"`
	ID              string                   `json:"id,omitempty"`
	LifetimeActions []RotationLifetimeAction `json:"lifetimeActions"`
}

func defaultKeyOps(family string) []string {
	switch family {
	case ktyRSA:
		return []string{opEncrypt, opDecrypt, opSign, opVerify, opWrapKey, opUnwrapKey}
	case ktyEC:
		return []string{opSign, opVerify}
	}

	return []string{opEncrypt, opDecrypt, opWrapKey, opUnwrapKey}
}

func defaultRotation(now int64) *RotationPolicy {
	return &RotationPolicy{
		LifetimeActions: []RotationLifetimeAction{{
			Trigger: RotationTrigger{TimeBeforeExpiry: defaultRotationExpiry},
			Action:  RotationAction{Type: "Notify"},
		}},
		Attributes: RotationAttributes{Created: &now, Updated: &now},
	}
}

func (k *KeyVersion) private() (any, error) {
	if ktyFamily(k.Kty) == ktyOct {
		return k.Priv, nil
	}

	return x509.ParsePKCS8PrivateKey(k.Priv)
}

func (k *KeyVersion) jwk(base, name string) *JWK {
	out := &JWK{KID: base + "/keys/" + name + "/" + k.ID, Kty: k.Kty, KeyOps: k.KeyOps}

	priv, err := k.private()
	if err != nil {
		return out
	}

	switch p := priv.(type) {
	case *rsa.PrivateKey:
		out.N = b64u(p.N.Bytes())
		out.E = b64u(big.NewInt(int64(p.E)).Bytes())
	case *ecdsa.PrivateKey:
		size := (p.Curve.Params().BitSize + bitsPerByte - 1) / bitsPerByte
		out.Crv = k.Crv
		if point, pErr := p.PublicKey.Bytes(); pErr == nil {
			out.X = b64u(point[1 : 1+size])
			out.Y = b64u(point[1+size:])
		}
	}

	return out
}

func (k *KeyVersion) wire(base, name string, full bool, e *Entity[*KeyVersion]) KeyWire {
	attrs := k.Attrs
	w := KeyWire{Attributes: &attrs, Tags: k.Tags, Managed: k.Managed}

	if full {
		w.Key = k.jwk(base, name)
	} else {
		w.KID = base + "/keys/" + name
	}

	if e != nil && e.Deleted != nil {
		d := *e.Deleted
		w.DeletedDate = &d.DeletedDate
		w.ScheduledPurgeDate = &d.ScheduledPurgeDate
		w.RecoveryID = base + "/deletedkeys/" + name
	}

	return w
}

func (v *vault) findKey(name, version string) (*KeyVersion, error) {
	e, ok := v.Keys.active(name)
	if !ok {
		return nil, errNotFound(kindKey, name)
	}

	ver, ok := e.version(version)
	if !ok {
		return nil, errNotFound(kindKey, name+"/"+version)
	}

	return ver, nil
}

func (v *vault) newKeyVersion(req *CreateKeyRequest, now int64) (*KeyVersion, error) {
	family := ktyFamily(req.Kty)

	priv, size, crv, err := generateKeyMaterial(req.Kty, req.KeySize, req.Crv)
	if err != nil {
		return nil, err
	}

	ops := req.KeyOps
	if len(ops) == 0 {
		ops = defaultKeyOps(family)
	}

	return &KeyVersion{
		ID:      newVersionID(),
		Kty:     req.Kty,
		Crv:     crv,
		KeySize: size,
		KeyOps:  ops,
		Priv:    priv,
		Tags:    cloneTags(req.Tags),
		Attrs:   newAttributes(req.Attributes, now, v.RetentionDays, protectedVault(v)),
	}, nil
}

func (v *vault) addKeyVersion(name string, ver *KeyVersion, now int64) error {
	e, err := v.Keys.create(kindKey, name)
	if err != nil {
		return err
	}

	if l := e.latest(); l != nil && l.Managed {
		return newErr(http.StatusConflict, "Conflict", "Key %s is managed by a certificate.", name)
	}

	e.Versions = append(e.Versions, ver)

	if e.Rotation == nil {
		e.Rotation = defaultRotation(now)
	}

	return nil
}

// CreateKey creates a new key (or a new version of an existing one).
func (b *InMemoryBackend) CreateKey(vaultName, base, name string, req *CreateKeyRequest) (KeyWire, error) {
	var out KeyWire

	err := b.do(vaultName, func(v *vault, now int64) error {
		ver, err := v.newKeyVersion(req, now)
		if err != nil {
			return err
		}

		if err = v.addKeyVersion(name, ver, now); err != nil {
			return err
		}

		out = ver.wire(base, name, true, nil)

		return nil
	})

	return out, err
}

func decodeBig(s string) (*big.Int, error) {
	b, err := unb64u(s)
	if err != nil {
		return nil, err
	}

	return new(big.Int).SetBytes(b), nil
}

func importRSA(j *JWK) ([]byte, int, error) {
	vals := make([]*big.Int, 0, rsaPrivateParts)

	for _, s := range []string{j.N, j.D, j.P, j.Q} {
		bi, err := decodeBig(s)
		if err != nil {
			return nil, 0, err
		}

		vals = append(vals, bi)
	}

	e, err := decodeBig(j.E)
	if err != nil {
		return nil, 0, err
	}

	priv := &rsa.PrivateKey{D: vals[1], Primes: []*big.Int{vals[2], vals[3]}}
	priv.N, priv.E = vals[0], int(e.Int64())

	if err = priv.Validate(); err != nil {
		return nil, 0, errBadParameter("The supplied RSA key is invalid.")
	}

	priv.Precompute()

	der, err := x509.MarshalPKCS8PrivateKey(priv)

	return der, priv.N.BitLen(), err
}

func importEC(j *JWK) ([]byte, string, error) {
	c, err := curveFor(j.Crv)
	if err != nil {
		return nil, "", err
	}

	d, err := unb64u(j.D)
	if err != nil {
		return nil, "", err
	}

	priv, err := ecdsa.ParseRawPrivateKey(c, d)
	if err != nil {
		return nil, "", errBadParameter("The supplied EC key is invalid.")
	}

	der, err := x509.MarshalPKCS8PrivateKey(priv)

	return der, curveName(c), err
}

func importMaterial(j *JWK) (*KeyVersion, error) {
	ver := &KeyVersion{Kty: j.Kty}

	var err error

	switch ktyFamily(j.Kty) {
	case ktyRSA:
		if j.D == "" || j.P == "" || j.Q == "" {
			return nil, errBadParameter("The key must contain the private key components.")
		}

		ver.Priv, ver.KeySize, err = importRSA(j)
	case ktyEC:
		if j.D == "" {
			return nil, errBadParameter("The key must contain the private key component.")
		}

		ver.Priv, ver.Crv, err = importEC(j)
	case ktyOct:
		ver.Priv, err = unb64u(j.K)
		ver.KeySize = len(ver.Priv) * bitsPerByte
	default:
		return nil, errBadParameter("Unsupported key type %q.", j.Kty)
	}

	return ver, err
}

// ImportKey imports a caller-supplied private key.
func (b *InMemoryBackend) ImportKey(vaultName, base, name string, req *ImportKeyRequest) (KeyWire, error) {
	var out KeyWire

	err := b.do(vaultName, func(v *vault, now int64) error {
		if req.Key == nil {
			return errBadParameter("The request body must contain the key property.")
		}

		ver, err := importMaterial(req.Key)
		if err != nil {
			return err
		}

		ver.ID = newVersionID()
		ver.KeyOps = req.Key.KeyOps

		if len(ver.KeyOps) == 0 {
			ver.KeyOps = defaultKeyOps(ktyFamily(ver.Kty))
		}

		ver.Tags = cloneTags(req.Tags)
		ver.Attrs = newAttributes(req.Attributes, now, v.RetentionDays, protectedVault(v))

		if err = v.addKeyVersion(name, ver, now); err != nil {
			return err
		}

		out = ver.wire(base, name, true, nil)

		return nil
	})

	return out, err
}

// GetKey returns a key version (latest when version is empty).
func (b *InMemoryBackend) GetKey(vaultName, base, name, version string) (KeyWire, error) {
	var out KeyWire

	err := b.do(vaultName, func(v *vault, _ int64) error {
		ver, err := v.findKey(name, version)
		if err != nil {
			return err
		}

		out = ver.wire(base, name, true, nil)

		return nil
	})

	return out, err
}

// UpdateKey updates a key version's mutable properties.
func (b *InMemoryBackend) UpdateKey(vaultName, base, name, version string, req *UpdateKeyRequest) (KeyWire, error) {
	var out KeyWire

	err := b.do(vaultName, func(v *vault, now int64) error {
		ver, err := v.findKey(name, version)
		if err != nil {
			return err
		}

		ver.Attrs.merge(req.Attributes)
		ver.Attrs.Updated = &now

		if req.Tags != nil {
			ver.Tags = cloneTags(req.Tags)
		}

		if req.KeyOps != nil {
			ver.KeyOps = req.KeyOps
		}

		out = ver.wire(base, name, true, nil)

		return nil
	})

	return out, err
}

// DeleteKey soft-deletes a key.
func (b *InMemoryBackend) DeleteKey(vaultName, base, name string) (KeyWire, error) {
	var out KeyWire

	err := b.do(vaultName, func(v *vault, now int64) error {
		if e, ok := v.Keys.active(name); ok && e.latest().Managed {
			return newErr(http.StatusConflict, "Conflict",
				"Key %s is managed by a certificate; delete the certificate.", name)
		}

		e, err := v.Keys.softDelete(kindKey, name, now, v.RetentionDays)
		if err != nil {
			return err
		}

		out = e.latest().wire(base, name, true, e)

		return nil
	})

	return out, err
}

// GetDeletedKey returns a soft-deleted key.
func (b *InMemoryBackend) GetDeletedKey(vaultName, base, name string) (KeyWire, error) {
	var out KeyWire

	err := b.do(vaultName, func(v *vault, _ int64) error {
		e, ok := v.Keys.deleted(name)
		if !ok {
			return errNotFound(kindKey, name)
		}

		out = e.latest().wire(base, name, true, e)

		return nil
	})

	return out, err
}

// RecoverKey restores a soft-deleted key.
func (b *InMemoryBackend) RecoverKey(vaultName, base, name string) (KeyWire, error) {
	var out KeyWire

	err := b.do(vaultName, func(v *vault, _ int64) error {
		e, err := v.Keys.recover(kindKey, name)
		if err != nil {
			return err
		}

		out = e.latest().wire(base, name, true, nil)

		return nil
	})

	return out, err
}

// PurgeKey permanently removes a soft-deleted key.
func (b *InMemoryBackend) PurgeKey(vaultName, name string) error {
	return b.do(vaultName, func(v *vault, _ int64) error {
		return v.Keys.purge(kindKey, name, v.PurgeProtection)
	})
}

// ListKeys lists keys, deleted or active.
func (b *InMemoryBackend) ListKeys(vaultName, base string, deleted bool) ([]KeyWire, error) {
	var out []KeyWire

	err := b.do(vaultName, func(v *vault, _ int64) error {
		for _, n := range v.Keys.names(deleted) {
			e := v.Keys[n]
			if deleted && e.latest().Managed {
				continue
			}

			out = append(out, e.latest().wire(base, n, false, e))
		}

		return nil
	})

	return out, err
}

// ListKeyVersions lists every version of a key.
func (b *InMemoryBackend) ListKeyVersions(vaultName, base, name string) ([]KeyWire, error) {
	var out []KeyWire

	err := b.do(vaultName, func(v *vault, _ int64) error {
		e, ok := v.Keys.active(name)
		if !ok {
			return errNotFound(kindKey, name)
		}

		versions := append([]*KeyVersion(nil), e.Versions...)
		sort.SliceStable(versions, func(i, j int) bool {
			return *versions[i].Attrs.Created < *versions[j].Attrs.Created
		})

		for _, ver := range versions {
			w := ver.wire(base, name, false, nil)
			w.KID += "/" + ver.ID
			out = append(out, w)
		}

		return nil
	})

	return out, err
}

// RotateKey creates a new version with fresh key material.
func (b *InMemoryBackend) RotateKey(vaultName, base, name string) (KeyWire, error) {
	var out KeyWire

	err := b.do(vaultName, func(v *vault, now int64) error {
		e, ok := v.Keys.active(name)
		if !ok {
			return errNotFound(kindKey, name)
		}

		next, err := v.rotateKey(e, now)
		if err != nil {
			return err
		}

		out = next.wire(base, name, true, nil)

		return nil
	})

	return out, err
}

// GetRotationPolicy returns a key's rotation policy.
func (b *InMemoryBackend) GetRotationPolicy(vaultName, base, name string) (RotationPolicy, error) {
	var out RotationPolicy

	err := b.do(vaultName, func(v *vault, _ int64) error {
		e, ok := v.Keys.active(name)
		if !ok {
			return errNotFound(kindKey, name)
		}

		out = *e.Rotation
		out.ID = base + "/keys/" + name + "/rotationpolicy"

		return nil
	})

	return out, err
}

// SetRotationPolicy replaces a key's rotation policy.
func (b *InMemoryBackend) SetRotationPolicy(
	vaultName, base, name string, in *RotationPolicy,
) (RotationPolicy, error) {
	var out RotationPolicy

	err := b.do(vaultName, func(v *vault, now int64) error {
		e, ok := v.Keys.active(name)
		if !ok {
			return errNotFound(kindKey, name)
		}

		if err := in.validate(); err != nil {
			return err
		}

		e.Rotation = &RotationPolicy{
			LifetimeActions: in.LifetimeActions,
			Attributes: RotationAttributes{
				ExpiryTime: in.Attributes.ExpiryTime,
				Created:    e.Rotation.Attributes.Created,
				Updated:    &now,
			},
		}
		out = *e.Rotation
		out.ID = base + "/keys/" + name + "/rotationpolicy"

		return nil
	})

	return out, err
}
