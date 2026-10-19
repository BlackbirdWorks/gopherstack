package azurekeyvault

import (
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"net/http"
	"sort"
	"strings"
)

// CertVersion is one stored version of a certificate.
type CertVersion struct {
	Tags  map[string]string `json:"tags,omitempty"`
	ID    string            `json:"id"`
	X5T   string            `json:"x5t"`
	Attrs Attributes        `json:"attrs"`
	Cer   []byte            `json:"cer"`
}

// VersionID implements Versioned.
func (c *CertVersion) VersionID() string { return c.ID }

// Attr implements Versioned.
func (c *CertVersion) Attr() *Attributes { return &c.Attrs }

// CertWire is the certificate bundle/item/deleted-bundle wire shape.
type CertWire struct {
	Attributes         *Attributes       `json:"attributes,omitempty"`
	Tags               map[string]string `json:"tags,omitempty"`
	Policy             *CertPolicy       `json:"policy,omitempty"`
	DeletedDate        *int64            `json:"deletedDate,omitempty"`
	ScheduledPurgeDate *int64            `json:"scheduledPurgeDate,omitempty"`
	ID                 string            `json:"id"`
	KID                string            `json:"kid,omitempty"`
	SID                string            `json:"sid,omitempty"`
	X5T                string            `json:"x5t,omitempty"`
	RecoveryID         string            `json:"recoveryId,omitempty"`
	Cer                []byte            `json:"cer,omitempty"`
}

// CertOperation is the pending certificate operation.
type CertOperation struct {
	Issuer                *IssuerRef `json:"issuer,omitempty"`
	ID                    string     `json:"id,omitempty"`
	Status                string     `json:"status"`
	StatusDetails         string     `json:"status_details,omitempty"`
	Target                string     `json:"target,omitempty"`
	RequestID             string     `json:"request_id,omitempty"`
	CSR                   []byte     `json:"csr,omitempty"`
	CancellationRequested bool       `json:"cancellation_requested"`
}

// CreateCertRequest is the POST /certificates/{name}/create body.
type CreateCertRequest struct {
	Policy     *CertPolicy       `json:"policy"`
	Attributes *Attributes       `json:"attributes"`
	Tags       map[string]string `json:"tags"`
}

// ImportCertRequest is the POST /certificates/{name}/import body.
type ImportCertRequest struct {
	Policy     *CertPolicy       `json:"policy"`
	Attributes *Attributes       `json:"attributes"`
	Tags       map[string]string `json:"tags"`
	Value      string            `json:"value"`
	Password   string            `json:"pwd"`
}

// UpdateCertRequest is the PATCH /certificates/{name}/{version} body.
type UpdateCertRequest struct {
	Attributes *Attributes       `json:"attributes"`
	Tags       map[string]string `json:"tags"`
}

// UpdatePendingRequest is the PATCH /certificates/{name}/pending body.
type UpdatePendingRequest struct {
	CancellationRequested bool `json:"cancellation_requested"`
}

func (c *CertVersion) wire(base, name string, e *Entity[*CertVersion], full bool) CertWire {
	attrs := c.Attrs
	w := CertWire{
		ID:         base + "/certificates/" + name,
		X5T:        c.X5T,
		Attributes: &attrs,
		Tags:       c.Tags,
	}

	if full {
		w.ID += "/" + c.ID
		w.KID = base + "/keys/" + name + "/" + c.ID
		w.SID = base + "/secrets/" + name + "/" + c.ID
		w.Cer = c.Cer

		if e != nil && e.CertPolicy != nil {
			p := *e.CertPolicy
			p.ID = base + "/certificates/" + name + "/policy"
			w.Policy = &p
		}
	}

	if e != nil && e.Deleted != nil {
		d := *e.Deleted
		w.DeletedDate = &d.DeletedDate
		w.ScheduledPurgeDate = &d.ScheduledPurgeDate
		w.RecoveryID = base + "/deletedcertificates/" + name
	}

	return w
}

func (v *vault) findCert(name, version string) (*Entity[*CertVersion], *CertVersion, error) {
	e, ok := v.Certs.active(name)
	if !ok {
		return nil, nil, errNotFound(kindCertificate, name)
	}

	ver, ok := e.version(version)
	if !ok {
		return nil, nil, errNotFound(kindCertificate, name+"/"+version)
	}

	return e, ver, nil
}

// addCert stores a certificate version plus its backing key and secret.
func (v *vault) addCert(
	name string, certDER, keyDER []byte, pol *CertPolicy, req attrsAndTags, now int64,
) (*CertVersion, error) {
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, errBadParameter("The certificate could not be parsed.")
	}

	if err = v.checkBackingFree(name); err != nil {
		return nil, err
	}

	ce, err := v.Certs.create(kindCertificate, name)
	if err != nil {
		return nil, err
	}

	id := newVersionID()
	attrs := newAttributes(req.attrs, now, v.RetentionDays, protectedVault(v))
	nbf, exp := cert.NotBefore.Unix(), cert.NotAfter.Unix()
	attrs.NBF, attrs.Exp = &nbf, &exp

	ver := &CertVersion{ID: id, X5T: thumbprint(certDER), Cer: certDER, Tags: cloneTags(req.tags), Attrs: attrs}
	ce.Versions = append(ce.Versions, ver)
	ce.CertPolicy = pol

	if err = v.addBacking(name, id, certDER, keyDER, attrs, req.tags, now); err != nil {
		return nil, err
	}

	ce.CertOp = &CertOperation{Status: statusCompleted, Issuer: pol.Issuer, RequestID: newVersionID()}

	return ver, nil
}

// checkBackingFree rejects a certificate whose backing key or secret name is
// already taken by an object the certificate does not own.
func (v *vault) checkBackingFree(name string) error {
	if e, ok := v.Secrets[name]; ok && !e.latest().Managed {
		return newErr(
			http.StatusConflict,
			"Conflict",
			"Secret %s already exists and is not managed by a certificate.",
			name,
		)
	}

	if e, ok := v.Keys[name]; ok && !e.latest().Managed {
		return newErr(
			http.StatusConflict,
			"Conflict",
			"Key %s already exists and is not managed by a certificate.",
			name,
		)
	}

	return nil
}

type attrsAndTags struct {
	tags  map[string]string
	attrs *Attributes
}

func (v *vault) addBacking(
	name, id string,
	certDER, keyDER []byte,
	attrs Attributes,
	tags map[string]string,
	now int64,
) error {
	priv, err := x509.ParsePKCS8PrivateKey(keyDER)
	if err != nil {
		return err
	}

	kty, crv, size := keyInfo(priv)

	keyEnt, err := v.Keys.create(kindKey, name)
	if err != nil {
		return err
	}

	keyEnt.Versions = append(keyEnt.Versions, &KeyVersion{
		ID: id, Kty: kty, Crv: crv, KeySize: size, KeyOps: defaultKeyOps(kty), Priv: keyDER,
		Tags: cloneTags(tags), Attrs: attrs, Managed: true,
	})

	if keyEnt.Rotation == nil {
		keyEnt.Rotation = defaultRotation(now)
	}

	secEnt, err := v.Secrets.create(kindSecret, name)
	if err != nil {
		return err
	}

	secEnt.Versions = append(secEnt.Versions, &SecretVersion{
		ID: id, Value: pemBundle(certDER, keyDER), ContentType: pemContentType,
		Tags: cloneTags(tags), Attrs: attrs, Managed: true,
		KID: "",
	})

	return nil
}

const pemKeyType = "PRIVATE KEY"

func pemBundle(certDER, keyDER []byte) string {
	out := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	return string(append(out, pem.EncodeToMemory(&pem.Block{Type: pemKeyType, Bytes: keyDER})...))
}

// CreateCertificate issues a self-signed certificate.
func (b *InMemoryBackend) CreateCertificate(
	vaultName, base, name string,
	req *CreateCertRequest,
) (CertOperation, error) {
	var out CertOperation

	err := b.do(vaultName, func(v *vault, now int64) error {
		if req.Policy == nil {
			return errBadParameter("The request body must contain the policy property.")
		}

		pol := req.Policy.normalized(now)
		if !strings.EqualFold(pol.Issuer.Name, issuerSelf) {
			return errBadParameter(
				"Issuer %q is not supported; only the %s issuer can issue certificates.",
				pol.Issuer.Name,
				issuerSelf,
			)
		}

		certDER, keyDER, err := selfSign(pol, b.now())
		if err != nil {
			return err
		}

		if _, err = v.addCert(
			name,
			certDER,
			keyDER,
			pol,
			attrsAndTags{tags: req.Tags, attrs: req.Attributes},
			now,
		); err != nil {
			return err
		}

		out = v.Certs[name].CertOp.wire(base, name)

		return nil
	})

	return out, err
}

func (o *CertOperation) wire(base, name string) CertOperation {
	out := *o
	out.ID = base + "/certificates/" + name + "/pending"

	if o.Status == statusCompleted {
		out.Target = base + "/certificates/" + name
	}

	return out
}

// parsePEMBundle extracts the leaf certificate and private key from PEM.
func parsePEMBundle(data []byte) ([]byte, []byte, error) {
	var (
		certs  [][]byte
		keyDER []byte
		err    error
	)

	rest := data

	for {
		var blk *pem.Block

		blk, rest = pem.Decode(rest)
		if blk == nil {
			break
		}

		switch blk.Type {
		case "CERTIFICATE":
			certs = append(certs, blk.Bytes)
		case pemKeyType, "RSA PRIVATE KEY", "EC PRIVATE KEY":
			keyDER, err = toPKCS8(blk)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	if len(certs) == 0 || keyDER == nil {
		return nil, nil, errBadParameter("The PEM value must contain a certificate and its private key.")
	}

	priv, err := x509.ParsePKCS8PrivateKey(keyDER)
	if err != nil {
		return nil, nil, errBadParameter("The private key could not be parsed.")
	}

	for _, c := range certs {
		parsed, pErr := x509.ParseCertificate(c)
		if pErr == nil && samePublicKey(parsed.PublicKey, publicOf(priv)) {
			return c, keyDER, nil
		}
	}

	return nil, nil, errBadParameter("No certificate in the PEM value matches the private key.")
}

func toPKCS8(blk *pem.Block) ([]byte, error) {
	switch blk.Type {
	case pemKeyType:
		return blk.Bytes, nil
	case "RSA PRIVATE KEY":
		k, err := x509.ParsePKCS1PrivateKey(blk.Bytes)
		if err != nil {
			return nil, errBadParameter("The RSA private key could not be parsed.")
		}

		return x509.MarshalPKCS8PrivateKey(k)
	}

	k, err := x509.ParseECPrivateKey(blk.Bytes)
	if err != nil {
		return nil, errBadParameter("The EC private key could not be parsed.")
	}

	return x509.MarshalPKCS8PrivateKey(k)
}

// ImportCertificate imports a PEM certificate with its private key.
func (b *InMemoryBackend) ImportCertificate(vaultName, base, name string, req *ImportCertRequest) (CertWire, error) {
	var out CertWire

	err := b.do(vaultName, func(v *vault, now int64) error {
		raw, err := base64.StdEncoding.DecodeString(req.Value)
		if err != nil {
			raw = []byte(req.Value)
		}

		if !strings.Contains(string(raw), "-----BEGIN") {
			return errBadParameter("Only PEM certificate values are supported; PKCS#12 import is not available.")
		}

		certDER, keyDER, err := parsePEMBundle(raw)
		if err != nil {
			return err
		}

		pol, err := derivePolicy(req.Policy, certDER, keyDER, now)
		if err != nil {
			return err
		}

		ver, err := v.addCert(name, certDER, keyDER, pol, attrsAndTags{tags: req.Tags, attrs: req.Attributes}, now)
		if err != nil {
			return err
		}

		out = ver.wire(base, name, v.Certs[name], true)

		return nil
	})

	return out, err
}

// GetCertificate returns a certificate version (latest when version is empty).
func (b *InMemoryBackend) GetCertificate(vaultName, base, name, version string) (CertWire, error) {
	var out CertWire

	err := b.do(vaultName, func(v *vault, _ int64) error {
		e, ver, err := v.findCert(name, version)
		if err != nil {
			return err
		}

		out = ver.wire(base, name, e, true)

		return nil
	})

	return out, err
}

// UpdateCertificate updates a certificate version's mutable properties.
func (b *InMemoryBackend) UpdateCertificate(
	vaultName, base, name, version string, req *UpdateCertRequest,
) (CertWire, error) {
	var out CertWire

	err := b.do(vaultName, func(v *vault, now int64) error {
		e, ver, err := v.findCert(name, version)
		if err != nil {
			return err
		}

		ver.Attrs.merge(&Attributes{Enabled: attrEnabled(req.Attributes)})
		ver.Attrs.Updated = &now

		if req.Tags != nil {
			ver.Tags = cloneTags(req.Tags)
		}

		out = ver.wire(base, name, e, true)

		return nil
	})

	return out, err
}

func attrEnabled(a *Attributes) *bool {
	if a == nil {
		return nil
	}

	return a.Enabled
}

// GetCertificatePolicy returns a certificate's policy.
func (b *InMemoryBackend) GetCertificatePolicy(vaultName, base, name string) (CertPolicy, error) {
	var out CertPolicy

	err := b.do(vaultName, func(v *vault, _ int64) error {
		e, ok := v.Certs.active(name)
		if !ok {
			return errNotFound(kindCertificate, name)
		}

		out = *e.CertPolicy
		out.ID = base + "/certificates/" + name + "/policy"

		return nil
	})

	return out, err
}

// UpdateCertificatePolicy replaces a certificate's policy (applies to the next version).
func (b *InMemoryBackend) UpdateCertificatePolicy(
	vaultName, base, name string, in *CertPolicy,
) (CertPolicy, error) {
	var out CertPolicy

	err := b.do(vaultName, func(v *vault, now int64) error {
		e, ok := v.Certs.active(name)
		if !ok {
			return errNotFound(kindCertificate, name)
		}

		merged := in.normalized(e.CertPolicy.Attributes.Created0(now))
		if in.X509Props == nil {
			merged.X509Props = e.CertPolicy.X509Props
		}

		e.CertPolicy = merged
		out = *merged
		out.ID = base + "/certificates/" + name + "/policy"

		return nil
	})

	return out, err
}

// Created0 returns the created timestamp or fallback.
func (a *Attributes) Created0(fallback int64) int64 {
	if a == nil || a.Created == nil {
		return fallback
	}

	return *a.Created
}

func (v *vault) setBackingDeleted(name string, info *DeletedInfo) {
	if e, ok := v.Keys[name]; ok && e.latest().Managed {
		e.Deleted = info
	}

	if e, ok := v.Secrets[name]; ok && e.latest().Managed {
		e.Deleted = info
	}
}

// DeleteCertificate soft-deletes a certificate and its backing key/secret.
func (b *InMemoryBackend) DeleteCertificate(vaultName, base, name string) (CertWire, error) {
	var out CertWire

	err := b.do(vaultName, func(v *vault, now int64) error {
		e, err := v.Certs.softDelete(kindCertificate, name, now, v.RetentionDays)
		if err != nil {
			return err
		}

		v.setBackingDeleted(name, e.Deleted)
		out = e.latest().wire(base, name, e, true)

		return nil
	})

	return out, err
}

// GetDeletedCertificate returns a soft-deleted certificate.
func (b *InMemoryBackend) GetDeletedCertificate(vaultName, base, name string) (CertWire, error) {
	var out CertWire

	err := b.do(vaultName, func(v *vault, _ int64) error {
		e, ok := v.Certs.deleted(name)
		if !ok {
			return errNotFound(kindCertificate, name)
		}

		out = e.latest().wire(base, name, e, true)

		return nil
	})

	return out, err
}

// RecoverCertificate restores a soft-deleted certificate.
func (b *InMemoryBackend) RecoverCertificate(vaultName, base, name string) (CertWire, error) {
	var out CertWire

	err := b.do(vaultName, func(v *vault, _ int64) error {
		e, err := v.Certs.recover(kindCertificate, name)
		if err != nil {
			return err
		}

		v.setBackingDeleted(name, nil)
		out = e.latest().wire(base, name, e, true)

		return nil
	})

	return out, err
}

// PurgeCertificate permanently removes a soft-deleted certificate.
func (b *InMemoryBackend) PurgeCertificate(vaultName, name string) error {
	return b.do(vaultName, func(v *vault, _ int64) error {
		if err := v.Certs.purge(kindCertificate, name, v.PurgeProtection); err != nil {
			return err
		}

		if e, ok := v.Keys[name]; ok && e.latest().Managed {
			delete(v.Keys, name)
		}

		if e, ok := v.Secrets[name]; ok && e.latest().Managed {
			delete(v.Secrets, name)
		}

		return nil
	})
}

// ListCertificates lists certificates, deleted or active.
func (b *InMemoryBackend) ListCertificates(vaultName, base string, deleted bool) ([]CertWire, error) {
	var out []CertWire

	err := b.do(vaultName, func(v *vault, _ int64) error {
		for _, n := range v.Certs.names(deleted) {
			e := v.Certs[n]
			out = append(out, e.latest().wire(base, n, e, false))
		}

		return nil
	})

	return out, err
}

// ListCertificateVersions lists every version of a certificate.
func (b *InMemoryBackend) ListCertificateVersions(vaultName, base, name string) ([]CertWire, error) {
	var out []CertWire

	err := b.do(vaultName, func(v *vault, _ int64) error {
		e, ok := v.Certs.active(name)
		if !ok {
			return errNotFound(kindCertificate, name)
		}

		versions := append([]*CertVersion(nil), e.Versions...)
		sort.SliceStable(versions, func(i, j int) bool {
			return *versions[i].Attrs.Created < *versions[j].Attrs.Created
		})

		for _, ver := range versions {
			w := ver.wire(base, name, nil, false)
			w.ID += "/" + ver.ID
			out = append(out, w)
		}

		return nil
	})

	return out, err
}

// GetPendingCertificate returns the certificate's pending operation.
func (b *InMemoryBackend) GetPendingCertificate(vaultName, base, name string) (CertOperation, error) {
	var out CertOperation

	err := b.do(vaultName, func(v *vault, _ int64) error {
		e, ok := v.Certs.active(name)
		if !ok || e.CertOp == nil {
			return newErr(http.StatusNotFound, "PendingCertificateNotFound",
				"Pending certificate not found: %s", name)
		}

		out = e.CertOp.wire(base, name)

		return nil
	})

	return out, err
}

// UpdatePendingCertificate records a cancellation request on the pending operation.
func (b *InMemoryBackend) UpdatePendingCertificate(
	vaultName, base, name string, req *UpdatePendingRequest,
) (CertOperation, error) {
	var out CertOperation

	err := b.do(vaultName, func(v *vault, _ int64) error {
		e, ok := v.Certs.active(name)
		if !ok || e.CertOp == nil {
			return newErr(http.StatusNotFound, "PendingCertificateNotFound", "Pending certificate not found: %s", name)
		}

		e.CertOp.CancellationRequested = req.CancellationRequested
		out = e.CertOp.wire(base, name)

		return nil
	})

	return out, err
}

// DeletePendingCertificate removes the pending operation record.
func (b *InMemoryBackend) DeletePendingCertificate(vaultName, base, name string) (CertOperation, error) {
	var out CertOperation

	err := b.do(vaultName, func(v *vault, _ int64) error {
		e, ok := v.Certs.active(name)
		if !ok || e.CertOp == nil {
			return newErr(http.StatusNotFound, "PendingCertificateNotFound", "Pending certificate not found: %s", name)
		}

		out = e.CertOp.wire(base, name)
		e.CertOp = nil

		return nil
	})

	return out, err
}
