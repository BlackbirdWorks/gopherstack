package azurekeyvault

import (
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"net/http"
	"strings"
)

const (
	issuerUnknown     = "Unknown"
	statusInProgress  = "inProgress"
	pendingStatusText = "Pending certificate created. Please Perform Merge to complete the request."
	pemBegin          = "-----BEGIN"
)

// PendingCert is a certificate awaiting a signed chain from an external CA.
type PendingCert struct {
	Policy *CertPolicy       `json:"policy"`
	Tags   map[string]string `json:"tags,omitempty"`
	Attrs  Attributes        `json:"attrs"`
	Key    []byte            `json:"key"`
	ID     string            `json:"id"`
	Op     CertOperation     `json:"op"`
}

// MergeCertRequest is the POST /certificates/{name}/pending/merge body.
type MergeCertRequest struct {
	Attributes *Attributes       `json:"attributes"`
	Tags       map[string]string `json:"tags"`
	X5C        []string          `json:"x5c"`
}

func errPendingNotFound(name string) *kvError {
	return newErr(http.StatusNotFound, "PendingCertificateNotFound", "Pending certificate not found: %s", name)
}

// buildCSR returns a DER certificate request for pol signed with keyDER.
func buildCSR(pol *CertPolicy, keyDER []byte) ([]byte, error) {
	if pol.X509Props == nil || pol.X509Props.Subject == "" {
		return nil, errBadParameter("The policy must contain x509_props.subject.")
	}

	subject, err := parseSubject(pol.X509Props.Subject)
	if err != nil {
		return nil, err
	}

	priv, err := x509.ParsePKCS8PrivateKey(keyDER)
	if err != nil {
		return nil, err
	}

	names := &x509.Certificate{}
	if err = applySANs(names, pol.X509Props.SANs); err != nil {
		return nil, err
	}

	return x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject:         subject,
		DNSNames:        names.DNSNames,
		EmailAddresses:  names.EmailAddresses,
		IPAddresses:     names.IPAddresses,
		URIs:            names.URIs,
		ExtraExtensions: names.ExtraExtensions,
	}, priv)
}

// startPending generates a key and CSR for an Unknown-issuer certificate and
// records the pending operation.
func (v *vault) startPending(name string, pol *CertPolicy, req attrsAndTags, now int64) (*PendingCert, error) {
	if _, ok := v.Pending[name]; ok {
		return nil, newErr(http.StatusConflict, "Conflict",
			"A pending operation already exists for certificate %s; delete it first.", name)
	}

	if e, ok := v.Certs[name]; ok && e.Deleted != nil {
		return nil, errDeletedConflict(kindCertificate, name)
	}

	if err := v.checkBackingFree(name); err != nil {
		return nil, err
	}

	if f := ktyFamily(pol.KeyProps.Kty); f != ktyRSA && f != ktyEC {
		return nil, errBadParameter("Certificate key type %q is not supported.", pol.KeyProps.Kty)
	}

	keyDER, _, _, err := generateKeyMaterial(pol.KeyProps.Kty, pol.KeyProps.KeySize, pol.KeyProps.Crv)
	if err != nil {
		return nil, err
	}

	csr, err := buildCSR(pol, keyDER)
	if err != nil {
		return nil, err
	}

	pc := &PendingCert{
		ID: newVersionID(), Key: keyDER, Policy: pol, Tags: cloneTags(req.tags),
		Attrs: newAttributes(req.attrs, now, v.RetentionDays, protectedVault(v)),
		Op: CertOperation{
			Status: statusInProgress, StatusDetails: pendingStatusText, Issuer: pol.Issuer,
			CSR: csr, RequestID: newVersionID(),
		},
	}
	v.Pending[name] = pc

	return pc, nil
}

func (pc *PendingCert) wire(base, name string) CertWire {
	attrs := pc.Attrs
	pol := *pc.Policy
	pol.ID = base + "/certificates/" + name + "/policy"

	return CertWire{
		ID:         base + "/certificates/" + name + "/" + pc.ID,
		KID:        base + "/keys/" + name + "/" + pc.ID,
		SID:        base + "/secrets/" + name + "/" + pc.ID,
		Attributes: &attrs,
		Tags:       pc.Tags,
		Policy:     &pol,
	}
}

// pendingOp returns the in-flight or last-completed operation for name.
func (v *vault) pendingOp(name string) (*CertOperation, bool) {
	if pc, ok := v.Pending[name]; ok {
		return &pc.Op, true
	}

	if e, ok := v.Certs.active(name); ok && e.CertOp != nil {
		return e.CertOp, true
	}

	return nil, false
}

// parseX5C decodes PEM or base64 DER certificates into parsed certificates.
func parseX5C(items []string) ([]*x509.Certificate, error) {
	var ders [][]byte

	for _, item := range items {
		raw := []byte(strings.TrimSpace(item))
		if !strings.Contains(item, pemBegin) {
			dec, err := base64.StdEncoding.DecodeString(string(raw))
			if err != nil {
				return nil, errBadParameter("x5c entries must be base64 DER or PEM certificates.")
			}

			raw = dec
		}

		ders = append(ders, pemOrDER(raw)...)
	}

	if len(ders) == 0 {
		return nil, errBadParameter("The x5c property must contain at least one certificate.")
	}

	out := make([]*x509.Certificate, 0, len(ders))

	for _, der := range ders {
		c, err := x509.ParseCertificate(der)
		if err != nil {
			return nil, errBadParameter("A certificate in x5c could not be parsed.")
		}

		out = append(out, c)
	}

	return out, nil
}

func pemOrDER(raw []byte) [][]byte {
	if !strings.Contains(string(raw), pemBegin) {
		return [][]byte{raw}
	}

	var out [][]byte

	for rest := raw; ; {
		var blk *pem.Block

		blk, rest = pem.Decode(rest)
		if blk == nil {
			return out
		}

		if blk.Type == pemCertType {
			out = append(out, blk.Bytes)
		}
	}
}

// splitChain returns the certificate matching keyDER and the remaining chain.
func splitChain(certs []*x509.Certificate, keyDER []byte) ([]byte, [][]byte, error) {
	priv, err := x509.ParsePKCS8PrivateKey(keyDER)
	if err != nil {
		return nil, nil, err
	}

	var (
		leaf  []byte
		chain [][]byte
	)

	for _, c := range certs {
		if leaf == nil && samePublicKey(c.PublicKey, publicOf(priv)) {
			leaf = c.Raw

			continue
		}

		chain = append(chain, c.Raw)
	}

	if leaf == nil {
		return nil, nil, errBadParameter("No certificate in x5c matches the pending certificate's private key.")
	}

	return leaf, chain, nil
}

// MergeCertificate completes a pending Unknown-issuer certificate with the signed chain.
func (b *InMemoryBackend) MergeCertificate(vaultName, base, name string, req *MergeCertRequest) (CertWire, error) {
	var out CertWire

	err := b.do(vaultName, func(v *vault, now int64) error {
		pc, ok := v.Pending[name]
		if !ok {
			return errPendingNotFound(name)
		}

		if pc.Op.CancellationRequested {
			return newErr(http.StatusConflict, "Conflict", "The pending operation for %s was cancelled.", name)
		}

		certs, err := parseX5C(req.X5C)
		if err != nil {
			return err
		}

		leaf, chain, err := splitChain(certs, pc.Key)
		if err != nil {
			return err
		}

		attrs, tags := pc.mergeInputs(req)

		ver, err := v.addCert(name, certMaterial{cert: leaf, key: pc.Key, chain: chain, id: pc.ID}, pc.Policy,
			attrsAndTags{tags: tags, attrs: attrs}, now)
		if err != nil {
			return err
		}

		delete(v.Pending, name)

		out = ver.wire(base, name, v.Certs[name], true)

		return nil
	})

	return out, err
}

func (pc *PendingCert) mergeInputs(req *MergeCertRequest) (*Attributes, map[string]string) {
	attrs := &Attributes{Enabled: pc.Attrs.Enabled}
	if req.Attributes != nil && req.Attributes.Enabled != nil {
		attrs.Enabled = req.Attributes.Enabled
	}

	if req.Tags != nil {
		return attrs, req.Tags
	}

	return attrs, pc.Tags
}
