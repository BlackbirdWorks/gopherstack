package ociapigateway

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

type createCertificateIn struct {
	Tags
	CompartmentID string          `json:"compartmentId"`
	DisplayName   string          `json:"displayName"`
	PrivateKey    string          `json:"privateKey"`
	Certificate   string          `json:"certificate"`
	Intermediate  string          `json:"intermediateCertificates"`
	Locks         json.RawMessage `json:"locks"`
}

type updateCertificateIn struct {
	Tags
	DisplayName *string `json:"displayName"`
}

// CertificateWire is the wire form of a certificate.
type CertificateWire struct {
	TimeCreated       time.Time                 `json:"timeCreated"`
	TimeUpdated       time.Time                 `json:"timeUpdated"`
	TimeNotValidAfter time.Time                 `json:"timeNotValidAfter"`
	DefinedTags       map[string]map[string]any `json:"definedTags,omitempty"`
	FreeformTags      map[string]string         `json:"freeformTags,omitempty"`
	Locks             json.RawMessage           `json:"locks,omitempty"`
	ID                string                    `json:"id"`
	CompartmentID     string                    `json:"compartmentId"`
	DisplayName       string                    `json:"displayName"`
	Certificate       string                    `json:"certificate,omitempty"`
	Intermediate      string                    `json:"intermediateCertificates,omitempty"`
	LifecycleState    string                    `json:"lifecycleState"`
	SubjectNames      []string                  `json:"subjectNames"`
}

func certificateView(c *Certificate, full bool) CertificateWire {
	w := CertificateWire{
		TimeCreated: c.TimeCreated, TimeUpdated: c.TimeUpdated, TimeNotValidAfter: c.TimeNotValidAfter,
		DefinedTags: c.Tags.DefinedTags, FreeformTags: c.Tags.FreeformTags, Locks: c.Locks, ID: c.ID,
		CompartmentID: c.CompartmentID, DisplayName: c.DisplayName, LifecycleState: c.State,
		SubjectNames: c.SubjectNames,
	}
	if full {
		w.Certificate, w.Intermediate = c.CertPEM, c.Intermediate
	}

	return w
}

// parseCertificate checks that the PEM chain parses and that the private key matches the leaf; the key is not kept.
func parseCertificate(in createCertificateIn) (*x509.Certificate, *APIError) {
	if in.Certificate == "" || in.PrivateKey == "" {
		return nil, errMissing("certificate and privateKey are required")
	}

	pair, err := tls.X509KeyPair([]byte(in.Certificate), []byte(in.PrivateKey))
	if err != nil {
		return nil, errParam("certificate and privateKey are not a matching PEM pair: " + err.Error())
	}

	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return nil, errParam("certificate is not a valid X.509 certificate")
	}

	if in.Intermediate != "" {
		rest, n := []byte(in.Intermediate), 0

		for {
			var blk *pem.Block

			blk, rest = pem.Decode(rest)
			if blk == nil {
				break
			}

			if _, perr := x509.ParseCertificate(blk.Bytes); perr != nil {
				return nil, errParam("intermediateCertificates contains an invalid certificate")
			}

			n++
		}

		if n == 0 {
			return nil, errParam("intermediateCertificates contains no PEM certificate")
		}
	}

	return leaf, nil
}

func subjectNames(c *x509.Certificate) []string {
	names := []string{}
	if c.Subject.CommonName != "" {
		names = append(names, c.Subject.CommonName)
	}

	for _, d := range c.DNSNames {
		if d != c.Subject.CommonName {
			names = append(names, d)
		}
	}

	return names
}

func (b *InMemoryBackend) certNameTaken(compartment, name, except string) bool {
	for _, c := range b.certificates {
		if c.ID != except && c.CompartmentID == compartment && c.State != stateDeleted &&
			strs.Equal(c.DisplayName, name) {
			return true
		}
	}

	return false
}

// CreateCertificate stores a certificate after validating the PEM material.
func (b *InMemoryBackend) CreateCertificate(in createCertificateIn) (CertificateWire, string, string, error) {
	b.mu.Lock("CreateCertificate")
	defer b.mu.Unlock()

	if err := b.checkCompartment(in.CompartmentID); err != nil {
		return CertificateWire{}, "", "", err
	}

	leaf, err := parseCertificate(in)
	if err != nil {
		return CertificateWire{}, "", "", err
	}

	now := b.nowUTC()
	name := in.DisplayName

	if name == "" {
		name = "certificate" + now.Format("20060102150405")
	}

	if b.certNameTaken(in.CompartmentID, name, "") {
		return CertificateWire{}, "", "", errConflict(
			"a certificate named " + name + " already exists in this compartment",
		)
	}

	c := &Certificate{
		ID: newOCID("apigatewaycertificate"), CompartmentID: in.CompartmentID, DisplayName: name,
		CertPEM: in.Certificate, Intermediate: in.Intermediate, SubjectNames: subjectNames(leaf),
		TimeNotValidAfter: leaf.NotAfter.UTC(), Locks: nullRaw(in.Locks), State: stateCreating,
		TransitionAt: b.transitionAt(), TimeCreated: now, TimeUpdated: now, Etag: newEtag(), Tags: in.Tags,
	}
	b.certificates[c.ID] = c
	wr := b.newWR(wrCreateCert, c.CompartmentID, entityCert, c.ID, actionCreated, *c.TransitionAt)

	return certificateView(c, true), wr, c.Etag, nil
}

func (b *InMemoryBackend) liveCertificate(id string) (*Certificate, *APIError) {
	c, ok := b.certificates[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return c, nil
}

// GetCertificate returns one certificate and its etag.
func (b *InMemoryBackend) GetCertificate(id string) (CertificateWire, string, error) {
	b.mu.RLock("GetCertificate")
	defer b.mu.RUnlock()

	c, err := b.liveCertificate(id)
	if err != nil {
		return CertificateWire{}, "", err
	}

	return certificateView(c, true), c.Etag, nil
}

// ListCertificates lists the certificates of a compartment.
func (b *InMemoryBackend) ListCertificates(
	compartment, name, state string, o ListOpts,
) (page.Page[CertificateWire], error) {
	if compartment == "" {
		return page.Page[CertificateWire]{}, errMissing("compartmentId is required")
	}

	b.mu.RLock("ListCertificates")
	defer b.mu.RUnlock()

	var all []*Certificate

	for _, c := range b.certificates {
		if c.CompartmentID != compartment || !stateMatches(c.State, state) ||
			(name != "" && !strs.Equal(c.DisplayName, name)) {
			continue
		}

		all = append(all, c)
	}

	pg := paginate(all, o, func(c *Certificate) string { return c.DisplayName },
		func(c *Certificate) (time.Time, int64) { return c.TimeCreated, 0 })

	out := make([]CertificateWire, 0, len(pg.Data))
	for _, c := range pg.Data {
		out = append(out, certificateView(c, false))
	}

	return page.Page[CertificateWire]{Data: out, Next: pg.Next}, nil
}

// UpdateCertificate updates the display name and tags.
func (b *InMemoryBackend) UpdateCertificate(id, ifMatch string, in updateCertificateIn) (string, error) {
	b.mu.Lock("UpdateCertificate")
	defer b.mu.Unlock()

	c, err := b.liveCertificate(id)
	if err != nil {
		return "", err
	}

	if c.State != stateActive {
		return "", errState("certificate " + id + " is in state " + c.State)
	}

	if err = checkEtag(c.Etag, ifMatch); err != nil {
		return "", err
	}

	if in.DisplayName != nil && *in.DisplayName != "" {
		if b.certNameTaken(c.CompartmentID, *in.DisplayName, c.ID) {
			return "", errConflict("a certificate named " + *in.DisplayName + " already exists in this compartment")
		}

		c.DisplayName = *in.DisplayName
	}

	c.Tags.merge(in.Tags)
	c.State, c.TransitionAt = stateUpdating, b.transitionAt()
	c.TimeUpdated, c.Etag = b.nowUTC(), newEtag()

	return b.newWR(wrUpdateCert, c.CompartmentID, entityCert, id, actionUpdated, *c.TransitionAt), nil
}

// DeleteCertificate starts deleting a certificate that no gateway uses.
func (b *InMemoryBackend) DeleteCertificate(id, ifMatch string) (string, error) {
	b.mu.Lock("DeleteCertificate")
	defer b.mu.Unlock()

	c, err := b.liveCertificate(id)
	if err != nil {
		return "", err
	}

	if c.State != stateActive {
		return "", errState("certificate " + id + " is in state " + c.State)
	}

	if err = checkEtag(c.Etag, ifMatch); err != nil {
		return "", err
	}

	for _, g := range b.gateways {
		if g.CertificateID == id && g.State != stateDeleted {
			return "", errConflict("certificate " + id + " is used by gateway " + g.ID)
		}
	}

	c.State, c.TransitionAt = stateDeleting, b.transitionAt()
	c.TimeUpdated = b.nowUTC()

	return b.newWR(wrDeleteCert, c.CompartmentID, entityCert, id, actionDeleted, *c.TransitionAt), nil
}

// ChangeCertificateCompartment moves a certificate.
func (b *InMemoryBackend) ChangeCertificateCompartment(id, ifMatch string, in changeCompartmentIn) (string, error) {
	b.mu.Lock("ChangeCertificateCompartment")
	defer b.mu.Unlock()

	c, err := b.liveCertificate(id)
	if err != nil {
		return "", err
	}

	if err = b.checkMove(c.Etag, ifMatch, in.CompartmentID); err != nil {
		return "", err
	}

	if b.certNameTaken(in.CompartmentID, c.DisplayName, c.ID) {
		return "", errConflict("a certificate named " + c.DisplayName + " already exists in the target compartment")
	}

	c.CompartmentID, c.TimeUpdated, c.Etag = in.CompartmentID, b.nowUTC(), newEtag()

	return b.newWR(wrUpdateCert, c.CompartmentID, entityCert, id, actionUpdated, b.nowUTC().Add(b.transition)), nil
}

func (h *Handler) createCertificate(q *req) error {
	return created(q, "/certificates/", func(w CertificateWire) string { return w.ID }, h.Backend.CreateCertificate)
}

func (h *Handler) getCertificate(q *req) error {
	w, etag, err := h.Backend.GetCertificate(q.param("certificateId"))

	return reply(q, w, etag, err)
}

func (h *Handler) listCertificates(q *req) error {
	pg, err := h.Backend.ListCertificates(q.query("compartmentId"), q.query("displayName"),
		q.query("lifecycleState"), q.listOpts())

	return collectionReply(q, pg, err)
}

func (h *Handler) updateCertificate(q *req) error {
	return withBody(q, func(in updateCertificateIn) (string, error) {
		return h.Backend.UpdateCertificate(q.param("certificateId"), q.ifMatch(), in)
	})
}

func (h *Handler) deleteCertificate(q *req) error {
	wr, err := h.Backend.DeleteCertificate(q.param("certificateId"), q.ifMatch())

	return acceptedWR(q, wr, err)
}

func (h *Handler) changeCertificateCompartment(q *req) error {
	return withBody(q, func(in changeCompartmentIn) (string, error) {
		return h.Backend.ChangeCertificateCompartment(q.param("certificateId"), q.ifMatch(), in)
	})
}
