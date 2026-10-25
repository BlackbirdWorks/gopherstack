package ociobjectstorage

import (
	"crypto/rand"
	"encoding/base64"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	parTokenBytes = 48
	idHexLen      = 32
	maxPARName    = 255

	parObjectRead         = "ObjectRead"
	parObjectWrite        = "ObjectWrite"
	parObjectReadWrite    = "ObjectReadWrite"
	parAnyObjectRead      = "AnyObjectRead"
	parAnyObjectWrite     = "AnyObjectWrite"
	parAnyObjectReadWrite = "AnyObjectReadWrite"

	parListObjects = "ListObjects"
	parDeny        = "Deny"
)

// CreatePARInput is the CreatePreauthenticatedRequestDetails payload.
type CreatePARInput struct {
	TimeExpires         time.Time `json:"timeExpires"`
	Name                string    `json:"name"`
	AccessType          string    `json:"accessType"`
	ObjectName          string    `json:"objectName"`
	BucketListingAction string    `json:"bucketListingAction"`
}

// PARWire is the PreauthenticatedRequest / PreauthenticatedRequestSummary body.
type PARWire struct {
	TimeExpires         time.Time `json:"timeExpires"`
	TimeCreated         time.Time `json:"timeCreated"`
	ObjectName          *string   `json:"objectName"`
	AccessURI           string    `json:"accessUri,omitempty"`
	ID                  string    `json:"id"`
	Name                string    `json:"name"`
	AccessType          string    `json:"accessType"`
	BucketListingAction string    `json:"bucketListingAction,omitempty"`
}

func parAccessURI(ns, bucket string, p *PAR) string {
	return "/p/" + p.Token + "/n/" + ns + "/b/" + bucket + "/o/" + url.PathEscape(p.ObjectName)
}

func parWire(p *PAR) PARWire {
	w := PARWire{
		ID: p.ID, Name: p.Name, AccessType: p.AccessType, TimeExpires: p.TimeExpires, TimeCreated: p.TimeCreated,
		BucketListingAction: p.BucketListingAction,
	}
	if p.ObjectName != "" {
		w.ObjectName = &p.ObjectName
	}

	return w
}

func isAnyObject(t string) bool {
	return t == parAnyObjectRead || t == parAnyObjectWrite || t == parAnyObjectReadWrite
}

func parAllowsRead(t string) bool {
	return t == parObjectRead || t == parObjectReadWrite || t == parAnyObjectRead || t == parAnyObjectReadWrite
}

func parAllowsWrite(t string) bool {
	return t == parObjectWrite || t == parObjectReadWrite || t == parAnyObjectWrite || t == parAnyObjectReadWrite
}

func (in CreatePARInput) validate() (string, error) {
	if in.Name == "" || len(in.Name) > maxPARName {
		return "", errParam("name is required and must be at most 255 characters")
	}

	switch in.AccessType {
	case parObjectRead, parObjectWrite, parObjectReadWrite, parAnyObjectRead, parAnyObjectWrite, parAnyObjectReadWrite:
	default:
		return "", errParam("Invalid accessType")
	}

	if in.TimeExpires.IsZero() {
		return "", errMissing("timeExpires is required")
	}

	if !isAnyObject(in.AccessType) && in.ObjectName == "" {
		return "", errMissing("objectName is required for object-level access types")
	}

	action := in.BucketListingAction
	if action == "" {
		action = parDeny
	}

	if action != parDeny && action != parListObjects {
		return "", errParam("Invalid bucketListingAction")
	}

	if action == parListObjects && in.AccessType != parAnyObjectRead && in.AccessType != parAnyObjectReadWrite {
		return "", errParam("bucketListingAction ListObjects requires AnyObjectRead or AnyObjectReadWrite")
	}

	return action, nil
}

// CreatePAR creates a pre-authenticated request; the returned wire includes the accessUri.
func (b *InMemoryBackend) CreatePAR(ns, bucket string, in CreatePARInput) (PARWire, error) {
	action, err := in.validate()
	if err != nil {
		return PARWire{}, err
	}

	b.mu.Lock("CreatePAR")
	defer b.mu.Unlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return PARWire{}, err
	}

	now := b.nowUTC()
	if !in.TimeExpires.After(now) {
		return PARWire{}, errParam("timeExpires must be in the future")
	}

	raw := make([]byte, parTokenBytes)
	_, _ = rand.Read(raw)

	p := &PAR{
		ID: awsid.NewN("", idHexLen), Name: in.Name, Token: base64.RawURLEncoding.EncodeToString(raw),
		AccessType: in.AccessType, ObjectName: in.ObjectName, BucketListingAction: action,
		TimeCreated: now, TimeExpires: in.TimeExpires.UTC(),
	}
	bk.PARs[p.ID] = p

	w := parWire(p)
	w.AccessURI = parAccessURI(ns, bucket, p)

	return w, nil
}

// GetPAR returns a PAR (without its accessUri, which is only shown at creation).
func (b *InMemoryBackend) GetPAR(ns, bucket, id string) (PARWire, error) {
	b.mu.RLock("GetPAR")
	defer b.mu.RUnlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return PARWire{}, err
	}

	p, ok := bk.PARs[id]
	if !ok {
		return PARWire{}, newErr(
			http.StatusNotFound,
			"PreauthenticatedRequestNotFound",
			"The pre-authenticated request '"+id+"' was not found",
		)
	}

	return parWire(p), nil
}

// DeletePAR revokes a PAR.
func (b *InMemoryBackend) DeletePAR(ns, bucket, id string) error {
	b.mu.Lock("DeletePAR")
	defer b.mu.Unlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return err
	}

	if _, ok := bk.PARs[id]; !ok {
		return newErr(
			http.StatusNotFound,
			"PreauthenticatedRequestNotFound",
			"The pre-authenticated request '"+id+"' was not found",
		)
	}

	delete(bk.PARs, id)

	return nil
}

// ListPARs lists PARs ordered by creation time.
func (b *InMemoryBackend) ListPARs(ns, bucket, prefix, token string, limit int) (page.Page[PARWire], error) {
	b.mu.RLock("ListPARs")
	defer b.mu.RUnlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return page.Page[PARWire]{}, err
	}

	ps := make([]*PAR, 0, len(bk.PARs))

	for _, p := range bk.PARs {
		if strings.HasPrefix(p.ObjectName, prefix) {
			ps = append(ps, p)
		}
	}

	sort.Slice(ps, func(i, j int) bool {
		if !ps[i].TimeCreated.Equal(ps[j].TimeCreated) {
			return ps[i].TimeCreated.Before(ps[j].TimeCreated)
		}

		return ps[i].ID < ps[j].ID
	})

	all := make([]PARWire, len(ps))
	for i, p := range ps {
		all[i] = parWire(p)
	}

	return page.New(all, token, limit, defaultListLimit), nil
}

// PARAccess is what a PAR token is being used for.
type PARAccess int

// PAR usage kinds.
const (
	PARRead PARAccess = iota
	PARWrite
	PARList
)

func (p *PAR) permits(object string, use PARAccess) bool {
	if use == PARList {
		return parAllowsRead(p.AccessType) && p.BucketListingAction == parListObjects
	}

	allowed := parAllowsRead(p.AccessType)
	if use == PARWrite {
		allowed = parAllowsWrite(p.AccessType)
	}

	if isAnyObject(p.AccessType) {
		return allowed && strings.HasPrefix(object, p.ObjectName)
	}

	return allowed && object == p.ObjectName
}

// AuthorizePAR checks token against the bucket/object/use; every failure is the same opaque 404.
func (b *InMemoryBackend) AuthorizePAR(token, ns, bucket, object string, use PARAccess) error {
	deny := newErr(
		http.StatusNotFound,
		"NotAuthorizedOrNotFound",
		"Authorization failed or requested resource not found",
	)

	b.mu.RLock("AuthorizePAR")
	defer b.mu.RUnlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return deny
	}

	now := b.nowUTC()

	for _, p := range bk.PARs {
		if p.Token != token || !now.Before(p.TimeExpires) {
			continue
		}

		if p.permits(object, use) {
			return nil
		}
	}

	return deny
}
