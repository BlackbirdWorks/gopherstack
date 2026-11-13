package ocidns

import (
	"encoding/base64"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

var tsigAlgorithms = map[string]bool{ //nolint:gochecknoglobals // immutable lookup table
	"hmac-sha1": true, "hmac-sha224": true, "hmac-sha256": true, "hmac-sha384": true, "hmac-sha512": true,
}

// TsigKeyWire is the wire form of a TSIG key, secret included.
type TsigKeyWire struct {
	TimeCreated    time.Time                 `json:"timeCreated"`
	TimeUpdated    time.Time                 `json:"timeUpdated"`
	FreeformTags   map[string]string         `json:"freeformTags"`
	DefinedTags    map[string]map[string]any `json:"definedTags"`
	Algorithm      string                    `json:"algorithm"`
	Name           string                    `json:"name"`
	CompartmentID  string                    `json:"compartmentId"`
	Secret         string                    `json:"secret"`
	ID             string                    `json:"id"`
	Self           string                    `json:"self"`
	LifecycleState string                    `json:"lifecycleState"`
}

// TsigKeySummaryWire is the list form of a TSIG key: it omits the secret.
type TsigKeySummaryWire struct {
	TimeCreated    time.Time                 `json:"timeCreated"`
	FreeformTags   map[string]string         `json:"freeformTags"`
	DefinedTags    map[string]map[string]any `json:"definedTags"`
	Algorithm      string                    `json:"algorithm"`
	Name           string                    `json:"name"`
	CompartmentID  string                    `json:"compartmentId"`
	ID             string                    `json:"id"`
	Self           string                    `json:"self"`
	LifecycleState string                    `json:"lifecycleState"`
}

func (b *InMemoryBackend) tsigWire(k *TsigKey) TsigKeyWire {
	return TsigKeyWire{
		TimeCreated: k.Created, TimeUpdated: k.Updated, FreeformTags: k.free(), DefinedTags: k.defined(),
		Algorithm: k.Algorithm, Name: k.Name, CompartmentID: k.CompartmentID, Secret: k.Secret, ID: k.ID,
		Self: b.self("tsigKeys", k.ID), LifecycleState: stateActive,
	}
}

func (b *InMemoryBackend) tsigSummary(k *TsigKey) TsigKeySummaryWire {
	return TsigKeySummaryWire{
		TimeCreated: k.Created, FreeformTags: k.free(), DefinedTags: k.defined(), Algorithm: k.Algorithm,
		Name: k.Name, CompartmentID: k.CompartmentID, ID: k.ID, Self: b.self("tsigKeys", k.ID),
		LifecycleState: stateActive,
	}
}

func (b *InMemoryBackend) tsigByID(id string) (*TsigKey, *APIError) {
	k, ok := b.tsigs[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return k, nil
}

// CreateTsigKeyIn is the body of CreateTsigKey.
type CreateTsigKeyIn struct {
	Tags
	Algorithm     string `json:"algorithm"`
	Name          string `json:"name"`
	CompartmentID string `json:"compartmentId"`
	Secret        string `json:"secret"`
}

func (b *InMemoryBackend) validateTsig(in *CreateTsigKeyIn) *APIError {
	if err := b.checkCompartment(in.CompartmentID); err != nil {
		return err
	}

	if in.Algorithm == "" || in.Name == "" || in.Secret == "" {
		return errMissing("algorithm, name and secret are required")
	}

	in.Algorithm = strings.ToLower(strings.TrimSuffix(in.Algorithm, "."))
	if !tsigAlgorithms[in.Algorithm] {
		return errParam("algorithm must be one of hmac-sha1, hmac-sha224, hmac-sha256, hmac-sha384, hmac-sha512")
	}

	name, ok := normName(in.Name, false)
	if !ok {
		return errParam("name must be a domain name")
	}

	in.Name = name

	if _, err := base64.StdEncoding.DecodeString(in.Secret); err != nil {
		return errParam("secret must be a base64 string")
	}

	for _, k := range b.tsigs {
		if strs.Equal(k.Name, in.Name) {
			return errExists("A TSIG key named " + in.Name + " already exists")
		}
	}

	return nil
}

// CreateTsigKey creates a TSIG key.
func (b *InMemoryBackend) CreateTsigKey(in CreateTsigKeyIn) (TsigKeyWire, tag, error) {
	b.mu.Lock("CreateTsigKey")
	defer b.mu.Unlock()

	if err := firstAPIErr(b.validateTsig(&in), b.checkTags(in.Tags)); err != nil {
		return TsigKeyWire{}, tag{}, err
	}

	now := b.nowUTC()
	k := &TsigKey{
		ID: newOCID("tsigkey"), CompartmentID: in.CompartmentID, Name: in.Name, Algorithm: in.Algorithm,
		Secret: in.Secret, Created: now, Updated: now, Seq: b.nextSeq(), Tags: in.Tags,
	}
	k.touch(now)
	b.tsigs[k.ID] = k

	return b.tsigWire(k), tagOf(k.ID, k.meta), nil
}

// GetTsigKey returns a TSIG key.
func (b *InMemoryBackend) GetTsigKey(id string) (TsigKeyWire, tag, error) {
	b.mu.RLock("GetTsigKey")
	defer b.mu.RUnlock()

	k, err := b.tsigByID(id)
	if err != nil {
		return TsigKeyWire{}, tag{}, err
	}

	return b.tsigWire(k), tagOf(k.ID, k.meta), nil
}

// TsigFilter selects TSIG keys in a list.
type TsigFilter struct {
	Compart string
	ID      string
	Name    string
	State   string
}

var tsigSorters = sorter[*TsigKey]{ //nolint:gochecknoglobals // immutable comparator table
	keyName:        byString(func(k *TsigKey) string { return k.Name }, func(k *TsigKey) int64 { return k.Seq }),
	keyTimeCreated: byCreated(func(k *TsigKey) time.Time { return k.Created }, func(k *TsigKey) int64 { return k.Seq }),
}

// ListTsigKeys lists the TSIG keys of a compartment; secrets are never included.
func (b *InMemoryBackend) ListTsigKeys(f TsigFilter, o ListOpts) (listResult[TsigKeySummaryWire], error) {
	b.mu.RLock("ListTsigKeys")
	defer b.mu.RUnlock()

	if err := b.checkCompartment(f.Compart); err != nil {
		return listResult[TsigKeySummaryWire]{}, err
	}

	var all []*TsigKey

	for _, k := range b.tsigs {
		if k.CompartmentID == f.Compart && (f.ID == "" || k.ID == f.ID) &&
			(f.Name == "" || strs.Equal(k.Name, f.Name)) &&
			(f.State == "" || strings.EqualFold(f.State, stateActive)) {
			all = append(all, k)
		}
	}

	pg, err := sortPage(all, o, keyTimeCreated, true, tsigSorters)
	if err != nil {
		return listResult[TsigKeySummaryWire]{}, err
	}

	return finish(pg, len(all), b.tsigSummary), nil
}

// UpdateTsigKeyIn is the body of UpdateTsigKey.
type UpdateTsigKeyIn struct {
	Tags
}

// UpdateTsigKey changes the tags of a TSIG key.
func (b *InMemoryBackend) UpdateTsigKey(id string, c cond, in UpdateTsigKeyIn) (TsigKeyWire, tag, error) {
	b.mu.Lock("UpdateTsigKey")
	defer b.mu.Unlock()

	k, err := b.tsigByID(id)
	if err != nil {
		return TsigKeyWire{}, tag{}, err
	}

	if err = firstAPIErr(c.check(k.ID, k.meta), b.checkTags(in.Tags)); err != nil {
		return TsigKeyWire{}, tag{}, err
	}

	k.Tags.merge(in.Tags)
	now := b.nowUTC()
	k.Updated = now
	k.touch(now)

	return b.tsigWire(k), tagOf(k.ID, k.meta), nil
}

// DeleteTsigKey deletes a TSIG key that no zone references.
func (b *InMemoryBackend) DeleteTsigKey(id string, c cond) error {
	b.mu.Lock("DeleteTsigKey")
	defer b.mu.Unlock()

	k, err := b.tsigByID(id)
	if err != nil {
		return err
	}

	if err = c.check(k.ID, k.meta); err != nil {
		return err
	}

	for _, z := range b.zones {
		if usesTsig(z, id) {
			return errConflict("The TSIG key is referenced by zone " + z.Name)
		}
	}

	delete(b.tsigs, id)

	return nil
}

// ChangeTsigKeyCompartment moves a TSIG key.
func (b *InMemoryBackend) ChangeTsigKeyCompartment(id string, c cond, in ChangeCompartmentIn) error {
	b.mu.Lock("ChangeTsigKeyCompartment")
	defer b.mu.Unlock()

	k, err := b.tsigByID(id)
	if err != nil {
		return err
	}

	if err = c.check(k.ID, k.meta); err != nil {
		return err
	}

	if err = b.checkCompartment(in.CompartmentID); err != nil {
		return err
	}

	k.CompartmentID = in.CompartmentID
	k.touch(b.nowUTC())

	return nil
}
