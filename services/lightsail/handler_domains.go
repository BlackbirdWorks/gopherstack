package lightsail

import "context"

// domainOps returns the dispatch table for family U (7 ops).
func (h *Handler) domainOps() map[string]opFunc {
	return map[string]opFunc{
		"CreateDomain":      h.handleCreateDomain,
		"DeleteDomain":      h.handleDeleteDomain,
		"GetDomain":         h.handleGetDomain,
		"GetDomains":        h.handleGetDomains,
		"CreateDomainEntry": h.handleCreateDomainEntry,
		"DeleteDomainEntry": h.handleDeleteDomainEntry,
		"UpdateDomainEntry": h.handleUpdateDomainEntry,
	}
}

type domainEntryWire struct {
	Options map[string]string `json:"options,omitempty"`
	ID      string            `json:"id,omitempty"`
	Name    string            `json:"name,omitempty"`
	Target  string            `json:"target,omitempty"`
	Type    string            `json:"type,omitempty"`
	IsAlias bool              `json:"isAlias,omitempty"`
}

func domainEntryFromWire(w domainEntryWire) DomainEntry {
	return DomainEntry{ID: w.ID, Name: w.Name, Type: w.Type, Target: w.Target, IsAlias: w.IsAlias, Options: w.Options}
}

func domainEntryToWire(e DomainEntry) domainEntryWire {
	return domainEntryWire{
		ID:      e.ID,
		IsAlias: e.IsAlias,
		Name:    e.Name,
		Options: e.Options,
		Target:  e.Target,
		Type:    e.Type,
	}
}

type domainWire struct {
	Arn           string                `json:"arn,omitempty"`
	CreatedAt     *float64              `json:"createdAt,omitempty"`
	DomainEntries []domainEntryWire     `json:"domainEntries,omitempty"`
	Location      *resourceLocationWire `json:"location,omitempty"`
	Name          string                `json:"name,omitempty"`
	ResourceType  string                `json:"resourceType,omitempty"`
	SupportCode   string                `json:"supportCode,omitempty"`
	Tags          []tagWire             `json:"tags,omitempty"`
}

func domainToWire(d *Domain) domainWire {
	entries := make([]domainEntryWire, len(d.Entries))
	for i, e := range d.Entries {
		entries[i] = domainEntryToWire(e)
	}

	return domainWire{
		Arn: d.Arn, CreatedAt: epochPtr(d.CreatedAt), DomainEntries: entries, Location: locationToWire(d.Location),
		Name: d.Name, ResourceType: ResourceTypeDomain, SupportCode: d.SupportCode, Tags: mapFromTags(d.Tags),
	}
}

type createDomainRequest struct {
	DomainName string    `json:"domainName"`
	Tags       []tagWire `json:"tags,omitempty"`
}

func (h *Handler) handleCreateDomain(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[createDomainRequest](body)
	if err != nil {
		return nil, err
	}

	op, createErr := h.Backend.CreateDomain(req.DomainName, tagsFromWire(req.Tags))
	if createErr != nil {
		return nil, createErr
	}

	return marshalResponse(opEnvelope(op))
}

type domainNameRequest struct {
	DomainName string `json:"domainName"`
}

func (h *Handler) handleDeleteDomain(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[domainNameRequest](body)
	if err != nil {
		return nil, err
	}

	op, delErr := h.Backend.DeleteDomain(req.DomainName)
	if delErr != nil {
		return nil, delErr
	}

	return marshalResponse(opEnvelope(op))
}

type domainEnvelope struct {
	Domain *domainWire `json:"domain,omitempty"`
}

func (h *Handler) handleGetDomain(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[domainNameRequest](body)
	if err != nil {
		return nil, err
	}

	d, getErr := h.Backend.GetDomain(req.DomainName)
	if getErr != nil {
		return nil, getErr
	}

	w := domainToWire(d)

	return marshalResponse(domainEnvelope{Domain: &w})
}

type domainsListResponse struct {
	NextPageToken string       `json:"nextPageToken,omitempty"`
	Domains       []domainWire `json:"domains,omitempty"`
}

func (h *Handler) handleGetDomains(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[pageTokenRequest](body)
	if err != nil {
		return nil, err
	}

	pg, pgErr := h.Backend.GetDomains(req.PageToken)
	if pgErr != nil {
		return nil, pgErr
	}

	out := make([]domainWire, len(pg.Data))
	for i, d := range pg.Data {
		out[i] = domainToWire(d)
	}

	return marshalResponse(domainsListResponse{Domains: out, NextPageToken: pg.Next})
}

type domainEntryRequest struct {
	DomainName  string          `json:"domainName"`
	DomainEntry domainEntryWire `json:"domainEntry"`
}

func (h *Handler) handleCreateDomainEntry(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[domainEntryRequest](body)
	if err != nil {
		return nil, err
	}

	op, createErr := h.Backend.CreateDomainEntry(req.DomainName, domainEntryFromWire(req.DomainEntry))
	if createErr != nil {
		return nil, createErr
	}

	return marshalResponse(opEnvelope(op))
}

func (h *Handler) handleDeleteDomainEntry(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[domainEntryRequest](body)
	if err != nil {
		return nil, err
	}

	op, delErr := h.Backend.DeleteDomainEntry(req.DomainName, domainEntryFromWire(req.DomainEntry))
	if delErr != nil {
		return nil, delErr
	}

	return marshalResponse(opEnvelope(op))
}

func (h *Handler) handleUpdateDomainEntry(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[domainEntryRequest](body)
	if err != nil {
		return nil, err
	}

	ops, updateErr := h.Backend.UpdateDomainEntry(req.DomainName, domainEntryFromWire(req.DomainEntry))
	if updateErr != nil {
		return nil, updateErr
	}

	return marshalResponse(opsEnvelope(ops))
}
