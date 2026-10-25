package ociidentity

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"
)

const (
	homeRegionKey = "IAD"
	adCount       = 3
	fdCount       = 3
	adPrefix      = "GpSt"
)

type regionWire struct {
	Key  string `json:"key"`
	Name string `json:"name"`
}

//nolint:gochecknoglobals // immutable region table
var regionTable = []regionWire{
	{"IAD", "us-ashburn-1"}, {"PHX", "us-phoenix-1"}, {"ORD", "us-chicago-1"}, {"SJC", "us-sanjose-1"},
	{"YYZ", "ca-toronto-1"}, {"YUL", "ca-montreal-1"}, {"GRU", "sa-saopaulo-1"}, {"VCP", "sa-vinhedo-1"},
	{"SCL", "sa-santiago-1"}, {"BOG", "sa-bogota-1"}, {"QRO", "mx-queretaro-1"}, {"FRA", "eu-frankfurt-1"},
	{"LHR", "uk-london-1"}, {"CWL", "uk-cardiff-1"}, {"AMS", "eu-amsterdam-1"}, {"ZRH", "eu-zurich-1"},
	{"MRS", "eu-marseille-1"}, {"ARN", "eu-stockholm-1"}, {"LIN", "eu-milan-1"}, {"MAD", "eu-madrid-1"},
	{"SYD", "ap-sydney-1"}, {"MEL", "ap-melbourne-1"}, {"NRT", "ap-tokyo-1"}, {"KIX", "ap-osaka-1"},
	{"ICN", "ap-seoul-1"}, {"YNY", "ap-chuncheon-1"}, {"BOM", "ap-mumbai-1"}, {"HYD", "ap-hyderabad-1"},
	{"SIN", "ap-singapore-1"}, {"DXB", "me-dubai-1"}, {"JED", "me-jeddah-1"}, {"AUH", "me-abudhabi-1"},
	{"MTZ", "il-jerusalem-1"}, {"JNB", "af-johannesburg-1"},
}

type tenancyWire struct {
	Tags
	ID            string `json:"id"`
	Name          string `json:"name"`
	Description   string `json:"description"`
	HomeRegionKey string `json:"homeRegionKey"`
}

type regionSubWire struct {
	RegionKey    string `json:"regionKey"`
	RegionName   string `json:"regionName"`
	Status       string `json:"status"`
	IsHomeRegion bool   `json:"isHomeRegion"`
}

type adWire struct {
	Name          string `json:"name"`
	ID            string `json:"id"`
	CompartmentID string `json:"compartmentId"`
}

type fdWire struct {
	Name               string `json:"name"`
	ID                 string `json:"id"`
	CompartmentID      string `json:"compartmentId"`
	AvailabilityDomain string `json:"availabilityDomain"`
}

// stableTail derives a deterministic 52 character id tail from seed.
func stableTail(seed string) string {
	sum := sha256.Sum256([]byte(seed))

	return hex.EncodeToString(sum[:])[:ocidTailLen]
}

// adNames returns the documented "<tenancy prefix>:<REGION>-AD-<n>" availability domain names.
func adNames() []string {
	region := strings.ToUpper(strings.TrimSuffix(Region, "-1"))
	names := make([]string, adCount)

	for i := range names {
		names[i] = adPrefix + ":" + region + "-AD-" + strconv.Itoa(i+1)
	}

	return names
}

// GetTenancy returns the emulated tenancy.
func (b *InMemoryBackend) GetTenancy(id string) (tenancyWire, error) {
	if id != TenancyID {
		return tenancyWire{}, errNotFound(id)
	}

	return tenancyWire{
		ID: id, Name: tenancyName, Description: "Gopherstack emulated tenancy", HomeRegionKey: homeRegionKey,
		Tags: nonNilTags(Tags{}),
	}, nil
}

// ListRegionSubscriptions returns the single home-region subscription.
func (b *InMemoryBackend) ListRegionSubscriptions(id string) ([]regionSubWire, error) {
	if id != TenancyID {
		return nil, errNotFound(id)
	}

	return []regionSubWire{{RegionKey: homeRegionKey, RegionName: Region, Status: "READY", IsHomeRegion: true}}, nil
}

// ListAvailabilityDomains returns the region's availability domains.
func (b *InMemoryBackend) ListAvailabilityDomains(compartment string) ([]adWire, error) {
	if compartment == "" {
		return nil, errMissing("compartmentId is required")
	}

	b.mu.RLock("ListAvailabilityDomains")
	defer b.mu.RUnlock()

	if !b.compartmentExists(compartment) {
		return nil, errNotFound(compartment)
	}

	names := adNames()
	out := make([]adWire, len(names))

	for i, n := range names {
		out[i] = adWire{
			Name: n, CompartmentID: TenancyID,
			ID: "ocid1.availabilitydomain.oc1." + strings.ToLower(homeRegionKey) + "." + stableTail(n),
		}
	}

	return out, nil
}

// ListFaultDomains returns the three fault domains of an availability domain.
func (b *InMemoryBackend) ListFaultDomains(compartment, ad string) ([]fdWire, error) {
	if compartment == "" || ad == "" {
		return nil, errMissing("compartmentId and availabilityDomain are required")
	}

	b.mu.RLock("ListFaultDomains")
	defer b.mu.RUnlock()

	if !b.compartmentExists(compartment) {
		return nil, errNotFound(compartment)
	}

	var canon string

	for _, n := range adNames() {
		if strings.EqualFold(n, ad) {
			canon = n
		}
	}

	if canon == "" {
		return nil, errParam("availabilityDomain " + ad + " does not exist")
	}

	out := make([]fdWire, fdCount)

	for i := range out {
		name := "FAULT-DOMAIN-" + strconv.Itoa(i+1)
		out[i] = fdWire{
			Name: name, AvailabilityDomain: canon, CompartmentID: TenancyID,
			ID: "ocid1.faultdomain.oc1." + strings.ToLower(homeRegionKey) + "." + stableTail(canon+name),
		}
	}

	return out, nil
}

func (h *Handler) getTenancy(q *req) error {
	v, err := h.Backend.GetTenancy(q.param("tenancyId"))

	return reply(q, v, "", err)
}

func (h *Handler) listRegions(q *req) error { return q.json(http.StatusOK, regionTable, "") }

func (h *Handler) listRegionSubscriptions(q *req) error {
	v, err := h.Backend.ListRegionSubscriptions(q.param("tenancyId"))

	return reply(q, v, "", err)
}

func (h *Handler) listAvailabilityDomains(q *req) error {
	v, err := h.Backend.ListAvailabilityDomains(q.query("compartmentId"))

	return reply(q, v, "", err)
}

func (h *Handler) listFaultDomains(q *req) error {
	v, err := h.Backend.ListFaultDomains(q.query("compartmentId"), q.query("availabilityDomain"))

	return reply(q, v, "", err)
}
