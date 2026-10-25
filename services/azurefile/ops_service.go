package azurefile

import (
	"encoding/xml"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	defaultMaxResults = 5000
	maxCORSRules      = 5
	maxRetentionDays  = 365
)

type metaXML map[string]string

func (m metaXML) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if err := e.EncodeToken(start); err != nil {
		return err
	}

	for k, v := range m {
		if err := e.EncodeElement(v, xml.StartElement{Name: xml.Name{Local: k}}); err != nil {
			return err
		}
	}

	return e.EncodeToken(start.End())
}

type sharePropsXML struct {
	RemainingDays  *int32 `xml:"RemainingRetentionDays,omitempty"`
	SMBDirLease    *bool  `xml:"EnableSMBDirectoryLease,omitempty"`
	SnapshotVDir   *bool  `xml:"EnableSnapshotVirtualDirectoryAccess,omitempty"`
	BurstBandwidth *int64 `xml:"PaidBurstingMaxBandwidthMibps,omitempty"`
	BurstIops      *int64 `xml:"PaidBurstingMaxIops,omitempty"`
	PaidBursting   *bool  `xml:"PaidBurstingEnabled,omitempty"`
	ProvBandwidth  *int64 `xml:"ProvisionedBandwidthMiBps,omitempty"`
	ProvIops       *int64 `xml:"ProvisionedIops,omitempty"`
	TierChanged    string `xml:"AccessTierChangeTime,omitempty"`
	RootSquash     string `xml:"RootSquash,omitempty"`
	DeletedTime    string `xml:"DeletedTime,omitempty"`
	Protocols      string `xml:"EnabledProtocols,omitempty"`
	LeaseDuration  string `xml:"LeaseDuration,omitempty"`
	LeaseState     string `xml:"LeaseState,omitempty"`
	LeaseStatus    string `xml:"LeaseStatus,omitempty"`
	LastModified   string `xml:"Last-Modified"`
	AccessTier     string `xml:"AccessTier"`
	ETag           string `xml:"Etag"`
	Quota          int32  `xml:"Quota"`
}

type shareXML struct {
	Metadata   *metaXML      `xml:"Metadata,omitempty"`
	Deleted    *bool         `xml:"Deleted,omitempty"`
	Name       string        `xml:"Name"`
	Snapshot   string        `xml:"Snapshot,omitempty"`
	Version    string        `xml:"Version,omitempty"`
	Properties sharePropsXML `xml:"Properties"`
}

type listSharesXML struct {
	MaxResults *int     `xml:"MaxResults,omitempty"`
	XMLName    xml.Name `xml:"EnumerationResults"`
	Endpoint   string   `xml:"ServiceEndpoint,attr"`
	Prefix     string   `xml:"Prefix"`
	Marker     string   `xml:"Marker"`
	NextMarker string   `xml:"NextMarker"`
	Shares     struct {
		Share []shareXML `xml:"Share"`
	} `xml:"Shares"`
}

func shareProps(sh *share, now time.Time, retentionDays int32) sharePropsXML {
	p := sharePropsXML{
		LastModified: fmtHTTP(sh.LastModified), ETag: sh.ETag, Quota: sh.Quota, AccessTier: sh.Tier,
		TierChanged: fmtHTTP(sh.TierChanged), Protocols: sh.Protocols, RootSquash: sh.RootSquash,
		ProvIops: sh.Extra.Iops, ProvBandwidth: sh.Extra.Bandwidth, PaidBursting: sh.Extra.PaidBursting,
		BurstIops: sh.Extra.BurstIops, BurstBandwidth: sh.Extra.BurstBandwidth,
		SnapshotVDir: sh.Extra.SnapshotVDirAccess, SMBDirLease: sh.Extra.SMBDirLease,
	}

	h := http.Header{}
	sh.Lease.setHeaders(h, now)
	p.LeaseStatus = h.Get("X-Ms-Lease-Status")
	p.LeaseState = h.Get("X-Ms-Lease-State")
	p.LeaseDuration = h.Get("X-Ms-Lease-Duration")

	if sh.Deleted {
		p.DeletedTime = fmtHTTP(sh.DeletedAt)
		p.RemainingDays = &retentionDays
	}

	return p
}

const (
	entryLive = iota
	entrySnapshot
	entryDeleted
)

type shareEntry struct {
	sh   *share
	kind int
}

// parseList splits a comma separated query value into a folded set.
func parseList(v string) map[string]bool {
	set := map[string]bool{}

	for part := range strings.SplitSeq(v, ",") {
		set[strs.Fold(strings.TrimSpace(part))] = true
	}

	return set
}

func parseMaxResults(v string) (int, *apiError) {
	if v == "" {
		return 0, nil
	}

	n, err := strconv.Atoi(v)
	if err != nil || n < 1 || n > defaultMaxResults {
		return 0, errInvalidQuery("maxresults")
	}

	return n, nil
}

// collectShareEntries lists the account's shares, snapshots and soft-deleted
// shares matching prefix, in listing order.
func collectShareEntries(a *account, prefix string, include map[string]bool, now time.Time) []shareEntry {
	if a == nil {
		return nil
	}

	var entries []shareEntry

	for _, sh := range a.Shares {
		if !strings.HasPrefix(strs.Fold(sh.Name), prefix) {
			continue
		}

		entries = append(entries, shareEntry{sh: sh, kind: entryLive})

		if include["snapshots"] {
			for _, sn := range sh.Snapshots {
				entries = append(entries, shareEntry{sh: sn, kind: entrySnapshot})
			}
		}
	}

	if include["deleted"] {
		days := retentionDays(a)

		for _, d := range a.Deleted {
			if strings.HasPrefix(strs.Fold(d.Name), prefix) && !d.expired(days, now) {
				entries = append(entries, shareEntry{sh: d, kind: entryDeleted})
			}
		}
	}

	sort.Slice(entries, func(i, j int) bool {
		a, b := entries[i], entries[j]
		if x, y := strs.Fold(a.sh.Name), strs.Fold(b.sh.Name); x != y {
			return x < y
		}

		if a.kind != b.kind {
			return a.kind < b.kind
		}

		return a.sh.SnapshotID+a.sh.Version < b.sh.SnapshotID+b.sh.Version
	})

	return entries
}

func (e shareEntry) toXML(now time.Time, days int32, withMetadata bool) shareXML {
	item := shareXML{Name: e.sh.Name, Snapshot: e.sh.SnapshotID, Properties: shareProps(e.sh, now, days)}

	if e.kind == entryDeleted {
		deleted := true
		item.Deleted = &deleted
		item.Version = e.sh.Version
	}

	if withMetadata {
		md := metaXML(e.sh.Metadata)
		item.Metadata = &md
	}

	return item
}

func (h *Handler) listShares(rc *reqCtx) (*resp, *apiError) {
	include := parseList(rc.q.Get("include"))

	maxResults, aerr := parseMaxResults(rc.q.Get("maxresults"))
	if aerr != nil {
		return nil, aerr
	}

	h.Backend.mu.RLock("ListShares")
	defer h.Backend.mu.RUnlock()

	a := h.Backend.peek(rc.acct)
	entries := collectShareEntries(a, strs.Fold(rc.q.Get("prefix")), include, rc.now)
	pg := page.New(entries, rc.q.Get("marker"), maxResults, defaultMaxResults)

	out := listSharesXML{
		Endpoint: rc.endpoint + "/", Prefix: rc.q.Get("prefix"), Marker: rc.q.Get("marker"), NextMarker: pg.Next,
	}
	if maxResults > 0 {
		out.MaxResults = &maxResults
	}

	days := int32(0)
	if a != nil {
		days = retentionDays(a)
	}

	for _, e := range pg.Data {
		out.Shares.Share = append(out.Shares.Share, e.toXML(rc.now, days, include["metadata"]))
	}

	return newResp(http.StatusOK).xml(out), nil
}

func retentionDays(a *account) int32 {
	if a.Props != nil && a.Props.ShareRetention != nil && a.Props.ShareRetention.Enabled &&
		a.Props.ShareRetention.Days != nil {
		return *a.Props.ShareRetention.Days
	}

	return 0
}

func (h *Handler) getServiceProps(rc *reqCtx) (*resp, *apiError) {
	h.Backend.mu.Lock("GetServiceProperties")
	defer h.Backend.mu.Unlock()

	return newResp(http.StatusOK).xml(h.Backend.acct(rc.acct).props()), nil
}

func (h *Handler) setServiceProps(rc *reqCtx) (*resp, *apiError) {
	data, aerr := rc.body()
	if aerr != nil {
		return nil, aerr
	}

	var in serviceProps

	if err := xml.Unmarshal(data, &in); err != nil {
		return nil, errInvalidXML()
	}

	if aerr = validateServiceProps(&in); aerr != nil {
		return nil, aerr
	}

	h.Backend.mu.Lock("SetServiceProperties")
	defer h.Backend.mu.Unlock()

	cur := h.Backend.acct(rc.acct).props()
	if in.HourMetrics != nil {
		cur.HourMetrics = in.HourMetrics
	}

	if in.MinuteMetrics != nil {
		cur.MinuteMetrics = in.MinuteMetrics
	}

	if in.CORS != nil {
		cur.CORS = in.CORS
	}

	if in.Protocol != nil {
		cur.Protocol = in.Protocol
	}

	if in.ShareRetention != nil {
		cur.ShareRetention = in.ShareRetention
	}

	return newResp(http.StatusAccepted), nil
}

const metricsVersion = "1.0"

func validateServiceProps(p *serviceProps) *apiError {
	for _, m := range []*metrics{p.HourMetrics, p.MinuteMetrics} {
		if m == nil {
			continue
		}

		if m.Version != metricsVersion || (m.Enabled && m.IncludeAPIs == nil) {
			return errInvalidXML()
		}

		if err := validateRetention(m.Retention); err != nil {
			return err
		}
	}

	if err := validateRetention(p.ShareRetention); err != nil {
		return err
	}

	return validateCORS(p.CORS)
}

func validateCORS(c *corsWrap) *apiError {
	if c == nil {
		return nil
	}

	if len(c.Rules) > maxCORSRules {
		return errInvalidXML()
	}

	for _, r := range c.Rules {
		if r.AllowedOrigins == "" || r.AllowedMethods == "" || r.MaxAgeInSeconds < 0 {
			return errInvalidXML()
		}

		for m := range strings.SplitSeq(r.AllowedMethods, ",") {
			if !validCORSMethod(strings.ToUpper(strings.TrimSpace(m))) {
				return errInvalidXML()
			}
		}
	}

	return nil
}

func validCORSMethod(m string) bool {
	switch m {
	case "DELETE", "GET", "HEAD", "MERGE", "POST", "OPTIONS", "PUT", "PATCH":
		return true
	}

	return false
}

func validateRetention(r *retentionPolicy) *apiError {
	if r == nil || !r.Enabled {
		return nil
	}

	if r.Days == nil || *r.Days < 1 || *r.Days > maxRetentionDays {
		return errInvalidXML()
	}

	return nil
}
