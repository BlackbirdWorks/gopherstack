package azurearm

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
)

const (
	fileSharesSeg      = "shares"
	fileSharesListLen  = 11
	fileSharesItemLen  = 12
	fileSharesActLen   = 13
	fileSharesNameIdx  = 11
	fileSharesDefIdx   = 9
	fileSharesSegIdx   = 10
	fileShareType      = "Microsoft.Storage/storageAccounts/fileServices/shares"
	fileShareTimeLayer = "2006-01-02T15:04:05.0000000Z"
	fileShareDefaultKV = "snapshots"
)

// FileShareACL is one stored access policy of a share.
type FileShareACL struct {
	ID         string
	Start      string
	Expiry     string
	Permission string
}

// FileShareInput is the writable part of a Microsoft.Storage file share.
type FileShareInput struct {
	Metadata       map[string]string
	Quota          *int64
	Iops           *int64
	Bandwidth      *int64
	BurstIops      *int64
	BurstBandwidth *int64
	PaidBursting   *bool
	Tier           string
	Protocols      string
	RootSquash     string
	ACL            []FileShareACL
	HasMetadata    bool
	HasACL         bool
}

// FileShareView is a share, share snapshot or soft-deleted share as the data plane holds it.
type FileShareView struct {
	Created        time.Time
	LastModified   time.Time
	TierChanged    time.Time
	DeletedAt      time.Time
	Metadata       map[string]string
	Iops           *int64
	Bandwidth      *int64
	BurstIops      *int64
	BurstBandwidth *int64
	PaidBursting   *bool
	SnapshotVDir   *bool
	Name           string
	ETag           string
	Tier           string
	Protocols      string
	RootSquash     string
	Snapshot       string
	LeaseStatus    string
	LeaseState     string
	LeaseDuration  string
	Version        string
	ACL            []FileShareACL
	Usage          int64
	RemainingDays  int32
	Quota          int32
	Deleted        bool
}

// FileShareError is a data-plane refusal carried to the client with its own status and code.
type FileShareError struct {
	Code    string
	Message string
	Status  int
}

func (e *FileShareError) Error() string { return e.Code + ": " + e.Message }

// FileShares is the seam the Microsoft.Storage fileServices/shares resource uses to reach the Azure Files data
// plane. Implementations return *FileShareError for refusals.
type FileShares interface {
	PutFileShare(account, name string, in FileShareInput, create bool) (FileShareView, bool, error)
	GetFileShare(account, name, snapshot string) (FileShareView, error)
	ListFileShares(account string, snapshots, deleted bool) []FileShareView
	DeleteFileShare(account, name, snapshot, include string) (bool, error)
	RestoreFileShare(account, target, deletedName, deletedVersion string) error
}

// SetFileShares gives the Microsoft.Storage provider (if any) the Azure Files data plane.
func (r *Registry) SetFileShares(fs FileShares) {
	p, ok := r.ProviderNamed(namespaceMicrosoftStorage)
	if !ok {
		return
	}

	if sp, spOk := p.(*StorageProvider); spOk {
		sp.fileShares = fs
	}
}

func fileShareErrorDetails(err error) (errorEntry, bool) {
	var fe *FileShareError
	if !errors.As(err, &fe) {
		return errorEntry{}, false
	}

	return errorEntry{code: fe.Code, message: fe.Message, status: fe.Status}, true
}

func isStorageFileShareScope(segs []string, n int) bool {
	return len(segs) == n && isSubscriptionScoped(segs) && segAt(segs, acrGroupsSegIdx, resourceGroupsSegment) &&
		segAt(segs, acrProvidersInGroupSegIdx, providersSegment) &&
		segAt(segs, acrProviderNsIdx, namespaceMicrosoftStorage) && segAt(segs, storageTypeIdx, storageAccountsType) &&
		segAt(segs, storageChildTypeIdx, fileServicesType) && segAt(segs, fileSharesDefIdx, fileServicesDefault) &&
		segAt(segs, fileSharesSegIdx, fileSharesSeg)
}

//nolint:gochecknoglobals // static route table, read-only after init
var storageFileShareRoutes = []armRoute{
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodGet && isStorageFileShareScope(segs, fileSharesListLen)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleFileShareList(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodPost && isStorageFileShareScope(segs, fileSharesActLen) &&
				strings.EqualFold(segs[fileSharesActLen-1], "restore")
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleFileShareRestore(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			switch r.Method {
			case http.MethodGet, http.MethodPut, http.MethodPatch, http.MethodDelete:
				return isStorageFileShareScope(segs, fileSharesItemLen)
			}

			return false
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleFileShare(c, segs) },
	},
}

// fileShareScope resolves the owning account and the data plane; it writes the error response itself.
func (h *Handler) fileShareScope(c *echo.Context, segs []string) (FileShares, ResourceID, bool, error) {
	p, ok := h.Registry.ProviderNamed(namespaceMicrosoftStorage)

	sp, spOk := p.(*StorageProvider)
	if !ok || !spOk || sp.fileShares == nil {
		return nil, ResourceID{}, false, h.writeNotFound(c)
	}

	id, err := ParseGenericResourcePath("/" + joinSegs(segs[:storageChildTypeIdx]))
	if err != nil {
		return nil, ResourceID{}, false, h.writeAPIError(c, err)
	}

	if !sp.ownsAccount(id) {
		return nil, ResourceID{}, false, h.writeAPIError(c, ErrStorageAccountNotFound)
	}

	return sp.fileShares, id, true, nil
}

func (h *Handler) handleFileShare(c *echo.Context, segs []string) error {
	fs, id, ok, err := h.fileShareScope(c, segs)
	if !ok {
		return err
	}

	account, name := id.LeafName(), segs[fileSharesNameIdx]
	r := c.Request()

	switch r.Method {
	case http.MethodGet:
		v, gerr := fs.GetFileShare(account, name, r.Header.Get("X-Ms-Snapshot"))
		if gerr != nil {
			return h.writeAPIError(c, gerr)
		}

		return h.writeJSON(c, http.StatusOK, fileShareBody(id, v, expandHas(r, "stats")))
	case http.MethodDelete:
		include := r.URL.Query().Get("$include")
		if include == "" {
			include = fileShareDefaultKV
		}

		deleted, derr := fs.DeleteFileShare(account, name, r.Header.Get("X-Ms-Snapshot"), include)
		if derr != nil {
			return h.writeAPIError(c, derr)
		}

		if !deleted {
			return c.NoContent(http.StatusNoContent)
		}

		return c.NoContent(http.StatusOK)
	}

	return h.putFileShare(c, fs, id, name)
}

func (h *Handler) handleFileShareRestore(c *echo.Context, segs []string) error {
	fs, id, ok, err := h.fileShareScope(c, segs)
	if !ok {
		return err
	}

	body, err := decodeJSONBody(c.Request())
	if err != nil {
		return h.writeAPIError(c, err)
	}

	name, _ := body["deletedShareName"].(string)

	version, _ := body["deletedShareVersion"].(string)
	if name == "" || version == "" {
		return h.writeAPIError(
			c,
			fmt.Errorf("%w: deletedShareName and deletedShareVersion are required", ErrInvalidRequestBody),
		)
	}

	if err = fs.RestoreFileShare(id.LeafName(), segs[fileSharesNameIdx], name, version); err != nil {
		return h.writeAPIError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) putFileShare(c *echo.Context, fs FileShares, id ResourceID, name string) error {
	r := c.Request()

	body, err := decodeJSONBody(r)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	in, err := parseFileShareInput(body)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	v, created, err := fs.PutFileShare(id.LeafName(), name, in, r.Method == http.MethodPut)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}

	return h.writeJSON(c, status, fileShareBody(id, v, false))
}

func expandHas(r *http.Request, want string) bool {
	for v := range strings.SplitSeq(r.URL.Query().Get("$expand"), ",") {
		if strings.EqualFold(strings.TrimSpace(v), want) {
			return true
		}
	}

	return false
}

func int64Prop(m map[string]any, key string) *int64 {
	if f, ok := m[key].(float64); ok {
		n := int64(f)

		return &n
	}

	return nil
}

func parseFileShareInput(body map[string]any) (FileShareInput, error) {
	props, _ := body[fieldProperties].(map[string]any)

	in := FileShareInput{
		Quota: int64Prop(props, "shareQuota"), Iops: int64Prop(props, "provisionedIops"),
		Bandwidth: int64Prop(props, "provisionedBandwidthMibps"),
	}

	in.Tier, _ = props["accessTier"].(string)
	in.Protocols, _ = props["enabledProtocols"].(string)
	in.RootSquash, _ = props["rootSquash"].(string)

	if md, ok := props["metadata"].(map[string]any); ok {
		in.HasMetadata = true
		in.Metadata = make(map[string]string, len(md))

		for k, v := range md {
			s, isStr := v.(string)
			if !isStr {
				return in, fmt.Errorf("%w: metadata values must be strings", ErrInvalidRequestBody)
			}

			in.Metadata[k] = s
		}
	}

	if pb, ok := props["fileSharePaidBursting"].(map[string]any); ok {
		if v, isBool := pb["paidBurstingEnabled"].(bool); isBool {
			in.PaidBursting = &v
		}

		in.BurstIops, in.BurstBandwidth = int64Prop(
			pb,
			"paidBurstingMaxIops",
		), int64Prop(
			pb,
			"paidBurstingMaxBandwidthMibps",
		)
	}

	if ids, ok := props["signedIdentifiers"].([]any); ok {
		in.HasACL = true

		for _, raw := range ids {
			m, _ := raw.(map[string]any)
			ap, _ := m["accessPolicy"].(map[string]any)
			a := FileShareACL{}
			a.ID, _ = m["id"].(string)
			a.Start, _ = ap["startTime"].(string)
			a.Expiry, _ = ap["expiryTime"].(string)
			a.Permission, _ = ap["permission"].(string)
			in.ACL = append(in.ACL, a)
		}
	}

	return in, nil
}

func fileShareTime(t time.Time) string { return t.UTC().Format(fileShareTimeLayer) }

func fileShareBody(id ResourceID, v FileShareView, stats bool) map[string]any {
	props := map[string]any{
		"accessTier": v.Tier, "accessTierChangeTime": fileShareTime(v.TierChanged),
		"enabledProtocols": v.Protocols, "shareQuota": v.Quota, "lastModifiedTime": fileShareTime(v.LastModified),
		"leaseStatus": v.LeaseStatus, "leaseState": v.LeaseState, authFieldMetadata: shareMetadata(v.Metadata),
	}

	if v.LeaseDuration != "" {
		props["leaseDuration"] = v.LeaseDuration
	}

	if v.RootSquash != "" {
		props["rootSquash"] = v.RootSquash
	}

	if stats {
		props["shareUsageBytes"] = v.Usage
	}

	fileShareExtras(props, v)

	if v.Snapshot != "" {
		props["snapshotTime"] = v.Snapshot
	}

	if v.Deleted {
		props["deleted"], props["deletedTime"] = true, fileShareTime(v.DeletedAt)
		props["remainingRetentionDays"], props["version"] = v.RemainingDays, v.Version
	}

	return map[string]any{
		"id":      id.ARMID() + "/" + fileServicesType + "/" + fileServicesDefault + "/" + fileSharesSeg + "/" + v.Name,
		fieldName: v.Name, fieldType: fileShareType, "etag": v.ETag, fieldProperties: props,
	}
}

func shareMetadata(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}

	return m
}

func fileShareExtras(props map[string]any, v FileShareView) {
	if v.Iops != nil {
		props["provisionedIops"] = *v.Iops
	}

	if v.Bandwidth != nil {
		props["provisionedBandwidthMibps"] = *v.Bandwidth
	}

	if v.PaidBursting != nil || v.BurstIops != nil || v.BurstBandwidth != nil {
		pb := map[string]any{}
		if v.PaidBursting != nil {
			pb["paidBurstingEnabled"] = *v.PaidBursting
		}

		if v.BurstIops != nil {
			pb["paidBurstingMaxIops"] = *v.BurstIops
		}

		if v.BurstBandwidth != nil {
			pb["paidBurstingMaxBandwidthMibps"] = *v.BurstBandwidth
		}

		props["fileSharePaidBursting"] = pb
	}

	if len(v.ACL) > 0 {
		ids := make([]map[string]any, 0, len(v.ACL))

		for _, a := range v.ACL {
			policy := map[string]any{}

			for k, v := range map[string]string{apimOpStartTime: a.Start, "expiryTime": a.Expiry, "permission": a.Permission} {
				if v != "" {
					policy[k] = v
				}
			}

			ids = append(ids, map[string]any{"id": a.ID, "accessPolicy": policy})
		}

		props["signedIdentifiers"] = ids
	}
}

func (h *Handler) handleFileShareList(c *echo.Context, segs []string) error {
	fs, id, ok, err := h.fileShareScope(c, segs)
	if !ok {
		return err
	}

	r := c.Request()
	q := r.URL.Query()
	views := fs.ListFileShares(id.LeafName(), expandHas(r, "snapshots"), expandHas(r, "deleted"))

	start := 0

	if tok := q.Get("$skipToken"); tok != "" {
		n, convErr := strconv.Atoi(tok[strings.LastIndex(tok, "/")+1:])
		if convErr != nil || n < 0 {
			return h.writeAPIError(c, fmt.Errorf("%w: invalid $skipToken", ErrInvalidRequestBody))
		}

		start = min(n, len(views))
	}

	end := len(views)
	next := ""

	if raw := q.Get("$maxpagesize"); raw != "" {
		size, convErr := strconv.Atoi(raw)
		if convErr != nil || size < 1 {
			return h.writeAPIError(c, fmt.Errorf("%w: invalid $maxpagesize", ErrInvalidRequestBody))
		}

		if start+size < end {
			end = start + size

			vals := q
			vals.Set("$skipToken", "/"+id.LeafName()+"/"+strconv.Itoa(end))
			next = h.baseURLFor(r) + r.URL.Path + "?" + vals.Encode()
		}
	}

	items := make([]map[string]any, 0, end-start)
	for _, v := range views[start:end] {
		items = append(items, fileShareBody(id, v, false))
	}

	out := map[string]any{fieldValue: items}
	if next != "" {
		out["nextLink"] = next
	}

	return h.writeJSON(c, http.StatusOK, out)
}
