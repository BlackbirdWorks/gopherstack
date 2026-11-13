package dashboard

import (
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"regexp"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	idbackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
)

const (
	ociFSPrefix      = ociProxyPrefix + "/filestorage"
	ociFSAPIPrefix   = "/20171215"
	ociFSMaxBody     = 1 << 16
	ociFSMaxName     = 255
	ociFSMaxPath     = 1024
	ociFSMaxOptions  = 32
	ociFSItemSegs    = 2
	ociFSKeyComp     = "compartmentId"
	ociFSKeyAD       = "availabilityDomain"
	ociFSKeyState    = "lifecycleState"
	ociFSKeyName     = "displayName"
	ociFSKeyLimit    = "limit"
	ociFSKeyPage     = "page"
	ociFSKeySortBy   = "sortBy"
	ociFSKeySortOrd  = "sortOrder"
	ociFSKeyFS       = "fileSystemId"
	ociFSKeyExportSt = "exportSetId"
	ociFSFileSystems = "fileSystems"
	ociFSMountTarget = "mountTargets"
	ociFSExportSets  = "exportSets"
	ociFSExports     = "exports"
	ociFSSnapshots   = "snapshots"
	ociFSPolicies    = "filesystemSnapshotPolicies"
	ociFSReplicas    = "replications"
	ociFSAccessRW    = "READ_WRITE"
	ociFSAccessRO    = "READ_ONLY"
	ociFSKeySubnet   = "subnetId"
	ociFSKeyAccess   = "access"
)

var (
	ociFSIDRe    = regexp.MustCompile(`^[A-Za-z0-9._-]{1,255}$`)
	ociFSSnapRe  = regexp.MustCompile(`^[A-Za-z0-9._-]{1,255}$`)
	ociFSLabelRe = regexp.MustCompile(`^[A-Za-z]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?$`)
)

type ociFSColl struct {
	create func(c *echo.Context) ([]byte, bool)
	keys   []string
	remove bool
}

// ociFSColls lists the collections the page reaches. A collection with a create builder accepts POST; remove allows
// DELETE of one item; every collection allows list and get.
//
//nolint:gochecknoglobals // immutable allow-list
var ociFSColls = map[string]ociFSColl{
	ociFSFileSystems: {
		create: ociFSFileSystemBody, remove: true,
		keys: []string{
			ociFSKeyComp, ociFSKeyAD, ociFSKeyState, ociFSKeyName, "parentFileSystemId", "sourceSnapshotId",
			ociFSKeyLimit, ociFSKeyPage, ociFSKeySortBy, ociFSKeySortOrd,
		},
	},
	ociFSMountTarget: {
		create: ociFSMountTargetBody, remove: true,
		keys: []string{
			ociFSKeyComp, ociFSKeyAD, ociFSKeyState, ociFSKeyName, ociFSKeyExportSt,
			ociFSKeyLimit, ociFSKeyPage, ociFSKeySortBy, ociFSKeySortOrd,
		},
	},
	ociFSExportSets: {
		keys: []string{
			ociFSKeyComp, ociFSKeyAD, ociFSKeyState, ociFSKeyName, ociFSKeyLimit, ociFSKeyPage,
			ociFSKeySortBy, ociFSKeySortOrd,
		},
	},
	ociFSExports: {
		create: ociFSExportBody, remove: true,
		keys: []string{
			ociFSKeyComp, ociFSKeyExportSt, ociFSKeyFS, ociFSKeyState, ociFSKeyLimit, ociFSKeyPage,
			ociFSKeySortBy, ociFSKeySortOrd,
		},
	},
	ociFSSnapshots: {
		create: ociFSSnapshotBody, remove: true,
		keys: []string{ociFSKeyFS, ociFSKeyState, ociFSKeyLimit, ociFSKeyPage, ociFSKeySortBy, ociFSKeySortOrd},
	},
	ociFSPolicies: {
		keys: []string{
			ociFSKeyComp, ociFSKeyAD, ociFSKeyState, ociFSKeyName, ociFSKeyLimit, ociFSKeyPage,
			ociFSKeySortBy, ociFSKeySortOrd,
		},
	},
	ociFSReplicas: {
		keys: []string{
			ociFSKeyComp, ociFSKeyAD, ociFSKeyFS, ociFSKeyState, ociFSKeyName, ociFSKeyLimit, ociFSKeyPage,
			ociFSKeySortBy, ociFSKeySortOrd,
		},
	},
}

// registerOCIFileStorageRoutes serves the File Storage page by dispatching in-process to the emulator's control
// plane without request signing. Only the file system, mount target, export, snapshot, policy and replication
// operations the page uses are reachable, each write rebuilt from fixed fields.
func (h *DashboardHandler) registerOCIFileStorageRoutes() {
	var inner *echo.Echo

	ops := h.config.OCIFileStorageOps
	if ops != nil {
		inner = echo.New()
		inner.Any("/*", ops.Trusted())
	}

	guard := func(next echo.HandlerFunc) echo.HandlerFunc {
		return ociGuard(inner != nil, "OCI File Storage", next)
	}

	h.SubRouter.Add(http.MethodGet, ociFSPrefix+"/compartments", guard(func(c *echo.Context) error {
		comps := ops.Backend.Compartments()
		if !slices.Contains(comps, idbackend.TenancyID) {
			comps = slices.Insert(comps, 0, idbackend.TenancyID)
		}

		return c.JSON(http.StatusOK, map[string]any{ociCompartmentsKey: comps})
	}))

	h.SubRouter.Add(http.MethodGet, ociFSPrefix+"/nfs", guard(func(c *echo.Context) error {
		return c.JSON(http.StatusOK, map[string]any{
			ociDNSKeyEnabled: ops.Backend.NFSEnabled(), "endpoints": ops.Backend.NFSEndpoints(),
		})
	}))

	h.SubRouter.Add(http.MethodGet, ociFSPrefix+"/availabilitydomains", guard(h.ociCIAvailabilityDomains))
	h.SubRouter.Add(http.MethodGet, ociFSPrefix+"/subnets", guard(h.ociCISubnets))
	h.SubRouter.Add(http.MethodGet, ociFSPrefix+"/privateips/:id", guard(h.ociFSPrivateIP))

	serve := guard(func(c *echo.Context) error { return ociFSServe(c, inner) })

	for _, m := range []string{http.MethodGet, http.MethodPost, http.MethodDelete} {
		h.SubRouter.Add(m, ociFSPrefix+"/api/*", serve)
	}
}

// ociFSPrivateIP reads the address of a mount target's private IP from OCI Core, reduced to what the page shows.
func (h *DashboardHandler) ociFSPrivateIP(c *echo.Context) error {
	id := c.Param("id")
	if !ociFSIDRe.MatchString(id) {
		return ociInvalid(c, "invalid private IP id")
	}

	out := map[string]any{}

	if ops := h.config.OCICoreOps; ops != nil {
		inner := echo.New()
		inner.Any("/*", ops.Trusted())

		r := c.Request()
		fwd := r.Clone(r.Context())
		fwd.URL.Path, fwd.URL.RawPath, fwd.URL.RawQuery = ociCICoreAPI+"/privateIps/"+id, "", ""
		fwd.RequestURI, fwd.Method, fwd.Body, fwd.ContentLength = "", http.MethodGet, http.NoBody, 0
		fwd.Header = http.Header{}

		rec := httptest.NewRecorder()
		inner.ServeHTTP(rec, fwd)

		var ip map[string]any
		if rec.Code == http.StatusOK && json.Unmarshal(rec.Body.Bytes(), &ip, jsonx.V1()) == nil {
			out = map[string]any{
				"ipAddress": ip["ipAddress"], "hostnameLabel": ip["hostnameLabel"], ociFSKeySubnet: ip[ociFSKeySubnet],
			}
		}
	}

	return c.JSON(http.StatusOK, out)
}

// ociFSServe forwards one allow-listed control-plane call.
func ociFSServe(c *echo.Context, inner *echo.Echo) error {
	r := c.Request()

	parts, ok := ociSplitPath(strings.TrimPrefix(r.URL.EscapedPath(), ociFSPrefix+"/api"))
	if !ok {
		return ociInvalid(c, "unsupported OCI File Storage proxy request")
	}

	coll, build, ok := ociFSResolve(r.Method, parts)
	if !ok {
		return ociInvalid(c, "unsupported OCI File Storage proxy request")
	}

	var body []byte

	if build != nil {
		if body, ok = build(c); !ok {
			return ociInvalid(c, "invalid OCI File Storage request")
		}
	}

	query := ociArtQuery(r.URL.Query(), coll.keys)
	r.Header = http.Header{}
	r.URL.RawQuery = ""

	if r.Method == http.MethodGet && len(parts) == 1 {
		r.URL.RawQuery = query
	}

	rec := ociArtForward(c, inner, ociFSAPIPrefix+"/"+strings.Join(parts, "/"), body)

	return ociRelay(c, rec, "opc-next-page", "etag", "opc-request-id")
}

// ociFSResolve maps a proxy request to its collection and body builder; false means outside the allow-list.
func ociFSResolve(method string, parts []string) (ociFSColl, func(*echo.Context) ([]byte, bool), bool) {
	if len(parts) > ociFSItemSegs || (len(parts) == ociFSItemSegs && !ociFSIDRe.MatchString(unescapedPath(parts[1]))) {
		return ociFSColl{}, nil, false
	}

	coll, known := ociFSColls[parts[0]]
	if !known {
		return ociFSColl{}, nil, false
	}

	item := len(parts) == ociFSItemSegs

	switch {
	case method == http.MethodGet:
		return coll, nil, true
	case method == http.MethodPost && !item && coll.create != nil:
		return coll, coll.create, true
	case method == http.MethodDelete && item && coll.remove:
		return coll, nil, true
	}

	return ociFSColl{}, nil, false
}

func ociFSName(s string) bool { return s != "" && len(s) <= ociFSMaxName }

func ociFSOptName(s string) bool { return len(s) <= ociFSMaxName }

func ociFSFileSystemBody(c *echo.Context) ([]byte, bool) {
	var in struct {
		DisplayName string `json:"displayName"`
		Compartment string `json:"compartmentId"`
		AD          string `json:"availabilityDomain"`
		Source      string `json:"sourceSnapshotId"`
	}

	if !ociDecodeBodyN(c, &in, ociFSMaxBody) || !ociFSName(in.Compartment) || !ociFSName(in.AD) ||
		!ociFSOptName(in.DisplayName) || (in.Source != "" && !ociFSIDRe.MatchString(in.Source)) {
		return nil, false
	}

	out := map[string]any{ociFSKeyComp: in.Compartment, ociFSKeyAD: in.AD}

	if in.DisplayName != "" {
		out[ociFSKeyName] = in.DisplayName
	}

	if in.Source != "" {
		out["sourceSnapshotId"] = in.Source
	}

	return ociArtMarshal(out)
}

func ociFSMountTargetBody(c *echo.Context) ([]byte, bool) {
	var in struct {
		DisplayName string `json:"displayName"`
		Compartment string `json:"compartmentId"`
		AD          string `json:"availabilityDomain"`
		SubnetID    string `json:"subnetId"`
		Hostname    string `json:"hostnameLabel"`
		IPAddress   string `json:"ipAddress"`
	}

	if !ociDecodeBodyN(c, &in, ociFSMaxBody) || !ociFSName(in.Compartment) || !ociFSName(in.AD) ||
		!ociFSIDRe.MatchString(in.SubnetID) || !ociFSOptName(in.DisplayName) ||
		(in.Hostname != "" && !ociFSLabelRe.MatchString(in.Hostname)) {
		return nil, false
	}

	out := map[string]any{ociFSKeyComp: in.Compartment, ociFSKeyAD: in.AD, ociFSKeySubnet: in.SubnetID}

	if in.DisplayName != "" {
		out[ociFSKeyName] = in.DisplayName
	}

	if in.Hostname != "" {
		out["hostnameLabel"] = in.Hostname
	}

	if in.IPAddress != "" {
		if _, err := netip.ParseAddr(in.IPAddress); err != nil {
			return nil, false
		}

		out["ipAddress"] = in.IPAddress
	}

	return ociArtMarshal(out)
}

type ociFSOption struct {
	Privileged *bool  `json:"requirePrivilegedSourcePort"`
	Source     string `json:"source"`
	Access     string `json:"access"`
	Squash     string `json:"identitySquash"`
}

func (o ociFSOption) build() (map[string]any, bool) {
	if !ociFSSource(o.Source) || !slices.Contains([]string{ociFSAccessRW, ociFSAccessRO}, o.Access) ||
		!slices.Contains([]string{"NONE", "ROOT", "ALL"}, o.Squash) {
		return nil, false
	}

	out := map[string]any{"source": o.Source, ociFSKeyAccess: o.Access, "identitySquash": o.Squash}

	if o.Privileged != nil {
		out["requirePrivilegedSourcePort"] = *o.Privileged
	}

	return out, true
}

// ociFSSource accepts one IPv4 or IPv6 address or CIDR block.
func ociFSSource(s string) bool {
	if _, err := netip.ParsePrefix(s); err == nil {
		return true
	}

	_, err := netip.ParseAddr(s)

	return err == nil
}

func ociFSExportBody(c *echo.Context) ([]byte, bool) {
	var in struct {
		ExportSet  string        `json:"exportSetId"`
		FileSystem string        `json:"fileSystemId"`
		Path       string        `json:"path"`
		Options    []ociFSOption `json:"exportOptions"`
	}

	if !ociDecodeBodyN(c, &in, ociFSMaxBody) || !ociFSIDRe.MatchString(in.ExportSet) ||
		!ociFSIDRe.MatchString(in.FileSystem) || !ociFSExportPath(in.Path) || len(in.Options) > ociFSMaxOptions {
		return nil, false
	}

	opts := make([]map[string]any, 0, len(in.Options))

	for _, o := range in.Options {
		built, ok := o.build()
		if !ok {
			return nil, false
		}

		opts = append(opts, built)
	}

	return ociArtMarshal(map[string]any{
		ociFSKeyExportSt: in.ExportSet, ociFSKeyFS: in.FileSystem, "path": in.Path, "exportOptions": opts,
	})
}

func ociFSExportPath(p string) bool {
	return len(p) > 0 && len(p) <= ociFSMaxPath && strings.HasPrefix(p, "/") &&
		!strings.ContainsFunc(p, func(r rune) bool { return r < ' ' })
}

func ociFSSnapshotBody(c *echo.Context) ([]byte, bool) {
	var in struct {
		FileSystem string `json:"fileSystemId"`
		Name       string `json:"name"`
	}

	if !ociDecodeBodyN(c, &in, ociFSMaxBody) || !ociFSIDRe.MatchString(in.FileSystem) ||
		!ociFSSnapRe.MatchString(in.Name) {
		return nil, false
	}

	return ociArtMarshal(map[string]any{ociFSKeyFS: in.FileSystem, netPxNameKey: in.Name})
}

// registerStorageAndWorkflowRoutes registers the File Storage and Logic Apps pages.
func (h *DashboardHandler) registerStorageAndWorkflowRoutes() {
	h.registerOCIFileStorageRoutes()
	h.registerAzureLogicRoutes()
	h.registerSearchAndWebRoutes()
}
