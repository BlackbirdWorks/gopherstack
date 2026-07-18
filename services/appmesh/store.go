package appmesh

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// InMemoryBackend is a thread-safe in-memory App Mesh backend.
//
// meshes, virtualNodes, virtualRouters, routes, virtualSvcs, virtualGWs, and
// gatewayRoutes are store.Table-backed (see store_setup.go for the Phase 3.3
// datalayer conversion): meshes registers directly on its real, globally
// unique Name field; virtualNodes/virtualRouters/virtualSvcs/virtualGWs are
// mesh-nested, so each registers under the composite "meshName|name" key
// with a byMesh Index answering the per-mesh list scans the old
// map[string]map[string]*T nesting used to serve directly; routes and
// gatewayRoutes are nested one level deeper (under a virtual router / virtual
// gateway within a mesh), so each registers under a three-part composite key
// with a byRouter/byGateway Index over its two-part parent key. None of these
// value types needed a hidden field added for the composite key: every
// parent-identifying component (MeshName, VirtualRouterName,
// VirtualGatewayName) was already a real, wire-visible field on the value
// type itself.
//
// tags (map[string]map[string]string, keyed by ARN rather than by a *T with
// its own identity) does not fit store.Table's keyed-collection shape and is
// left as a plain map; see persistence.go for how it round-trips through
// Snapshot/Restore alongside the registered tables.
type InMemoryBackend struct {
	registry               *store.Registry
	meshes                 *store.Table[Mesh]
	virtualNodes           *store.Table[VirtualNode]
	virtualNodesByMesh     *store.Index[VirtualNode]
	virtualRouters         *store.Table[VirtualRouter]
	virtualRoutersByMesh   *store.Index[VirtualRouter]
	routes                 *store.Table[Route]
	routesByRouter         *store.Index[Route]
	virtualSvcs            *store.Table[VirtualService]
	virtualSvcsByMesh      *store.Index[VirtualService]
	virtualGWs             *store.Table[VirtualGateway]
	virtualGWsByMesh       *store.Index[VirtualGateway]
	gatewayRoutes          *store.Table[GatewayRoute]
	gatewayRoutesByGateway *store.Index[GatewayRoute]
	tags                   map[string]map[string]string
	mu                     *lockmetrics.RWMutex
	accountID              string
	region                 string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		accountID: accountID,
		region:    region,
		registry:  store.NewRegistry(),
		tags:      make(map[string]map[string]string),
		mu:        lockmetrics.New("appmesh"),
	}
	registerAllTables(b)

	return b
}

func (b *InMemoryBackend) AccountID() string { return b.accountID }
func (b *InMemoryBackend) Region() string    { return b.region }

func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()
	b.registry.ResetAll()
	b.tags = make(map[string]map[string]string)
}

func newUID() string {
	return fmt.Sprintf("%016x", time.Now().UnixNano())
}

func newMeta(arn, accountID string) ResourceMeta {
	now := time.Now().UTC()

	return ResourceMeta{
		CreatedAt:     now,
		UpdatedAt:     now,
		Arn:           arn,
		UID:           newUID(),
		MeshOwner:     accountID,
		ResourceOwner: accountID,
		Version:       1,
	}
}

const statusActive = "ACTIVE"

// normalizeSpec returns a non-nil JSON object if spec is nil or empty.
func normalizeSpec(spec json.RawMessage) json.RawMessage {
	if len(spec) == 0 {
		return json.RawMessage(`{}`)
	}

	return spec
}

func cloneTags(src map[string]string) map[string]string {
	dst := make(map[string]string, len(src))
	maps.Copy(dst, src)

	return dst
}

// paginateStrings returns a page of items starting after nextToken and a new nextToken.
func paginateStrings(sorted []string, nextToken string, maxResults int32) ([]string, string) {
	start := 0
	if nextToken != "" {
		for i, s := range sorted {
			if strings.Compare(s, nextToken) > 0 {
				start = i

				break
			}
		}
		if start == 0 && (len(sorted) == 0 || strings.Compare(sorted[0], nextToken) <= 0) {
			return nil, ""
		}
	}
	items := sorted[start:]
	if maxResults <= 0 || int(maxResults) >= len(items) {
		return items, ""
	}
	page := items[:maxResults]

	return page, page[len(page)-1]
}

// ErrIs checks whether err wraps sentinel.
func ErrIs(err, sentinel error) bool {
	return errors.Is(err, sentinel)
}
