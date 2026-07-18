package apigateway

import (
	"bytes"
	"compress/gzip"
	"maps"
	"net/http"
	"strconv"
	"strings"
)

// trieCacheEntry pairs a built routing trie with the resource-set version it was built
// from, so the proxy can detect staleness cheaply.
type trieCacheEntry struct {
	trie    *resourcePathTrie
	version uint64
}

// routingTrie returns the cached routing trie for the API, rebuilding it only when the
// backend reports a newer resource-set version.
func (h *Handler) routingTrie(apiID string) (*resourcePathTrie, error) {
	resources, version, err := h.Backend.ResourcesForRouting(apiID)
	if err != nil {
		return nil, err
	}

	if cached, ok := h.trieCache.Load(apiID); ok {
		if entry, isEntry := cached.(*trieCacheEntry); isEntry && entry.version == version {
			return entry.trie, nil
		}
	}

	trie := buildResourceTrie(resources)
	h.trieCache.Store(apiID, &trieCacheEntry{trie: trie, version: version})

	return trie, nil
}

// writeCORSPreflight writes an HTTP 200 response with CORS preflight headers.
func (h *Handler) writeCORSPreflight(w http.ResponseWriter, r *http.Request, cors *CorsConfiguration) {
	h.addCORSHeaders(w, r, cors)
	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusOK)
}

// addCORSHeaders adds Access-Control-* headers to the response based on the CorsConfiguration.
func (h *Handler) addCORSHeaders(w http.ResponseWriter, r *http.Request, cors *CorsConfiguration) {
	origin := r.Header.Get("Origin")
	allowed := corsOriginAllowed(origin, cors.AllowOrigins)
	if allowed != "" {
		w.Header().Set("Access-Control-Allow-Origin", allowed)
	}

	if len(cors.AllowMethods) > 0 {
		w.Header().Set("Access-Control-Allow-Methods", strings.Join(cors.AllowMethods, ", "))
	}

	if len(cors.AllowHeaders) > 0 {
		w.Header().Set("Access-Control-Allow-Headers", strings.Join(cors.AllowHeaders, ", "))
	}

	if len(cors.ExposeHeaders) > 0 {
		w.Header().Set("Access-Control-Expose-Headers", strings.Join(cors.ExposeHeaders, ", "))
	}

	if cors.MaxAge > 0 {
		w.Header().Set("Access-Control-Max-Age", strconv.Itoa(cors.MaxAge))
	}
}

// maybeCompressResponse gzip-compresses body if the request accepts gzip encoding and body
// length meets the minimumCompressionSize threshold (0 means no compression).
// Sets Content-Encoding: gzip on the response header when compression is applied.
func maybeCompressResponse(
	w http.ResponseWriter,
	r *http.Request,
	body []byte,
	minCompressSize int,
) []byte {
	if minCompressSize <= 0 || len(body) < minCompressSize {
		return body
	}

	if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		return body
	}

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(body); err != nil {
		return body
	}

	if err := gz.Close(); err != nil {
		return body
	}

	w.Header().Set("Content-Encoding", "gzip")

	return buf.Bytes()
}

// corsOriginAllowed returns the effective Access-Control-Allow-Origin value for the given origin.
// Returns "*" if origins includes "*", the exact origin if it matches, or empty if not allowed.
func corsOriginAllowed(origin string, allowOrigins []string) string {
	for _, allowed := range allowOrigins {
		if allowed == "*" {
			return "*"
		}

		if allowed == origin {
			return origin
		}
	}

	return ""
}

// findMatchingResource finds a resource whose path pattern matches the request path.
// It supports exact path segments, single-segment path variables ({param}), and
// greedy path variables ({proxy+} or {param+}). The most-specific match wins.
// Returns the matched resource and extracted path parameters, or nil if no match.
// Stage name prefix is stripped from the request path before matching.
func findMatchingResource(resources []Resource, requestPath, stageName string) (*Resource, map[string]string) {
	return matchResourceTrie(buildResourceTrie(resources), requestPath, stageName)
}

// buildResourceTrie constructs a routing trie from the resource set. The proxy builds
// this once per resource-set version and caches it instead of rebuilding per request.
func buildResourceTrie(resources []Resource) *resourcePathTrie {
	trie := newResourcePathTrie()
	for _, resource := range resources {
		trie.insert(resource)
	}

	return trie
}

// matchResourceTrie strips the stage prefix from the request path and matches it
// against a pre-built routing trie.
func matchResourceTrie(trie *resourcePathTrie, requestPath, stageName string) (*Resource, map[string]string) {
	// Strip stage prefix: /{stageName}/... -> /...
	stripped := requestPath
	prefix := "/" + stageName
	if strings.HasPrefix(requestPath, prefix) {
		stripped = requestPath[len(prefix):]
	}

	if stripped == "" {
		stripped = "/"
	}

	return trie.match(stripped)
}

type resourcePathTrie struct {
	root *resourcePathTrieNode
}

type resourcePathTrieNode struct {
	literalChildren map[string]*resourcePathTrieNode
	paramChild      *resourcePathTrieNode
	greedyChild     *resourcePathTrieNode
	resource        *Resource
	paramName       string
	greedyParamName string
}

func newResourcePathTrie() *resourcePathTrie {
	return &resourcePathTrie{
		root: &resourcePathTrieNode{
			literalChildren: make(map[string]*resourcePathTrieNode),
		},
	}
}

func (t *resourcePathTrie) insert(resource Resource) {
	segs := splitPathSegs(resource.Path)
	node := t.root

	for i, seg := range segs {
		isLast := i == len(segs)-1

		if strings.HasPrefix(seg, "{") && strings.HasSuffix(seg, "+}") {
			if !isLast {
				return
			}

			if node.greedyChild == nil {
				node.greedyChild = &resourcePathTrieNode{
					literalChildren: make(map[string]*resourcePathTrieNode),
					greedyParamName: seg[1 : len(seg)-2],
				}
			}

			node = node.greedyChild

			continue
		}

		if strings.HasPrefix(seg, "{") && strings.HasSuffix(seg, "}") {
			if node.paramChild == nil {
				node.paramChild = &resourcePathTrieNode{
					literalChildren: make(map[string]*resourcePathTrieNode),
					paramName:       seg[1 : len(seg)-1],
				}
			}

			node = node.paramChild

			continue
		}

		child, ok := node.literalChildren[seg]
		if !ok {
			child = &resourcePathTrieNode{
				literalChildren: make(map[string]*resourcePathTrieNode),
			}
			node.literalChildren[seg] = child
		}

		node = child
	}

	resourceCopy := resource
	node.resource = &resourceCopy
}

func (t *resourcePathTrie) match(path string) (*Resource, map[string]string) {
	return t.root.match(splitPathSegs(path), 0, map[string]string{})
}

func (n *resourcePathTrieNode) match(
	urlSegs []string,
	index int,
	params map[string]string,
) (*Resource, map[string]string) {
	if index == len(urlSegs) {
		if n.resource == nil {
			return nil, nil
		}

		return n.resource, params
	}

	seg := urlSegs[index]

	if child, ok := n.literalChildren[seg]; ok {
		if res, matchedParams := child.match(urlSegs, index+1, params); res != nil {
			return res, matchedParams
		}
	}

	if n.paramChild != nil {
		nextParams := clonePathParams(params)
		nextParams[n.paramChild.paramName] = seg

		if res, matchedParams := n.paramChild.match(urlSegs, index+1, nextParams); res != nil {
			return res, matchedParams
		}
	}

	if n.greedyChild != nil && n.greedyChild.resource != nil {
		nextParams := clonePathParams(params)
		nextParams[n.greedyChild.greedyParamName] = "/" + strings.Join(urlSegs[index:], "/")

		return n.greedyChild.resource, nextParams
	}

	return nil, nil
}

func clonePathParams(params map[string]string) map[string]string {
	clone := make(map[string]string, len(params)+1)
	maps.Copy(clone, params)

	return clone
}

// splitPathSegs splits a URL path into non-empty segments, ignoring leading and trailing slashes.
func splitPathSegs(path string) []string {
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return []string{}
	}

	return strings.Split(trimmed, "/")
}

// realClientIP extracts the client IP from X-Forwarded-For or RemoteAddr.
func realClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		host, _, _ := strings.Cut(xff, ",")

		return strings.TrimSpace(host)
	}

	host := r.RemoteAddr
	if idx := strings.LastIndexByte(host, ':'); idx >= 0 {
		host = host[:idx]
	}

	return strings.Trim(host, "[]")
}
