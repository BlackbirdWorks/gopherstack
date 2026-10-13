package ec2

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	imdsAuthHeader     = "X-Aws-Ec2-Metadata-Token"
	imdsLifetimeHeader = "X-Aws-Ec2-Metadata-Token-Ttl-Seconds"
	imdsAPIPath        = "/latest/api/token"
	imdsMinTTLSeconds  = 1
	imdsMaxTTLSeconds  = 21600
	imdsTokenBytes     = 40
	imdsCredRefreshAt  = 5 * time.Minute
	imdsContentType    = "text/plain"
	imdsServerHeader   = "EC2ws"
	imdsMaxListEntries = 4096
)

var (
	errIMDSNoProfile = errors.New("instance has no IAM role")

	imdsVersionPattern = regexp.MustCompile(`^(latest|\d{4}-\d{2}-\d{2})$`)
)

// RoleCredentials are temporary credentials issued for an instance profile's role.
type RoleCredentials struct {
	Expires         time.Time
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

// IMDSProfile is an IAM instance profile resolved to its single role.
type IMDSProfile struct {
	ARN      string
	ID       string
	RoleName string
	RoleARN  string
}

// IMDSPlatform resolves IAM instance profiles and mints role credentials for IMDS.
type IMDSPlatform interface {
	InstanceProfileRole(profileName string) (IMDSProfile, error)
	IssueInstanceCredentials(roleArn, sessionName string) (RoleCredentials, error)
}

// IMDSListener exposes one instance's metadata service at a network endpoint reachable from the
// instance container, and tears it down when the instance goes away.
type IMDSListener interface {
	Listen(instanceID string, h http.Handler) (endpoint string, err error)
	Close(instanceID string)
}

type imdsCredEntry struct {
	creds   RoleCredentials
	roleARN string
}

// imdsService is shared by a home handler and its region siblings.
type imdsService struct {
	platform  IMDSPlatform
	listener  IMDSListener
	tokens    map[string]map[string]time.Time
	linkLocal *linkLocalState
	creds     map[string]imdsCredEntry
	mu        *lockmetrics.RWMutex
}

func newIMDSService() *imdsService {
	return &imdsService{
		mu:        lockmetrics.New("ec2.imds"),
		tokens:    map[string]map[string]time.Time{},
		creds:     map[string]imdsCredEntry{},
		linkLocal: &linkLocalState{},
	}
}

// SetIMDSPlatform installs the IAM/STS bridge IMDS uses for iam/* paths.
func (h *Handler) SetIMDSPlatform(p IMDSPlatform) {
	h.imds.mu.Lock("SetIMDSPlatform")
	defer h.imds.mu.Unlock()
	h.imds.platform = p
}

// SetIMDSListener installs the per-instance endpoint provider used for compute-backed instances.
func (h *Handler) SetIMDSListener(l IMDSListener) {
	h.imds.mu.Lock("SetIMDSListener")
	defer h.imds.mu.Unlock()
	h.imds.listener = l
}

func (s *imdsService) listenerOf() IMDSListener {
	s.mu.RLock("listenerOf")
	defer s.mu.RUnlock()

	return s.listener
}

func (s *imdsService) platformOf() IMDSPlatform {
	s.mu.RLock("platformOf")
	defer s.mu.RUnlock()

	return s.platform
}

func (s *imdsService) forget(instanceID string) {
	s.mu.Lock("forget")
	delete(s.tokens, instanceID)
	delete(s.creds, instanceID)
	s.mu.Unlock()

	if l := s.listenerOf(); l != nil {
		l.Close(instanceID)
	}
}

// issueToken mints a token for instanceID valid for ttl and drops that instance's expired tokens.
func (s *imdsService) issueToken(instanceID string, ttl time.Duration) (string, error) {
	raw := make([]byte, imdsTokenBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}

	token := base64.URLEncoding.EncodeToString(raw)
	now := time.Now()

	s.mu.Lock("issueToken")
	defer s.mu.Unlock()

	live := s.tokens[instanceID]
	if live == nil {
		live = map[string]time.Time{}
		s.tokens[instanceID] = live
	}

	for t, exp := range live {
		if !exp.After(now) {
			delete(live, t)
		}
	}

	live[token] = now.Add(ttl)

	return token, nil
}

// authorized reports whether a request carrying token (possibly empty) may read metadata.
func (s *imdsService) authorized(instanceID, token string, required bool) bool {
	if token == "" {
		return !required
	}

	return s.tokenValid(instanceID, token)
}

func (s *imdsService) tokenValid(instanceID, token string) bool {
	s.mu.RLock("tokenValid")
	defer s.mu.RUnlock()

	exp, ok := s.tokens[instanceID][token]

	return ok && exp.After(time.Now())
}

// credentialsFor returns cached role credentials, reissuing them within imdsCredRefreshAt of expiry.
func (s *imdsService) credentialsFor(instanceID string, prof IMDSProfile) (RoleCredentials, error) {
	s.mu.RLock("credentialsFor")
	cached, ok := s.creds[instanceID]
	s.mu.RUnlock()

	if ok && cached.roleARN == prof.RoleARN && time.Until(cached.creds.Expires) > imdsCredRefreshAt {
		return cached.creds, nil
	}

	p := s.platformOf()
	if p == nil {
		return RoleCredentials{}, errIMDSNoProfile
	}

	creds, err := p.IssueInstanceCredentials(prof.RoleARN, instanceID)
	if err != nil {
		return RoleCredentials{}, err
	}

	s.mu.Lock("credentialsFor")
	s.creds[instanceID] = imdsCredEntry{creds: creds, roleARN: prof.RoleARN}
	s.mu.Unlock()

	return creds, nil
}

func (h *Handler) imdsBackends() []*InMemoryBackend {
	if mem, ok := h.Backend.(*InMemoryBackend); ok && mem.allBackends != nil {
		return mem.allBackends()
	}

	if mem, ok := h.Backend.(*InMemoryBackend); ok {
		return []*InMemoryBackend{mem}
	}

	return nil
}

func (h *Handler) imdsViewOf(instanceID string) (*imdsView, bool) {
	for _, bk := range h.imdsBackends() {
		if v, ok := bk.imdsViewOf(instanceID); ok {
			return v, true
		}
	}

	return nil, false
}

// IMDSHandler serves instanceID's metadata service rooted at "/".
func (h *Handler) IMDSHandler(instanceID string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.serveIMDS(w, r, instanceID)
	})
}

// ServeIMDS serves the metadata service under /_gopherstack/ec2/imds/:instance/*.
func (h *Handler) ServeIMDS(c *echo.Context) error {
	r := c.Request().Clone(c.Request().Context())
	r.URL.Path = "/" + c.Param("*")
	r.URL.RawPath = ""

	if c.Param("instance") == imdsLinkLocalInstance {
		h.serveLinkLocalIMDS(c.Response(), r)

		return nil
	}

	h.serveIMDS(c.Response(), r, c.Param("instance"))

	return nil
}

func imdsReply(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", imdsContentType)
	w.Header().Set("Server", imdsServerHeader)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body) //nolint:gosec // text/plain with nosniff; not rendered as HTML
}

func (h *Handler) serveIMDS(w http.ResponseWriter, r *http.Request, instanceID string) {
	view, ok := h.imdsViewOf(instanceID)
	if !ok {
		h.imds.forget(instanceID)
		imdsReply(w, http.StatusNotFound, "instance not found")

		return
	}

	opts := metadataOptionsOf(&view.Instance)
	if opts.HTTPEndpoint == imdsDisabled {
		imdsReply(w, http.StatusForbidden, "Forbidden")

		return
	}

	path := "/" + strings.Trim(r.URL.Path, "/")

	switch r.Method {
	case http.MethodPut:
		h.serveIMDSToken(w, r, instanceID, path)
	case http.MethodGet, http.MethodHead:
		h.serveIMDSGet(w, r, view, opts, path)
	default:
		imdsReply(w, http.StatusMethodNotAllowed, "Method Not Allowed")
	}
}

func (h *Handler) serveIMDSToken(w http.ResponseWriter, r *http.Request, instanceID, path string) {
	if path != imdsAPIPath {
		imdsReply(w, http.StatusMethodNotAllowed, "Method Not Allowed")

		return
	}

	if r.Header.Get("X-Forwarded-For") != "" {
		imdsReply(w, http.StatusForbidden, "Forbidden")

		return
	}

	ttl, err := strconv.Atoi(r.Header.Get(imdsLifetimeHeader))
	if err != nil || ttl < imdsMinTTLSeconds || ttl > imdsMaxTTLSeconds {
		imdsReply(w, http.StatusBadRequest, "Bad Request")

		return
	}

	token, err := h.imds.issueToken(instanceID, time.Duration(ttl)*time.Second)
	if err != nil {
		imdsReply(w, http.StatusInternalServerError, "Internal Server Error")

		return
	}

	w.Header().Set(imdsLifetimeHeader, strconv.Itoa(ttl))
	imdsReply(w, http.StatusOK, token)
}

func (h *Handler) serveIMDSGet(w http.ResponseWriter, r *http.Request, view *imdsView, opts IMDSOptions, path string) {
	token := r.Header.Get(imdsAuthHeader)
	if !h.imds.authorized(view.Instance.ID, token, opts.HTTPTokens == imdsTokensRequired) {
		imdsReply(w, http.StatusUnauthorized, "Unauthorized")

		return
	}

	rel, ok := imdsRelativePath(path)
	if !ok {
		imdsReply(w, http.StatusNotFound, "Not Found")

		return
	}

	body, status := h.resolveIMDS(r.Context(), view, opts, rel)
	imdsReply(w, status, body)
}

// imdsRelativePath strips the version segment: "/latest/meta-data/x" becomes "meta-data/x".
func imdsRelativePath(path string) (string, bool) {
	trimmed := strings.TrimPrefix(path, "/")
	version, rel, _ := strings.Cut(trimmed, "/")

	if trimmed == "" {
		return "", true
	}

	return rel, imdsVersionPattern.MatchString(version)
}

func (h *Handler) resolveIMDS(ctx context.Context, view *imdsView, opts IMDSOptions, rel string) (string, int) {
	tree := h.buildIMDSTree(ctx, view, opts)

	rel = strings.Trim(rel, "/")
	if leaf, ok := tree.leaves[rel]; ok {
		body, err := leaf()
		if err != nil {
			return "Internal Server Error", http.StatusInternalServerError
		}

		return body, http.StatusOK
	}

	if listing, ok := tree.list(rel); ok {
		return listing, http.StatusOK
	}

	return "Not Found", http.StatusNotFound
}
