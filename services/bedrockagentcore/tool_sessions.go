package bedrockagentcore

import (
	"cmp"
	"context"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

const (
	sessionReady      = "READY"
	sessionTerminated = "TERMINATED"
	defaultSessionSec = 900
	maxSessionSec     = 28800
	sessionIDLen      = 32
)

// label is the path label naming the sandbox in this family's session operations.
func (k ToolKind) label() string {
	if k == ToolBrowser {
		return "browserIdentifier"
	}

	return "codeInterpreterIdentifier"
}

// configMembers are the start-request members read back by the Get operations.
func (k ToolKind) configMembers() []string {
	if k == ToolBrowser {
		return []string{
			"certificates", "enterprisePolicies", "extensions", "filesystemConfigurations", "profileConfiguration",
			"proxyConfiguration", "viewPort",
		}
	}

	return []string{"certificates", "filesystemConfigurations"}
}

func toolID(ref string) string {
	if strings.HasPrefix(ref, "arn:") {
		return ref[strings.LastIndex(ref, "/")+1:]
	}

	return ref
}

func (b *InMemoryBackend) toolOf(sc Scope, d ToolKind, ref string) (ToolInfo, error) {
	c := b.ctl()
	if c == nil {
		return ToolInfo{}, internal("the control plane is not wired")
	}

	t, ok := c.Tool(sc, d, toolID(ref))
	if !ok {
		return ToolInfo{}, notFound("%s %q not found", d, ref)
	}

	return t, nil
}

// effective is the status clients see: READY sessions idle past their timeout read as TERMINATED.
func (b *InMemoryBackend) effective(s *ToolSession) string {
	if s.Status == sessionReady && !b.nowT().Before(s.LastActive.Add(time.Duration(s.TimeoutSec)*time.Second)) {
		return sessionTerminated
	}

	return s.Status
}

func (b *InMemoryBackend) startSession(d ToolKind) restroute.OpFunc {
	return func(ctx context.Context, req *restroute.Request) (any, int, error) {
		in, err := decodeDoc(req)
		if err != nil {
			return nil, 0, err
		}

		sc := b.scopeOf(ctx)

		tool, err := b.toolOf(sc, d, req.Params[d.label()])
		if err != nil {
			return nil, 0, err
		}

		timeout := intOf(in, "sessionTimeoutSeconds")
		if timeout == 0 {
			timeout = defaultSessionSec
		}

		if timeout < 1 || timeout > maxSessionSec {
			return nil, 0, invalid("sessionTimeoutSeconds must be between 1 and %d", maxSessionSec)
		}

		b.mu.Lock("StartSession")
		defer b.mu.Unlock()

		s := b.sessionByToken(sc, d, tool.ID, strOf(in, "clientToken"))
		if s == nil {
			s = b.newSession(sc, d, tool, in, timeout)
			b.sessions.Put(s)
		}

		return doc{d.label(): tool.ID, keySessionID: s.ID, keyCreatedAt: isoTime(s.Created)}, 0, nil
	}
}

func (b *InMemoryBackend) sessionByToken(sc Scope, kind ToolKind, tool, token string) *ToolSession {
	if token == "" {
		return nil
	}

	for _, s := range b.sessions.All() {
		if s.Account == sc.Account && s.Region == sc.Region && s.Kind == kind && s.ToolID == tool &&
			s.Token == token && b.effective(s) == sessionReady {
			return s
		}
	}

	return nil
}

func (b *InMemoryBackend) newSession(sc Scope, d ToolKind, tool ToolInfo, in doc, timeout int) *ToolSession {
	now := b.nowT()
	cfg := doc{}

	for _, k := range d.configMembers() {
		if v, ok := in[k]; ok {
			cfg[k] = v
		}
	}

	return &ToolSession{
		Account: sc.Account, Region: sc.Region, Kind: d, ToolID: tool.ID, ID: awsid.NewN("", sessionIDLen),
		Name: strOf(in, "name"), Status: sessionReady, Token: strOf(in, "clientToken"), TimeoutSec: timeout,
		Created: now, Updated: now, LastActive: now, Config: cfg,
	}
}

func (b *InMemoryBackend) findSession(sc Scope, d ToolKind, ref, id string) (*ToolSession, ToolInfo, error) {
	tool, err := b.toolOf(sc, d, ref)
	if err != nil {
		return nil, tool, err
	}

	s, ok := b.sessions.Get(sessionKey(sc.Account, sc.Region, d, tool.ID, id))
	if !ok {
		return nil, tool, notFound("session %q not found", id)
	}

	return s, tool, nil
}

func (b *InMemoryBackend) stopSession(d ToolKind) restroute.OpFunc {
	return func(ctx context.Context, req *restroute.Request) (any, int, error) {
		sc := b.scopeOf(ctx)

		b.mu.Lock("StopSession")
		s, tool, err := b.findSession(sc, d, req.Params[d.label()], req.Query.Get("sessionId"))

		if err != nil {
			b.mu.Unlock()

			return nil, 0, err
		}

		b.terminateLocked(s)
		out := doc{d.label(): tool.ID, keySessionID: s.ID, "lastUpdatedAt": isoTime(s.Updated)}
		eng := b.engine
		b.mu.Unlock()

		eng.releaseSandbox(s.key())

		return out, 0, nil
	}
}

func (s *ToolSession) key() string { return sessionKey(s.Account, s.Region, s.Kind, s.ToolID, s.ID) }

func (b *InMemoryBackend) terminateLocked(s *ToolSession) {
	if s.Status == sessionTerminated {
		return
	}

	s.Status = sessionTerminated
	s.Updated = b.nowT()
}

func (b *InMemoryBackend) getSession(d ToolKind) restroute.OpFunc {
	return func(ctx context.Context, req *restroute.Request) (any, int, error) {
		b.mu.RLock("GetSession")
		defer b.mu.RUnlock()

		s, tool, err := b.findSession(b.scopeOf(ctx), d, req.Params[d.label()], req.Query.Get("sessionId"))
		if err != nil {
			return nil, 0, err
		}

		out := doc{
			d.label(): tool.ID, keySessionID: s.ID, keyCreatedAt: isoTime(s.Created), keyStatus: b.effective(s),
			"sessionTimeoutSeconds": s.TimeoutSec,
		}

		if s.Name != "" {
			out["name"] = s.Name
		}

		maps.Copy(out, s.Config)

		if d == ToolBrowser {
			out["lastUpdatedAt"] = isoTime(s.Updated)
		}

		return out, 0, nil
	}
}

func (b *InMemoryBackend) listSessionsOf(d ToolKind) restroute.OpFunc {
	return func(ctx context.Context, req *restroute.Request) (any, int, error) {
		in, err := decodeDoc(req)
		if err != nil {
			return nil, 0, err
		}

		b.mu.RLock("ListSessions")
		defer b.mu.RUnlock()

		sc := b.scopeOf(ctx)

		tool, err := b.toolOf(sc, d, req.Params[d.label()])
		if err != nil {
			return nil, 0, err
		}

		all := b.sessionsOf(sc, d, tool.ID, strOf(in, keyStatus))

		pg, err := restroute.Paginate(
			all,
			strOf(in, "nextToken"),
			intOf(in, "maxResults"),
			defaultPageSize,
			maxPageSize,
		)
		if err != nil {
			return nil, 0, err
		}

		items := make([]doc, 0, len(pg.Data))

		for _, s := range pg.Data {
			item := doc{
				d.label(): tool.ID, keySessionID: s.ID, keyCreatedAt: isoTime(s.Created), keyStatus: b.effective(s),
				"lastUpdatedAt": isoTime(s.Updated),
			}
			if s.Name != "" {
				item["name"] = s.Name
			}

			items = append(items, item)
		}

		return pageBody("items", items, pg.Next), 0, nil
	}
}

func (b *InMemoryBackend) sessionsOf(sc Scope, kind ToolKind, tool, status string) []*ToolSession {
	var out []*ToolSession

	for _, s := range b.sessions.All() {
		if s.Account == sc.Account && s.Region == sc.Region && s.Kind == kind && s.ToolID == tool &&
			(status == "" || b.effective(s) == status) {
			out = append(out, s)
		}
	}

	slices.SortFunc(out, func(x, y *ToolSession) int {
		return cmp.Or(x.Created.Compare(y.Created), cmp.Compare(x.ID, y.ID))
	})

	return out
}

// touch records activity on a READY session so its idle timeout restarts; it fails for terminated sessions.
func (b *InMemoryBackend) touch(sc Scope, d ToolKind, ref, id string) (*ToolSession, error) {
	b.mu.Lock("TouchSession")
	defer b.mu.Unlock()

	s, _, err := b.findSession(sc, d, ref, id)
	if err != nil {
		return nil, err
	}

	if b.effective(s) != sessionReady {
		return nil, notFound("session %q is not active", id)
	}

	s.LastActive = b.nowT()

	return s, nil
}

// expireSessions terminates sessions idle past their timeout and returns their sandbox keys.
func (b *InMemoryBackend) expireSessions() []string {
	b.mu.Lock("ExpireSessions")
	defer b.mu.Unlock()

	var keys []string

	for _, s := range b.sessions.All() {
		if s.Status == sessionReady && b.effective(s) == sessionTerminated {
			b.terminateLocked(s)
			keys = append(keys, s.key())
		}
	}

	return keys
}
