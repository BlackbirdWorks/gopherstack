package amp

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

const (
	silenceRetention = 120 * time.Hour
	maxSilences      = 10000
	maxSilenceSize   = 4096

	silencePending = "pending"
	silenceActive  = "active"
	silenceExpired = "expired"
)

// SilenceMatcher is one label matcher of a silence.
type SilenceMatcher struct {
	Name    string `json:"name"`
	Value   string `json:"value"`
	IsRegex bool   `json:"isRegex"`
	IsEqual bool   `json:"isEqual"`
}

// Silence mutes the alerts its matchers select between StartsAt and EndsAt.
type Silence struct {
	StartsAt    time.Time        `json:"startsAt"`
	EndsAt      time.Time        `json:"endsAt"`
	UpdatedAt   time.Time        `json:"updatedAt"`
	ID          string           `json:"id"`
	WorkspaceID string           `json:"workspaceId"`
	CreatedBy   string           `json:"createdBy"`
	Comment     string           `json:"comment"`
	Matchers    []SilenceMatcher `json:"matchers"`
}

func silenceKey(ws, id string) string { return ws + "/" + id }

func silenceKeyOf(s *Silence) string     { return silenceKey(s.WorkspaceID, s.ID) }
func silenceWorkspace(s *Silence) string { return s.WorkspaceID }
func (s *Silence) state(now time.Time) string {
	switch {
	case now.Before(s.StartsAt):
		return silencePending
	case now.Before(s.EndsAt):
		return silenceActive
	}

	return silenceExpired
}

func (m SilenceMatcher) compile() (*promql.Matcher, error) {
	t := promql.MatchEqual

	switch {
	case m.IsRegex && m.IsEqual:
		t = promql.MatchRegexp
	case m.IsRegex:
		t = promql.MatchNotRegexp
	case !m.IsEqual:
		t = promql.MatchNotEqual
	}

	return promql.NewMatcher(t, m.Name, m.Value)
}

type compiledSilence struct {
	starts   time.Time
	ends     time.Time
	id       string
	matchers []*promql.Matcher
}

func compileSilence(s *Silence) (compiledSilence, error) {
	c := compiledSilence{id: s.ID, starts: s.StartsAt, ends: s.EndsAt}

	for _, m := range s.Matchers {
		cm, err := m.compile()
		if err != nil {
			return compiledSilence{}, err
		}

		c.matchers = append(c.matchers, cm)
	}

	return c, nil
}

func (m *alertManager) setSilences(list []*Silence) {
	compiled := make([]compiledSilence, 0, len(list))

	for _, s := range list {
		if c, err := compileSilence(s); err == nil {
			compiled = append(compiled, c)
		}
	}

	m.mu.Lock("setSilences")
	defer m.mu.Unlock()

	m.silences = compiled
}

func (m *alertManager) silencedByLocked(l promql.Labels, now time.Time) []string {
	out := []string{}

	for _, s := range m.silences {
		if !now.Before(s.starts) && now.Before(s.ends) && promql.MatchesAll(l, s.matchers) {
			out = append(out, s.id)
		}
	}

	slices.Sort(out)

	return out
}

type silenceInput struct {
	StartsAt  *time.Time       `json:"startsAt"`
	EndsAt    *time.Time       `json:"endsAt"`
	ID        string           `json:"id"`
	CreatedBy string           `json:"createdBy"`
	Comment   string           `json:"comment"`
	Matchers  []silenceMatcher `json:"matchers"`
}

type silenceMatcher struct {
	IsEqual *bool  `json:"isEqual"`
	Name    string `json:"name"`
	Value   string `json:"value"`
	IsRegex bool   `json:"isRegex"`
}

func (in silenceInput) checkEnvelope(now time.Time) error {
	switch {
	case len(in.Matchers) == 0:
		return invalid("at least one matcher required")
	case in.StartsAt == nil || in.EndsAt == nil:
		return invalid("startsAt and endsAt are required")
	case in.CreatedBy == "" || in.Comment == "":
		return invalid("createdBy and comment are required")
	case len(in.CreatedBy) > maxSilenceSize || len(in.Comment) > maxSilenceSize:
		return invalid("createdBy or comment is too long")
	case in.EndsAt.Before(*in.StartsAt):
		return invalid("end time must not be before start time")
	case in.EndsAt.Before(now):
		return invalid("end time can't be in the past")
	}

	return nil
}

func (in silenceInput) buildMatchers() ([]SilenceMatcher, error) {
	ms := make([]SilenceMatcher, 0, len(in.Matchers))

	for _, m := range in.Matchers {
		sm := SilenceMatcher{Name: m.Name, Value: m.Value, IsRegex: m.IsRegex, IsEqual: m.IsEqual == nil || *m.IsEqual}
		if m.Name == "" || len(m.Name) > maxSilenceSize || len(m.Value) > maxSilenceSize {
			return nil, invalid("invalid matcher %q", m.Name)
		}

		if _, err := sm.compile(); err != nil {
			return nil, invalid("invalid matcher %q: %v", m.Name, err)
		}

		ms = append(ms, sm)
	}

	return ms, nil
}

func (in silenceInput) validate(now time.Time) ([]SilenceMatcher, time.Time, time.Time, error) {
	var zero time.Time

	if err := in.checkEnvelope(now); err != nil {
		return nil, zero, zero, err
	}

	ms, err := in.buildMatchers()
	if err != nil {
		return nil, zero, zero, err
	}

	starts := *in.StartsAt
	if starts.Before(now) {
		starts = now
	}

	return ms, starts, *in.EndsAt, nil
}

func (b *InMemoryBackend) workspaceSilencesLocked(wsID string) []*Silence {
	return slices.Clone(b.silencesByWS.Get(wsID))
}

func (b *InMemoryBackend) syncSilencesLocked(wsID string) {
	if m := b.ams[wsID]; m != nil {
		m.setSilences(b.workspaceSilencesLocked(wsID))
	}
}

func (b *InMemoryBackend) silenceManagerLocked(ctx context.Context, wsID string) error {
	_, err := b.managerLocked(b.scopeOf(ctx), wsID)

	return err
}

// putSilence creates a silence, or replaces the unexpired one named by in.ID, and returns its id.
func (b *InMemoryBackend) putSilence(ctx context.Context, wsID string, in silenceInput) (string, error) {
	b.mu.Lock("PutAlertManagerSilence")
	defer b.mu.Unlock()

	if err := b.silenceManagerLocked(ctx, wsID); err != nil {
		return "", err
	}

	now := b.now()

	ms, starts, ends, err := in.validate(now)
	if err != nil {
		return "", err
	}

	id := uuid.NewString()

	if in.ID != "" {
		old, ok := b.silences.Get(silenceKey(wsID, in.ID))
		if !ok {
			return "", notFound(resSilence, in.ID, "silence %s not found", in.ID)
		}

		if old.state(now) != silenceExpired {
			id = old.ID
		}
	} else if len(b.silencesByWS.Get(wsID)) >= maxSilences {
		return "", quotaExceeded(resSilence, "too many silences")
	}

	b.silences.Put(&Silence{
		ID: id, WorkspaceID: wsID, StartsAt: starts, EndsAt: ends, UpdatedAt: now,
		CreatedBy: in.CreatedBy, Comment: in.Comment, Matchers: ms,
	})
	b.syncSilencesLocked(wsID)

	return id, nil
}

// expireSilence ends a silence now, or before it starts; it stays listed until the retention window passes.
func (b *InMemoryBackend) expireSilence(ctx context.Context, wsID, id string) error {
	b.mu.Lock("DeleteAlertManagerSilence")
	defer b.mu.Unlock()

	if err := b.silenceManagerLocked(ctx, wsID); err != nil {
		return err
	}

	s, ok := b.silences.Get(silenceKey(wsID, id))
	if !ok {
		return notFound(resSilence, id, "silence %s not found", id)
	}

	now := b.now()
	if s.state(now) == silenceExpired {
		return invalid("silence %s already expired", id)
	}

	if s.StartsAt.After(now) {
		s.StartsAt = now
	}

	s.EndsAt, s.UpdatedAt = now, now
	b.silences.Put(s)
	b.syncSilencesLocked(wsID)

	return nil
}

type silenceJSON struct {
	Status    map[string]string `json:"status"`
	ID        string            `json:"id"`
	UpdatedAt string            `json:"updatedAt"`
	StartsAt  string            `json:"startsAt"`
	EndsAt    string            `json:"endsAt"`
	CreatedBy string            `json:"createdBy"`
	Comment   string            `json:"comment"`
	Matchers  []SilenceMatcher  `json:"matchers"`
}

func silenceView(s *Silence, now time.Time) silenceJSON {
	return silenceJSON{
		ID: s.ID, Status: map[string]string{"state": s.state(now)}, UpdatedAt: timeJSON(s.UpdatedAt),
		StartsAt: timeJSON(s.StartsAt), EndsAt: timeJSON(s.EndsAt), CreatedBy: s.CreatedBy,
		Comment: s.Comment, Matchers: slices.Clone(s.Matchers),
	}
}

func silenceRank(state string) int {
	return slices.Index([]string{silenceActive, silencePending, silenceExpired}, state)
}

// matchesFilter applies the alert manager filter semantics: the equality matchers of the silence form the label set.
func (s *Silence) matchesFilter(filters []*promql.Matcher) bool {
	set := promql.Labels{}
	for _, m := range s.Matchers {
		set[m.Name] = m.Value
	}

	return promql.MatchesAll(set, filters)
}

func (b *InMemoryBackend) listSilences(
	ctx context.Context, wsID string, filters []*promql.Matcher,
) ([]silenceJSON, error) {
	b.mu.RLock("ListAlertManagerSilences")
	defer b.mu.RUnlock()

	if err := b.silenceManagerLocked(ctx, wsID); err != nil {
		return nil, err
	}

	now := b.now()
	list := b.workspaceSilencesLocked(wsID)

	slices.SortFunc(list, func(x, y *Silence) int {
		if c := silenceRank(x.state(now)) - silenceRank(y.state(now)); c != 0 {
			return c
		}

		if c := x.EndsAt.Compare(y.EndsAt); c != 0 {
			return c
		}

		return strings.Compare(x.ID, y.ID)
	})

	out := make([]silenceJSON, 0, len(list))

	for _, s := range list {
		if s.matchesFilter(filters) {
			out = append(out, silenceView(s, now))
		}
	}

	return out, nil
}

func (b *InMemoryBackend) getSilence(ctx context.Context, wsID, id string) (silenceJSON, error) {
	b.mu.RLock("GetAlertManagerSilence")
	defer b.mu.RUnlock()

	if err := b.silenceManagerLocked(ctx, wsID); err != nil {
		return silenceJSON{}, err
	}

	s, ok := b.silences.Get(silenceKey(wsID, id))
	if !ok {
		return silenceJSON{}, notFound(resSilence, id, "silence %s not found", id)
	}

	return silenceView(s, b.now()), nil
}

func (b *InMemoryBackend) purgeSilencesLocked(now time.Time) {
	for _, s := range b.silences.All() {
		if now.After(s.EndsAt.Add(silenceRetention)) {
			b.silences.Delete(silenceKeyOf(s))
		}
	}
}
