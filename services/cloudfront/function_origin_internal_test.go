package cloudfront

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubKVS struct{}

func (stubKVS) KVSIDs() []string                 { return nil }
func (stubKVS) Value(_, _ string) (string, bool) { return "", false }
func (stubKVS) Meta(string) (kvsMeta, bool)      { return kvsMeta{}, false }

func runOrigin(t *testing.T, call string, ids ...string) fnResult {
	t.Helper()

	return runCloudFrontFunction(fnRun{
		Runtime: runtimeJS2, EventType: eventViewerRequest, KVS: stubKVS{}, Origins: newOriginCatalog(ids),
		Code:  `import cf from 'cloudfront'; function handler(e) { ` + call + `; return e.request; }`,
		Event: map[string]any{"request": map[string]any{"uri": "/a"}},
	})
}

func TestOriginDirective(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check   func(*testing.T, *originDirective)
		name    string
		call    string
		wantErr string
		ids     []string
	}{
		{
			name: "update_records_params", ids: []string{"o1"},
			call: `cf.updateRequestOrigin({domainName: 'x.example.com', originPath: '/p'})`,
			check: func(t *testing.T, d *originDirective) {
				t.Helper()
				assert.Empty(t, d.Selected)
				assert.Equal(t, "x.example.com", d.Params.Domain)
				assert.Equal(t, "/p", *d.Params.Path)
			},
		},
		{
			name: "select_records_id_and_overrides", ids: []string{"o1", "o2"},
			call: `cf.selectRequestOriginById('o2', {originPath: '/z'})`,
			check: func(t *testing.T, d *originDirective) {
				t.Helper()
				assert.Equal(t, "o2", d.Selected)
				assert.Equal(t, "/z", *d.Params.Path)
			},
		},
		{
			name: "select_unknown_origin_fails", ids: []string{"o1"},
			call: `cf.selectRequestOriginById('nope')`, wantErr: "origin nope does not exist",
		},
		{
			name: "group_records_members_and_failover", ids: []string{"o1", "o2"},
			call: `cf.createRequestOriginGroup({originIds: ['o1', 'o2'], failoverCriteria: {statusCodes: [503, 500]}})`,
			check: func(t *testing.T, d *originDirective) {
				t.Helper()
				require.NotNil(t, d.Group)
				assert.Equal(t, "o1", d.Group.Members[0].ID)
				assert.Equal(t, []int{500, 503}, d.Group.Failover)
				assert.Equal(t, criteriaDefault, d.Group.Criteria)
			},
		},
		{
			name: "group_default_failover_codes", ids: []string{"o1", "o2"},
			call: `cf.createRequestOriginGroup({originIds: ['o1', 'o2']})`,
			check: func(t *testing.T, d *originDirective) {
				t.Helper()
				assert.Equal(t, defaultFailoverCodes, d.Group.Failover)
			},
		},
		{
			name: "group_unknown_member_fails", ids: []string{"o1"},
			call: `cf.createRequestOriginGroup({originIds: ['o1', 'o9']})`, wantErr: "origin \"o9\" does not exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res := runOrigin(t, tt.call, tt.ids...)

			if tt.wantErr != "" {
				assert.Contains(t, res.ErrMsg, tt.wantErr)
				assert.Nil(t, res.Origin)

				return
			}

			require.Empty(t, res.ErrMsg)
			require.NotNil(t, res.Origin)
			tt.check(t, res.Origin)
		})
	}
}

func TestOriginPlan(t *testing.T) {
	t.Parallel()

	cfg := &dpDistConfig{
		Origins: []dpOrigin{
			{ID: "a", Domain: "a.example.com", Custom: &dpCustomOrigin{Policy: originPolicyHTTP, HTTPPort: 80}},
			{ID: "b", Domain: "b.example.com", Custom: &dpCustomOrigin{Policy: originPolicyHTTP, HTTPPort: 80}},
			{ID: "s", Domain: "bkt.s3.us-east-1.amazonaws.com", S3: &dpS3Origin{}},
		},
		Groups: []dpOriginGroup{{ID: "g", Members: []string{"a", "b"}, Codes: []int{503}}},
	}
	path := "/p"

	tests := []struct {
		dir      *originDirective
		check    func(*testing.T, *originPlan)
		name     string
		target   string
		wantNone bool
	}{
		{name: "plain_origin", target: "a", check: func(t *testing.T, p *originPlan) {
			t.Helper()
			require.Len(t, p.members, 1)
			assert.Equal(t, "a.example.com", p.members[0].Domain)
		}},
		{name: "unknown_target", target: "zz", wantNone: true},
		{name: "behavior_targets_group", target: "g", check: func(t *testing.T, p *originPlan) {
			t.Helper()
			require.Len(t, p.members, 2)
			assert.True(t, p.shouldFailover("GET", 503))
			assert.False(t, p.shouldFailover("POST", 503))
			assert.False(t, p.shouldFailover("GET", 500))
		}},
		{
			name: "select_swaps_origin", target: "a", dir: &originDirective{Selected: "b"},
			check: func(t *testing.T, p *originPlan) {
				t.Helper()
				assert.Equal(t, "b.example.com", p.members[0].Domain)
			},
		},
		{
			name:   "update_overrides_copy_only",
			target: "a",
			dir: &originDirective{
				Params: &originParams{Domain: "new.example.com", Path: &path, Port: 9000, Attempts: 2},
			},
			check: func(t *testing.T, p *originPlan) {
				t.Helper()
				m := p.members[0]
				assert.Equal(t, "new.example.com", m.Domain)
				assert.Equal(t, "/p", m.Path)
				assert.Equal(t, 9000, m.Custom.HTTPPort)
				assert.Equal(t, 2, m.ConnAttempts)
				assert.Equal(t, "a.example.com", cfg.Origins[0].Domain, "config must stay untouched")
				assert.Equal(t, 80, cfg.Origins[0].Custom.HTTPPort)
			},
		},
		{
			name: "update_domain_to_s3_drops_custom", target: "a",
			dir: &originDirective{Params: &originParams{Domain: "bkt2.s3.us-east-1.amazonaws.com"}},
			check: func(t *testing.T, p *originPlan) {
				t.Helper()
				assert.True(t, isS3Origin(&p.members[0]))
			},
		},
		{
			name: "update_s3_to_custom_domain", target: "s",
			dir: &originDirective{Params: &originParams{Domain: "api.example.com"}},
			check: func(t *testing.T, p *originPlan) {
				t.Helper()
				assert.False(t, isS3Origin(&p.members[0]))
				assert.NotNil(t, p.members[0].Custom)
			},
		},
		{
			name: "created_group_overrides_member", target: "a",
			dir: &originDirective{Group: &groupSpec{
				Members:  []groupMember{{ID: "a"}, {ID: "b", Over: &originParams{Path: &path}}},
				Failover: []int{500},
			}},
			check: func(t *testing.T, p *originPlan) {
				t.Helper()
				require.Len(t, p.members, 2)
				assert.Equal(t, "/p", p.members[1].Path)
				assert.Empty(t, p.members[0].Path)
				assert.True(t, p.shouldFailover("HEAD", 500))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := cfg.plan(tt.target, tt.dir)
			if tt.wantNone {
				assert.Nil(t, p)

				return
			}

			require.NotNil(t, p)
			tt.check(t, p)
		})
	}
}
