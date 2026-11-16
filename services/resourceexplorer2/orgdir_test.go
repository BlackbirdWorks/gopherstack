package resourceexplorer2_test

import (
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/resourceexplorer2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourceexplorer2"
)

const (
	orgMgmt   = "000000000001"
	orgMember = "111111111111"
	orgAdmin  = "222222222222"
	orgOU     = "arn:aws:organizations::000000000001:ou/o-abc/ou-abc-1"
	orgARN    = "arn:aws:organizations::000000000001:organization/o-abc"
)

type fakeDirectory struct {
	members  map[string]bool
	noAccess bool
}

func (f fakeDirectory) ServiceAccessEnabled(string) bool { return !f.noAccess }

func (f fakeDirectory) ManagementAccount() (string, bool) { return orgMgmt, true }
func (f fakeDirectory) IsMember(a string) bool            { return f.members[a] }
func (f fakeDirectory) IsDelegatedAdmin(_, a string) bool { return a == orgAdmin }
func (f fakeDirectory) ScopeExists(s string) bool         { return s == orgOU || s == orgARN }

func newOrgFixture(t *testing.T) *fixture {
	t.Helper()

	b := resourceexplorer2.NewInMemoryBackend(orgMgmt, "us-east-1")
	b.SetClock((&fakeClock{t: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}).now)
	b.SetOrgAccess(fakeDirectory{
		members: map[string]bool{orgMgmt: true, orgMember: true, orgAdmin: true},
	})

	return &fixture{backend: b, url: newServedURL(t, b)}
}

func TestListIndexesForMembersAuthorization(t *testing.T) {
	t.Parallel()

	f := newOrgFixture(t)
	member := f.clientIn(t, "us-east-1", orgMember)

	_, err := member.CreateIndex(t.Context(), &sdk.CreateIndexInput{})
	require.NoError(t, err)

	_, err = f.clientIn(t, "us-east-1", "333333333333").CreateIndex(t.Context(), &sdk.CreateIndexInput{})
	require.NoError(t, err)

	tests := []struct {
		name     string
		caller   string
		wantCode string
		accounts []string
		want     int
	}{
		{name: "management", caller: orgMgmt, accounts: []string{orgMember}, want: 1},
		{name: "delegated_admin", caller: orgAdmin, accounts: []string{orgMember}, want: 1},
		{
			name:     "plain_member_denied",
			caller:   orgMember,
			accounts: []string{orgMember},
			wantCode: "AccessDeniedException",
		},
		{name: "non_member_account_hidden", caller: orgMgmt, accounts: []string{"333333333333"}, want: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, lerr := f.clientIn(t, "us-east-1", tc.caller).ListIndexesForMembers(
				t.Context(), &sdk.ListIndexesForMembersInput{AccountIdList: tc.accounts})
			if tc.wantCode != "" {
				assert.Equal(t, tc.wantCode, apiCode(lerr))

				return
			}

			require.NoError(t, lerr)
			assert.Len(t, out.Indexes, tc.want)
		})
	}
}

func TestCreateViewScopeValidation(t *testing.T) {
	t.Parallel()

	f := newOrgFixture(t)
	c := f.clientIn(t, "us-east-1", orgMgmt)

	_, err := c.CreateIndex(t.Context(), &sdk.CreateIndexInput{})
	require.NoError(t, err)

	tests := []struct {
		name     string
		scope    string
		wantCode string
	}{
		{name: "account_root", scope: "arn:aws:iam::" + orgMgmt + ":root"},
		{name: "organization", scope: orgARN},
		{name: "ou", scope: orgOU},
		{
			name:     "unknown_ou",
			scope:    "arn:aws:organizations::000000000001:ou/o-abc/ou-zzz",
			wantCode: "ValidationException",
		},
		{name: "foreign_root", scope: "arn:aws:iam::999999999999:root", wantCode: "ValidationException"},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, cerr := c.CreateView(t.Context(), &sdk.CreateViewInput{
				ViewName: aws.String("v" + string(rune('a'+i))), Scope: aws.String(tc.scope),
			})
			if tc.wantCode != "" {
				assert.Equal(t, tc.wantCode, apiCode(cerr))

				return
			}

			assert.NoError(t, cerr)
		})
	}
}

func TestSearchResourceTypeSupportsFilter(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	c := f.client(t)
	setupViewed(t, c, "tags")

	tests := []struct {
		name  string
		query string
		want  int
	}{
		{name: "tags", query: "resourcetype.supports:tags", want: 5},
		{name: "tags_upper", query: "resourcetype.supports:TAGS", want: 5},
		{name: "negated", query: "-resourcetype.supports:tags", want: 0},
		{name: "unsupported_value", query: "resourcetype.supports:other", want: 0},
		{name: "combined", query: "resourcetype.supports:tags service:sqs", want: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := c.Search(t.Context(), &sdk.SearchInput{QueryString: aws.String(tc.query)})
			require.NoError(t, err)
			assert.Len(t, out.Resources, tc.want)
		})
	}
}

type lockProbeDirectory struct {
	b    *resourceexplorer2.InMemoryBackend
	held *atomic.Bool
	fakeDirectory
}

func (lockProbeDirectory) ManagementAccount() (string, bool) { return testAccount, true }

func (d lockProbeDirectory) IsMember(string) bool {
	done := make(chan struct{})

	go func() {
		d.b.SetClock(time.Now)
		close(done)
	}()

	for {
		select {
		case <-done:
			return true
		default:
			if d.b.WriteWaiters() > 0 {
				d.held.Store(true)

				return true
			}

			runtime.Gosched()
		}
	}
}

func TestListIndexesForMembersCallsDirectoryOutsideLock(t *testing.T) {
	t.Parallel()

	b := resourceexplorer2.NewInMemoryBackend(orgMgmt, "us-east-1")
	held := &atomic.Bool{}
	b.SetOrgAccess(lockProbeDirectory{b: b, held: held})

	_, err := b.ListIndexesForMembers(t.Context(), []string{orgMember}, "", 0)
	require.NoError(t, err)
	assert.False(t, held.Load(), "Organizations was called while the Resource Explorer lock was held")
}

func TestSearchMalformedQueries(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	c := f.client(t)
	setupViewed(t, c, "tags")

	tests := []struct {
		name     string
		query    string
		wantCode string
	}{
		{name: "unterminated_quote", query: `"abc`, wantCode: "ValidationException"},
		{name: "empty_filter_value", query: "service:", wantCode: "ValidationException"},
		{name: "empty_supports_value", query: "resourcetype.supports:", wantCode: "ValidationException"},
		{name: "negated_empty_filter", query: "-tag:", wantCode: "ValidationException"},
		{name: "over_length", query: strings.Repeat("a", 2049), wantCode: "ValidationException"},
		{name: "lone_dash", query: "-"},
		{name: "lone_colon", query: ":"},
		{name: "trailing_backslash", query: `abc\`},
		{name: "only_quotes", query: `""""""`},
		{name: "escaped_quotes_run", query: strings.Repeat(`\"`, 1000)},
		{name: "max_length_terms", query: strings.Repeat("a ", 1024)},
		{name: "max_length_values", query: "service:" + strings.Repeat("a,", 1020)},
		{name: "nul_bytes", query: "a\x00b service:\x00"},
		{name: "unknown_filter", query: "nosuch:value"},
		{name: "multibyte", query: "日本語 tag:ключ=значение"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := c.Search(t.Context(), &sdk.SearchInput{QueryString: aws.String(tc.query)})
			if tc.wantCode != "" {
				assert.Equal(t, tc.wantCode, apiCode(err))

				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestOrgChecksOffWithoutOrganizations(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	c := f.clientIn(t, "us-east-1", "333333333333")

	_, err := c.CreateIndex(t.Context(), &sdk.CreateIndexInput{})
	require.NoError(t, err)

	out, err := c.ListIndexesForMembers(t.Context(), &sdk.ListIndexesForMembersInput{
		AccountIdList: []string{"333333333333", "444444444444"},
	})
	require.NoError(t, err)
	assert.Len(t, out.Indexes, 1)

	_, err = c.CreateView(t.Context(), &sdk.CreateViewInput{
		ViewName: aws.String("anyscope"),
		Scope:    aws.String("arn:aws:organizations::1:ou/o-nothing/ou-nothing-1"),
	})
	assert.NoError(t, err)
}

func TestListIndexesForMembersNeedsServiceAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		caller   string
		wantCode string
		noAccess bool
	}{
		{name: "admin_with_access", caller: orgAdmin},
		{name: "admin_without_access", caller: orgAdmin, noAccess: true, wantCode: "AccessDeniedException"},
		{name: "management_without_access", caller: orgMgmt, noAccess: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := resourceexplorer2.NewInMemoryBackend(orgMgmt, "us-east-1")
			b.SetOrgAccess(fakeDirectory{members: map[string]bool{orgMember: true}, noAccess: tc.noAccess})
			f := &fixture{backend: b, url: newServedURL(t, b)}

			_, err := f.clientIn(t, "us-east-1", tc.caller).ListIndexesForMembers(
				t.Context(), &sdk.ListIndexesForMembersInput{AccountIdList: []string{orgMember}})
			assert.Equal(t, tc.wantCode, apiCode(err))
		})
	}
}
