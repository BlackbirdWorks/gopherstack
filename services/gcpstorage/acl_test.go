package gcpstorage_test

import (
	"bytes"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

func aclMap(rules []storage.ACLRule) map[string]storage.ACLRole {
	out := map[string]storage.ACLRole{}

	for _, r := range rules {
		e := string(r.Entity)
		if strings.HasPrefix(e, "project-") {
			e = "project-" + e[len("project-"):strings.LastIndex(e, "-")]
		}

		out[e] = r.Role
	}

	return out
}

func TestSDK_ACLCRUD(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		handle func(b *storage.BucketHandle) *storage.ACLHandle
	}{
		"bucket":         {func(b *storage.BucketHandle) *storage.ACLHandle { return b.ACL() }},
		"default object": {func(b *storage.BucketHandle) *storage.ACLHandle { return b.DefaultObjectACL() }},
		"object": {func(b *storage.BucketHandle) *storage.ACLHandle {
			return b.Object("o").ACL()
		}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			s := newSDK(t)
			ctx := t.Context()
			b := s.bucket(t, "acl-"+strings.ReplaceAll(name, " ", "-"), nil)
			write(t, b.Object("o"), []byte("x"), 0, nil)

			h := tc.handle(b)

			rules, err := h.List(ctx)
			require.NoError(t, err)
			assert.Equal(t, map[string]storage.ACLRole{
				"project-owners": storage.RoleOwner, "project-editors": storage.RoleOwner,
				"project-viewers": storage.RoleReader,
			}, aclMap(rules), "projectPrivate is the default")

			require.NoError(t, h.Set(ctx, storage.AllUsers, storage.RoleReader))
			require.NoError(t, h.Set(ctx, "user-a@example.com", storage.RoleOwner))
			require.NoError(t, h.Set(ctx, storage.AllUsers, storage.RoleOwner), "set replaces the role")

			rules, err = h.List(ctx)
			require.NoError(t, err)
			got := aclMap(rules)
			assert.Equal(t, storage.RoleOwner, got["allUsers"])
			assert.Equal(t, storage.RoleOwner, got["user-a@example.com"])

			idx := slices.IndexFunc(rules, func(r storage.ACLRule) bool { return r.Entity == "user-a@example.com" })
			assert.Equal(t, "a@example.com", rules[idx].Email)

			require.NoError(t, h.Delete(ctx, "user-a@example.com"))
			assert.Equal(t, http.StatusNotFound, apiCode(h.Delete(ctx, "user-a@example.com")))

			rules, err = h.List(ctx)
			require.NoError(t, err)
			assert.NotContains(t, aclMap(rules), "user-a@example.com")
		})
	}
}

func TestSDK_ACLRejectsBadInput(t *testing.T) {
	t.Parallel()

	s := newSDK(t)
	ctx := t.Context()
	b := s.bucket(t, "acl-bad", nil)
	write(t, b.Object("o"), []byte("x"), 0, nil)

	type badCase struct {
		h      *storage.ACLHandle
		entity storage.ACLEntity
		role   storage.ACLRole
		status int
		ok     bool
	}

	for name, tc := range map[string]badCase{
		"bad entity": {h: b.ACL(), entity: "nobody", role: storage.RoleReader, status: http.StatusBadRequest},
		"bad role":   {h: b.ACL(), entity: storage.AllUsers, role: "ADMIN", status: http.StatusBadRequest},
		"writer on object": {
			h: b.Object("o").ACL(), entity: storage.AllUsers, role: storage.RoleWriter, status: http.StatusBadRequest,
		},
		"writer on default": {
			h: b.DefaultObjectACL(), entity: storage.AllUsers, role: storage.RoleWriter, status: http.StatusBadRequest,
		},
		"object not found": {
			h: b.Object("missing").ACL(), entity: storage.AllUsers, role: storage.RoleReader, status: http.StatusNotFound,
		},
		"malformed team": {
			h: b.ACL(), entity: "project-nobody-1", role: storage.RoleReader, status: http.StatusBadRequest,
		},
		"writer on bucket": {h: b.ACL(), entity: storage.AllUsers, role: storage.RoleWriter, ok: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := tc.h.Set(ctx, tc.entity, tc.role)

			if tc.ok {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, tc.status, apiCode(err))
		})
	}
}

func TestSDK_PredefinedACL(t *testing.T) {
	t.Parallel()

	type want = map[string]storage.ACLRole

	owners := "project-owners"

	type preCase struct {
		bucket     want
		object     want
		predefined string
		bucketBad  bool
		objectBad  bool
	}

	pre := func(predefined string, bucket, object want, bucketBad, objectBad bool) preCase {
		return preCase{bucket, object, predefined, bucketBad, objectBad}
	}

	for name, tc := range map[string]preCase{
		"private": pre(
			"private",
			want{owners: storage.RoleOwner},
			want{"user-emulator@test-project.iam.gserviceaccount.com": storage.RoleOwner},
			false, false,
		),
		"publicRead": pre(
			"publicRead",
			want{owners: storage.RoleOwner, "allUsers": storage.RoleReader},
			want{"user-emulator@test-project.iam.gserviceaccount.com": storage.RoleOwner, "allUsers": storage.RoleReader},
			false, false,
		),
		"authenticatedRead": pre(
			"authenticatedRead",
			want{owners: storage.RoleOwner, "allAuthenticatedUsers": storage.RoleReader},
			want{
				"user-emulator@test-project.iam.gserviceaccount.com": storage.RoleOwner,
				"allAuthenticatedUsers":                              storage.RoleReader,
			},
			false, false,
		),
		"projectPrivate": pre(
			"projectPrivate",
			want{owners: storage.RoleOwner, "project-editors": storage.RoleOwner, "project-viewers": storage.RoleReader},
			want{owners: storage.RoleOwner, "project-editors": storage.RoleOwner, "project-viewers": storage.RoleReader},
			false, false,
		),
		"publicReadWrite": pre(
			"publicReadWrite",
			want{owners: storage.RoleOwner, "allUsers": storage.RoleWriter},
			nil,
			false, true,
		),
		"bucketOwnerRead": pre(
			"bucketOwnerRead",
			nil,
			want{"user-emulator@test-project.iam.gserviceaccount.com": storage.RoleOwner, owners: storage.RoleReader},
			true, false,
		),
		"bucketOwnerFullControl": pre(
			"bucketOwnerFullControl",
			nil,
			want{"user-emulator@test-project.iam.gserviceaccount.com": storage.RoleOwner, owners: storage.RoleOwner},
			true, false,
		),
		"unknown": pre("nonsense", nil, nil, true, true),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			s := newSDK(t)
			ctx := t.Context()
			id := strings.ToLower(name)

			err := s.client.Bucket("pre-b-"+id).
				Create(ctx, sdkProject, &storage.BucketAttrs{PredefinedACL: tc.predefined})
			if tc.bucketBad {
				assert.Equal(t, http.StatusBadRequest, apiCode(err))
			} else {
				require.NoError(t, err)

				rules, listErr := s.client.Bucket("pre-b-" + id).ACL().List(ctx)
				require.NoError(t, listErr)
				assert.Equal(t, tc.bucket, aclMap(rules))
			}

			err = s.client.Bucket("pre-d-"+id).Create(ctx, sdkProject,
				&storage.BucketAttrs{PredefinedDefaultObjectACL: tc.predefined})
			if tc.objectBad {
				assert.Equal(t, http.StatusBadRequest, apiCode(err))
			} else {
				require.NoError(t, err)

				b := s.client.Bucket("pre-d-" + id)
				rules, listErr := b.DefaultObjectACL().List(ctx)
				require.NoError(t, listErr)
				assert.Equal(t, tc.object, aclMap(rules))

				attrs := write(t, b.Object("inherits"), []byte("x"), 0, nil)
				assert.Equal(t, tc.object, aclMap(attrs.ACL), "new objects take the default object ACL")
			}

			ob := s.bucket(t, "pre-o-"+id, nil)
			w := ob.Object("up").NewWriter(ctx)
			w.PredefinedACL = tc.predefined
			_, werr := w.Write([]byte("x"))
			require.NoError(t, werr)

			cerr := w.Close()
			if tc.objectBad {
				assert.Equal(t, http.StatusBadRequest, apiCode(cerr))

				return
			}

			require.NoError(t, cerr)
			assert.Equal(t, tc.object, aclMap(w.Attrs().ACL))

			cp := ob.Object("copy").CopierFrom(ob.Object("up"))
			cp.PredefinedACL = tc.predefined
			cattrs, err := cp.Run(ctx)
			require.NoError(t, err)
			assert.Equal(t, tc.object, aclMap(cattrs.ACL))

			ob2 := ob.Object("plain")
			write(t, ob2, []byte("x"), 0, nil)

			uattrs, err := ob2.Update(ctx, storage.ObjectAttrsToUpdate{PredefinedACL: tc.predefined})
			require.NoError(t, err)
			assert.Equal(t, tc.object, aclMap(uattrs.ACL))
		})
	}
}

func TestSDK_PredefinedACLOnBucketUpdate(t *testing.T) {
	t.Parallel()

	s := newSDK(t)
	ctx := t.Context()
	b := s.bucket(t, "pre-upd", nil)

	_, err := b.Update(ctx, storage.BucketAttrsToUpdate{
		PredefinedACL: "publicRead", PredefinedDefaultObjectACL: "private",
	})
	require.NoError(t, err)

	rules, err := b.ACL().List(ctx)
	require.NoError(t, err)
	assert.Equal(t, storage.RoleReader, aclMap(rules)["allUsers"])

	def, err := b.DefaultObjectACL().List(ctx)
	require.NoError(t, err)
	assert.Len(t, def, 1)
}

func TestACLComposePredefined(t *testing.T) {
	t.Parallel()

	s := newSDK(t)
	b := s.bucket(t, "compose-acl", nil)
	write(t, b.Object("a"), []byte("a"), 0, nil)

	body := `{"sourceObjects":[{"name":"a"}],"destination":{"contentType":"text/plain"}}`
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		s.url+"/storage/v1/b/compose-acl/o/dst/compose?destinationPredefinedAcl=publicRead",
		bytes.NewBufferString(body))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)

	attrs, err := b.Object("dst").Attrs(t.Context())
	require.NoError(t, err)
	assert.Equal(t, storage.RoleReader, aclMap(attrs.ACL)["allUsers"])
}

func TestSDK_UniformBucketLevelAccessRejectsACL(t *testing.T) {
	t.Parallel()

	s := newSDK(t)
	ctx := t.Context()
	b := s.bucket(t, "ubla-bucket", &storage.BucketAttrs{
		UniformBucketLevelAccess: storage.UniformBucketLevelAccess{Enabled: true},
	})
	write(t, b.Object("o"), []byte("x"), 0, nil)

	attrs, err := b.Attrs(ctx)
	require.NoError(t, err)
	assert.True(t, attrs.UniformBucketLevelAccess.Enabled)
	assert.Empty(t, attrs.ACL)
	assert.Empty(t, attrs.DefaultObjectACL)

	oa, err := b.Object("o").Attrs(ctx)
	require.NoError(t, err)
	assert.Empty(t, oa.ACL)

	calls := map[string]func() error{
		"bucket list": func() error {
			_, e := b.ACL().List(ctx)

			return e
		},
		"bucket set":    func() error { return b.ACL().Set(ctx, storage.AllUsers, storage.RoleReader) },
		"bucket delete": func() error { return b.ACL().Delete(ctx, storage.AllUsers) },
		"default list": func() error {
			_, e := b.DefaultObjectACL().List(ctx)

			return e
		},
		"default set": func() error { return b.DefaultObjectACL().Set(ctx, storage.AllUsers, storage.RoleReader) },
		"object list": func() error {
			_, e := b.Object("o").ACL().List(ctx)

			return e
		},
		"object set":    func() error { return b.Object("o").ACL().Set(ctx, storage.AllUsers, storage.RoleReader) },
		"object delete": func() error { return b.Object("o").ACL().Delete(ctx, storage.AllUsers) },
		"update bucket": func() error {
			_, e := b.Update(ctx, storage.BucketAttrsToUpdate{PredefinedACL: "publicRead"})

			return e
		},
		"update object": func() error {
			_, e := b.Object("o").Update(ctx, storage.ObjectAttrsToUpdate{PredefinedACL: "private"})

			return e
		},
		"upload private": func() error {
			w := b.Object("p").NewWriter(ctx)
			w.PredefinedACL = "private"
			_, _ = w.Write([]byte("x"))

			return w.Close()
		},
	}

	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			callErr := call()
			assert.Equal(t, http.StatusBadRequest, apiCode(callErr))
			require.Error(t, callErr)
			assert.Contains(t, callErr.Error(), "uniform bucket-level access")
		})
	}
}

func TestSDK_UniformBucketLevelAccessDisable(t *testing.T) {
	t.Parallel()

	s := newSDK(t)
	ctx := t.Context()
	b := s.bucket(t, "ubla-off", &storage.BucketAttrs{
		UniformBucketLevelAccess: storage.UniformBucketLevelAccess{Enabled: true},
	})

	require.Error(t, b.ACL().Set(ctx, storage.AllUsers, storage.RoleReader))

	_, err := b.Update(ctx, storage.BucketAttrsToUpdate{
		UniformBucketLevelAccess: &storage.UniformBucketLevelAccess{Enabled: false},
	})
	require.NoError(t, err)
	require.NoError(t, b.ACL().Set(ctx, storage.AllUsers, storage.RoleReader), "ACLs work again once disabled")
}

func TestSDK_ACLPersists(t *testing.T) {
	t.Parallel()

	s := newSDK(t)
	ctx := t.Context()
	b := s.bucket(t, "acl-persist", nil)
	write(t, b.Object("o"), []byte("x"), 0, nil)
	require.NoError(t, b.ACL().Set(ctx, storage.AllUsers, storage.RoleReader))
	require.NoError(t, b.Object("o").ACL().Set(ctx, storage.AllAuthenticatedUsers, storage.RoleReader))

	s2 := newSDK(t)
	require.NoError(t, s2.backend.Restore(ctx, s.backend.Snapshot(ctx)))

	rules, err := s2.client.Bucket("acl-persist").ACL().List(ctx)
	require.NoError(t, err)
	assert.Equal(t, storage.RoleReader, aclMap(rules)["allUsers"])

	rules, err = s2.client.Bucket("acl-persist").Object("o").ACL().List(ctx)
	require.NoError(t, err)
	assert.Equal(t, storage.RoleReader, aclMap(rules)["allAuthenticatedUsers"])
}

func listNames(t *testing.T, b *storage.BucketHandle, q *storage.Query) ([]string, []string) {
	t.Helper()

	var names, prefixes []string

	it := b.Objects(t.Context(), q)

	for {
		a, err := it.Next()
		if errorsIsDone(err) {
			return names, prefixes
		}

		require.NoError(t, err)

		if a.Prefix != "" {
			prefixes = append(prefixes, a.Prefix)
		} else {
			names = append(names, a.Name)
		}
	}
}

func errorsIsDone(err error) bool { return errors.Is(err, iterator.Done) }

func TestSDK_MatchGlob(t *testing.T) {
	t.Parallel()

	s := newSDK(t)
	b := s.bucket(t, "glob-bucket", nil)

	for _, n := range []string{
		"a.txt", "b.txt", "ab.txt", "c.jpg", "foo/bar", "foo/baz/bar", "foo/baz/qux.jpg", "foo/x.txt", "dog1", "dog22",
		"é.txt",
	} {
		write(t, b.Object(n), []byte(n), 0, nil)
	}

	for name, tc := range map[string]struct {
		glob     string
		delim    string
		prefix   string
		names    []string
		prefixes []string
	}{
		"star no slash":     {glob: "*.txt", names: []string{"a.txt", "ab.txt", "b.txt", "é.txt"}},
		"question":          {glob: "?.txt", names: []string{"a.txt", "b.txt", "é.txt"}},
		"double star":       {glob: "**.jpg", names: []string{"c.jpg", "foo/baz/qux.jpg"}},
		"double star slash": {glob: "foo/**/bar", names: []string{"foo/bar", "foo/baz/bar"}},
		"class":             {glob: "[ab].txt", names: []string{"a.txt", "b.txt"}},
		"range":             {glob: "dog[0-9]", names: []string{"dog1"}},
		"negated class":     {glob: "[!a].txt", names: []string{"b.txt", "é.txt"}},
		"caret class":       {glob: "[^ab].txt", names: []string{"é.txt"}},
		"braces":            {glob: "*.{jpg,txt}", names: []string{"a.txt", "ab.txt", "b.txt", "c.jpg", "é.txt"}},
		"nested braces":     {glob: "{a,{b,c}}.{txt,jpg}", names: []string{"a.txt", "b.txt", "c.jpg"}},
		"escaped star":      {glob: `dog\*`, names: nil},
		"only objects": {
			glob: "**?", delim: "/", names: []string{"a.txt", "ab.txt", "b.txt", "c.jpg", "dog1", "dog22", "é.txt"},
		},
		"only prefixes":      {glob: "**/", delim: "/", prefixes: []string{"foo/"}},
		"with prefix":        {glob: "foo/*.txt", prefix: "foo/", names: []string{"foo/x.txt"}},
		"prefix excludes":    {glob: "*.txt", prefix: "foo/", names: nil},
		"delimiter filtered": {glob: "foo/*", delim: "/", names: nil, prefixes: []string{"foo/"}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			names, prefixes := listNames(
				t,
				b,
				&storage.Query{MatchGlob: tc.glob, Delimiter: tc.delim, Prefix: tc.prefix},
			)
			assert.Equal(t, tc.names, names)
			assert.Equal(t, tc.prefixes, prefixes)
		})
	}
}

func TestSDK_MatchGlobRejected(t *testing.T) {
	t.Parallel()

	s := newSDK(t)
	b := s.bucket(t, "glob-bad", nil)

	for name, q := range map[string]*storage.Query{
		"bad delimiter":    {MatchGlob: "*", Delimiter: "-"},
		"unclosed class":   {MatchGlob: "[abc"},
		"unclosed brace":   {MatchGlob: "{a,b"},
		"stray close":      {MatchGlob: "a}"},
		"trailing escape":  {MatchGlob: `a\`},
		"slash in braces":  {MatchGlob: "{a/b,c}"},
		"double star in {": {MatchGlob: "{**,c}"},
		"too long":         {MatchGlob: strings.Repeat("a", 1025)},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := b.Objects(t.Context(), q).Next()
			assert.Equal(t, http.StatusBadRequest, apiCode(err))
		})
	}
}

func TestSDK_StablePageTokens(t *testing.T) {
	t.Parallel()

	s := newSDK(t)
	ctx := t.Context()
	b := s.bucket(t, "page-bucket", nil)

	for _, n := range []string{"b", "d", "f", "h"} {
		write(t, b.Object(n), []byte(n), 0, nil)
	}

	var page []*storage.ObjectAttrs

	token, err := iterator.NewPager(b.Objects(ctx, nil), 2, "").NextPage(&page)
	require.NoError(t, err)
	require.Len(t, page, 2)
	require.NotEmpty(t, token)
	assert.Equal(t, "d", page[1].Name)

	// Objects land before the token, between pages, and after the cursor; one already-listed object is deleted.
	write(t, b.Object("a"), []byte("a"), 0, nil)
	write(t, b.Object("c"), []byte("c"), 0, nil)
	write(t, b.Object("g"), []byte("g"), 0, nil)
	require.NoError(t, b.Object("b").Delete(ctx))

	var next []*storage.ObjectAttrs

	_, err = iterator.NewPager(b.Objects(ctx, nil), 10, token).NextPage(&next)
	require.NoError(t, err)

	names := make([]string, 0, len(next))
	for _, a := range next {
		names = append(names, a.Name)
	}

	assert.Equal(t, []string{"f", "g", "h"}, names, "no repeats or skips for names after the cursor")
}

func TestSDK_PageTokenWithDelimiterAndVersions(t *testing.T) {
	t.Parallel()

	s := newSDK(t)
	ctx := t.Context()
	b := s.bucket(t, "page-delim", &storage.BucketAttrs{VersioningEnabled: true})

	for _, n := range []string{"a/1", "a/2", "b", "c/1", "d"} {
		write(t, b.Object(n), []byte(n), 0, nil)
	}

	write(t, b.Object("b"), []byte("again"), 0, nil)

	var seen []string

	token := ""

	for {
		var page []*storage.ObjectAttrs

		var err error

		token, err = iterator.NewPager(b.Objects(ctx, &storage.Query{Delimiter: "/", Versions: true}), 1, token).
			NextPage(&page)
		require.NoError(t, err)

		for _, a := range page {
			seen = append(seen, a.Prefix+a.Name)
		}

		if token == "" {
			break
		}
	}

	assert.Equal(t, []string{"a/", "b", "b", "c/", "d"}, seen)

	_, err := iterator.NewPager(b.Objects(ctx, nil), 1, "!!not-a-token").NextPage(&[]*storage.ObjectAttrs{})
	assert.Equal(t, http.StatusBadRequest, apiCode(err))
}
