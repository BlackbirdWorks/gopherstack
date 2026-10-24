package gcpartifactregistry_test

import (
	"net/http"
	"testing"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"google.golang.org/api/iterator"
	"google.golang.org/genproto/googleapis/type/expr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry/ocitest"
)

func ruleOf(action artifactregistrypb.Rule_Action, pkg string) *artifactregistrypb.Rule {
	return &artifactregistrypb.Rule{Action: action, Operation: artifactregistrypb.Rule_DOWNLOAD, PackageId: pkg}
}

func TestRuleCRUD(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "rules", dockerRepo())

	ctx, parent := t.Context(), repoName("rules")

	created, err := e.client.CreateRule(ctx, &artifactregistrypb.CreateRuleRequest{
		Parent: parent, RuleId: "no-dl", Rule: ruleOf(artifactregistrypb.Rule_DENY, ""),
	})
	require.NoError(t, err)
	assert.Equal(t, parent+"/rules/no-dl", created.GetName())

	tests := []struct {
		req  *artifactregistrypb.CreateRuleRequest
		name string
		code codes.Code
	}{
		{
			&artifactregistrypb.CreateRuleRequest{
				Parent: parent,
				RuleId: "no-dl",
				Rule:   ruleOf(artifactregistrypb.Rule_DENY, "x"),
			},
			"duplicate id",
			codes.AlreadyExists,
		},
		{
			&artifactregistrypb.CreateRuleRequest{
				Parent: parent,
				RuleId: "second",
				Rule:   ruleOf(artifactregistrypb.Rule_ALLOW, ""),
			},
			"second repository rule",
			codes.AlreadyExists,
		},
		{
			&artifactregistrypb.CreateRuleRequest{
				Parent: parent,
				RuleId: "Bad_ID",
				Rule:   ruleOf(artifactregistrypb.Rule_ALLOW, "p"),
			},
			"bad id",
			codes.InvalidArgument,
		},
		{&artifactregistrypb.CreateRuleRequest{Parent: parent, RuleId: "noact", Rule: &artifactregistrypb.Rule{
			Operation: artifactregistrypb.Rule_DOWNLOAD,
		}}, "no action", codes.InvalidArgument},
		{&artifactregistrypb.CreateRuleRequest{Parent: parent, RuleId: "noop", Rule: &artifactregistrypb.Rule{
			Action: artifactregistrypb.Rule_ALLOW,
		}}, "no operation", codes.InvalidArgument},
		{
			&artifactregistrypb.CreateRuleRequest{
				Parent: repoName("ghost"),
				RuleId: "r",
				Rule:   ruleOf(artifactregistrypb.Rule_ALLOW, ""),
			},
			"no repository",
			codes.NotFound,
		},
	}

	for _, tt := range tests {
		_, err = e.client.CreateRule(ctx, tt.req)
		assert.Equal(t, tt.code, status.Code(err), tt.name)
	}

	_, err = e.client.CreateRule(ctx, &artifactregistrypb.CreateRuleRequest{
		Parent: parent, RuleId: "pkg-rule", Rule: ruleOf(artifactregistrypb.Rule_ALLOW, "app"),
	})
	require.NoError(t, err)

	var names []string

	it := e.client.ListRules(ctx, &artifactregistrypb.ListRulesRequest{Parent: parent, PageSize: 1})

	for {
		r, nextErr := it.Next()
		if nextErr != nil {
			require.ErrorIs(t, nextErr, iterator.Done)

			break
		}

		names = append(names, r.GetName())
	}

	assert.Equal(t, []string{parent + "/rules/no-dl", parent + "/rules/pkg-rule"}, names)

	updated, err := e.client.UpdateRule(ctx, &artifactregistrypb.UpdateRuleRequest{
		Rule:       &artifactregistrypb.Rule{Name: parent + "/rules/pkg-rule", Action: artifactregistrypb.Rule_DENY},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"action"}},
	})
	require.NoError(t, err)
	assert.Equal(t, artifactregistrypb.Rule_DENY, updated.GetAction())
	assert.Equal(t, "app", updated.GetPackageId())

	_, err = e.client.UpdateRule(ctx, &artifactregistrypb.UpdateRuleRequest{
		Rule:       &artifactregistrypb.Rule{Name: parent + "/rules/pkg-rule", PackageId: ""},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"package_id"}},
	})
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	_, err = e.client.UpdateRule(ctx, &artifactregistrypb.UpdateRuleRequest{
		Rule: &artifactregistrypb.Rule{
			Name: parent + "/rules/pkg-rule",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"name"}},
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	require.NoError(
		t,
		e.client.DeleteRule(ctx, &artifactregistrypb.DeleteRuleRequest{Name: parent + "/rules/pkg-rule"}),
	)

	_, err = e.client.GetRule(ctx, &artifactregistrypb.GetRuleRequest{Name: parent + "/rules/pkg-rule"})
	assert.Equal(t, codes.NotFound, status.Code(err))
	assert.Equal(t, codes.NotFound, status.Code(
		e.client.DeleteRule(ctx, &artifactregistrypb.DeleteRuleRequest{Name: parent + "/rules/pkg-rule"})))
}

func TestRulesDenyDockerPulls(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "pull", dockerRepo())

	c := e.docker(t)
	app := pushTo(c, "pull", "app", "v1")
	pushTo(c, "pull", "lib", "v1")

	ctx, parent := t.Context(), repoName("pull")
	base := "/v2/" + project + "/pull/"
	accept := map[string]string{"Accept": "application/vnd.docker.distribution.manifest.v2+json"}

	get := func(path string) int { return c.Do(http.MethodGet, base+path, nil, accept).Status }
	pull := func(image string) (int, int) {
		return get(image + "/manifests/v1"), get(image + "/blobs/" + ocitest.Digest(app.Layer))
	}

	create := func(id, pkg string, action artifactregistrypb.Rule_Action) {
		_, err := e.client.CreateRule(ctx, &artifactregistrypb.CreateRuleRequest{
			Parent: parent, RuleId: id, Rule: ruleOf(action, pkg),
		})
		require.NoError(t, err)
	}

	first, _ := pull("app")
	assert.Equal(t, http.StatusOK, first, "no rules allows")

	create("deny-repo", "", artifactregistrypb.Rule_DENY)

	for _, image := range []string{"app", "lib"} {
		m, b := pull(image)
		assert.Equal(t, http.StatusForbidden, m, image)
		assert.Equal(t, http.StatusForbidden, b, image)
	}

	assert.Equal(t, http.StatusForbidden, c.Do(http.MethodHead, base+"app/manifests/v1", nil, accept).Status)

	create("allow-app", "app", artifactregistrypb.Rule_ALLOW)

	m, b := pull("app")
	assert.Equal(t, http.StatusOK, m, "package rule overrides repository rule")
	assert.NotEqual(t, http.StatusForbidden, b)

	lib, _ := pull("lib")
	assert.Equal(t, http.StatusForbidden, lib)

	require.NoError(
		t,
		e.client.DeleteRule(ctx, &artifactregistrypb.DeleteRuleRequest{Name: parent + "/rules/deny-repo"}),
	)

	lib, _ = pull("lib")
	assert.Equal(t, http.StatusOK, lib, "deleting the rule allows again")

	_, err := e.client.CreateRule(ctx, &artifactregistrypb.CreateRuleRequest{
		Parent: parent, RuleId: "cond", Rule: &artifactregistrypb.Rule{
			Action: artifactregistrypb.Rule_DENY, Operation: artifactregistrypb.Rule_DOWNLOAD, PackageId: "lib",
			Condition: &expr.Expr{Expression: `tag_name == "v1"`},
		},
	})
	require.NoError(t, err)

	lib, _ = pull("lib")
	assert.Equal(t, http.StatusOK, lib, "rules with a condition are stored but not evaluated")
}
