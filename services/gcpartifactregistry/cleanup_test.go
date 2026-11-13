package gcpartifactregistry_test

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	arapi "google.golang.org/api/artifactregistry/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry/ocitest"
	"github.com/blackbirdworks/gopherstack/services/gcpartifactregistry"
)

func epochTime() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }

const day = 24 * time.Hour

type fakeClock struct{ ns atomic.Int64 }

func newFakeClock(t time.Time) *fakeClock {
	c := &fakeClock{}
	c.ns.Store(t.UnixNano())

	return c
}

func (c *fakeClock) now() time.Time          { return time.Unix(0, c.ns.Load()).UTC() }
func (c *fakeClock) set(t time.Time)         { c.ns.Store(t.UnixNano()) }
func (c *fakeClock) advance(d time.Duration) { c.ns.Add(int64(d)) }

func del(id string, c *artifactregistrypb.CleanupPolicyCondition) *artifactregistrypb.CleanupPolicy {
	return &artifactregistrypb.CleanupPolicy{
		Id: id, Action: artifactregistrypb.CleanupPolicy_DELETE,
		ConditionType: &artifactregistrypb.CleanupPolicy_Condition{Condition: c},
	}
}

func keep(id string, c *artifactregistrypb.CleanupPolicyCondition) *artifactregistrypb.CleanupPolicy {
	return &artifactregistrypb.CleanupPolicy{
		Id: id, Action: artifactregistrypb.CleanupPolicy_KEEP,
		ConditionType: &artifactregistrypb.CleanupPolicy_Condition{Condition: c},
	}
}

func keepRecent(id string, n int32, prefixes ...string) *artifactregistrypb.CleanupPolicy {
	return &artifactregistrypb.CleanupPolicy{
		Id: id, Action: artifactregistrypb.CleanupPolicy_KEEP,
		ConditionType: &artifactregistrypb.CleanupPolicy_MostRecentVersions{
			MostRecentVersions: &artifactregistrypb.CleanupPolicyMostRecentVersions{
				PackageNamePrefixes: prefixes,
				KeepCount:           &n,
			},
		},
	}
}

func older(d time.Duration) *artifactregistrypb.CleanupPolicyCondition {
	return &artifactregistrypb.CleanupPolicyCondition{OlderThan: durationpb.New(d)}
}

func policyRepo(dryRun bool, policies ...*artifactregistrypb.CleanupPolicy) *artifactregistrypb.Repository {
	r := &artifactregistrypb.Repository{
		Format: artifactregistrypb.Repository_GENERIC, CleanupPolicyDryRun: dryRun,
		CleanupPolicies: map[string]*artifactregistrypb.CleanupPolicy{},
	}
	for _, p := range policies {
		r.CleanupPolicies[p.GetId()] = p
	}

	return r
}

// uploadAt uploads pkg@version to a generic repository with the backend clock at when.
func uploadAt(t *testing.T, svc *arapi.Service, clock *fakeClock, when time.Time, pkg, version string) {
	t.Helper()

	clock.set(when)

	_, err := upload(t, svc, "cleanup", &arapi.UploadGenericArtifactRequest{
		PackageId: pkg, VersionId: version, Filename: "f.bin",
	}, []byte(pkg+version))
	require.NoError(t, err)
}

func versionsOf(t *testing.T, e *env, pkg string) []string {
	t.Helper()

	var out []string

	it := e.client.ListVersions(t.Context(), &artifactregistrypb.ListVersionsRequest{
		Parent: repoName("cleanup") + "/packages/" + pkg, OrderBy: "name",
	})

	for {
		v, err := it.Next()
		if err != nil {
			break
		}

		out = append(out, v.GetName()[len(repoName("cleanup")+"/packages/"+pkg+"/versions/"):])
	}

	return out
}

func TestCleanupPolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		policies []*artifactregistrypb.CleanupPolicy
		wantApp  []string
		wantLib  []string
		wantGone int
		dryRun   bool
	}{
		{
			name:     "older than",
			policies: []*artifactregistrypb.CleanupPolicy{del("d", older(15*day))},
			wantApp:  []string{"3"}, wantLib: []string{"3"}, wantGone: 4,
		},
		{
			name: "newer than",
			policies: []*artifactregistrypb.CleanupPolicy{
				del("d", &artifactregistrypb.CleanupPolicyCondition{NewerThan: durationpb.New(15 * day)}),
			},
			wantApp:  []string{"1", "2"},
			wantLib:  []string{"1", "2"},
			wantGone: 2,
		},
		{
			name: "keep beats delete",
			policies: []*artifactregistrypb.CleanupPolicy{
				del(
					"d",
					older(5*day),
				),
				keep("k", &artifactregistrypb.CleanupPolicyCondition{VersionNamePrefixes: []string{"1"}}),
			},
			wantApp: []string{"1"}, wantLib: []string{"1"}, wantGone: 4,
		},
		{
			name: "most recent versions",
			policies: []*artifactregistrypb.CleanupPolicy{
				del("d", older(time.Hour)), keepRecent("k", 2),
			},
			wantApp: []string{"2", "3"}, wantLib: []string{"2", "3"}, wantGone: 2,
		},
		{
			name: "most recent versions with package prefix",
			policies: []*artifactregistrypb.CleanupPolicy{
				del("d", older(time.Hour)), keepRecent("k", 1, "app"),
			},
			wantApp: []string{"3"}, wantLib: nil, wantGone: 5,
		},
		{
			name: "package prefix",
			policies: []*artifactregistrypb.CleanupPolicy{
				del("d", &artifactregistrypb.CleanupPolicyCondition{PackageNamePrefixes: []string{"lib"}}),
			},
			wantApp: []string{"1", "2", "3"}, wantLib: nil, wantGone: 3,
		},
		{
			name: "conditions are anded",
			policies: []*artifactregistrypb.CleanupPolicy{
				del(
					"d",
					&artifactregistrypb.CleanupPolicyCondition{
						PackageNamePrefixes: []string{"app"},
						OlderThan:           durationpb.New(15 * day),
						VersionNamePrefixes: []string{"1"},
					},
				),
			},
			wantApp:  []string{"2", "3"},
			wantLib:  []string{"1", "2", "3"},
			wantGone: 1,
		},
		{
			name:     "dry run deletes nothing",
			policies: []*artifactregistrypb.CleanupPolicy{del("d", older(time.Hour))},
			dryRun:   true,
			wantApp:  []string{"1", "2", "3"}, wantLib: []string{"1", "2", "3"}, wantGone: 6,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			clock := newFakeClock(epochTime())
			e := newEnv(t, "")
			e.handler.Backend.SetClock(clock.now)
			e.createRepo(t, "cleanup", policyRepo(tt.dryRun, tt.policies...))

			svc := restService(t, e)

			for i, v := range []string{"1", "2", "3"} {
				for _, pkg := range []string{"app", "lib"} {
					uploadAt(t, svc, clock, epochTime().Add(time.Duration(i)*10*day), pkg, v)
				}
			}

			clock.set(epochTime().Add(30 * day))

			results := e.handler.Backend.RunCleanup(t.Context())
			require.Len(t, results, 1)
			assert.Equal(t, tt.dryRun, results[0].DryRun)
			assert.Len(t, results[0].Versions, tt.wantGone)

			assert.ElementsMatch(t, tt.wantApp, versionsOf(t, e, "app"))
			assert.ElementsMatch(t, tt.wantLib, versionsOf(t, e, "lib"))
		})
	}
}

func TestCleanupWorkerRunsOnTick(t *testing.T) {
	t.Parallel()

	clock := newFakeClock(epochTime())
	ticks := make(chan time.Time)

	e := newEnvWith(t, "", func(b *gcpartifactregistry.InMemoryBackend) {
		b.SetClock(clock.now)
		b.SetCleanupTicker(func(time.Duration) (<-chan time.Time, func()) { return ticks, func() {} })
	})

	e.createRepo(t, "cleanup", policyRepo(false, del("d", older(day))))

	svc := restService(t, e)
	uploadAt(t, svc, clock, epochTime(), "app", "1")

	clock.advance(2 * day)
	ticks <- clock.now()

	require.Eventually(t, func() bool { return len(versionsOf(t, e, "app")) == 0 }, 5*time.Second, 10*time.Millisecond)
}

func TestCleanupPolicyValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")

	tests := []struct {
		policy *artifactregistrypb.CleanupPolicy
		name   string
	}{
		{
			name:   "delete without condition",
			policy: &artifactregistrypb.CleanupPolicy{Id: "x", Action: artifactregistrypb.CleanupPolicy_DELETE},
		},
		{
			name: "no action",
			policy: &artifactregistrypb.CleanupPolicy{
				Id:            "x",
				ConditionType: &artifactregistrypb.CleanupPolicy_Condition{Condition: older(day)},
			},
		},
		{name: "delete most recent", policy: &artifactregistrypb.CleanupPolicy{
			Id:     "x",
			Action: artifactregistrypb.CleanupPolicy_DELETE,
			ConditionType: &artifactregistrypb.CleanupPolicy_MostRecentVersions{
				MostRecentVersions: &artifactregistrypb.CleanupPolicyMostRecentVersions{},
			},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.client.CreateRepository(t.Context(), &artifactregistrypb.CreateRepositoryRequest{
				Parent: loc, RepositoryId: "bad-policy", Repository: policyRepo(false, tt.policy),
			})
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}

	e.createRepo(t, "ok-policy", policyRepo(false))

	_, err := e.client.UpdateRepository(t.Context(), &artifactregistrypb.UpdateRepositoryRequest{
		Repository: policyRepo(false, tt0()), UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"cleanup_policies"}},
	})
	assert.Error(t, err)
}

func tt0() *artifactregistrypb.CleanupPolicy {
	return &artifactregistrypb.CleanupPolicy{Id: "x", Action: artifactregistrypb.CleanupPolicy_DELETE}
}

func TestCleanupDockerTagState(t *testing.T) {
	t.Parallel()

	clock := newFakeClock(epochTime())
	e := newEnvWith(t, "", func(b *gcpartifactregistry.InMemoryBackend) { b.SetClock(clock.now) })

	repo := dockerRepo()
	repo.CleanupPolicies = map[string]*artifactregistrypb.CleanupPolicy{
		"tagged": del("tagged", &artifactregistrypb.CleanupPolicyCondition{
			TagState: artifactregistrypb.CleanupPolicyCondition_TAGGED.Enum(), OlderThan: durationpb.New(day),
		}),
		"keep-release": keep(
			"keep-release",
			&artifactregistrypb.CleanupPolicyCondition{TagPrefixes: []string{"release"}},
		),
	}
	e.createRepo(t, "dock", repo)

	c := e.docker(t)
	pushTo(c, "dock", "app", "dev-1")
	pushTo(c, "dock", "svc", "release-1")

	clock.advance(2 * day)
	pushTo(c, "dock", "fresh", "dev-2")

	results := e.handler.Backend.RunCleanup(t.Context())
	require.Len(t, results, 1)
	require.Len(t, results[0].Versions, 1)
	assert.Contains(t, results[0].Versions[0], "/dock/packages/app/versions/sha256:")

	pk := e.client.ListPackages(t.Context(), &artifactregistrypb.ListPackagesRequest{Parent: repoName("dock")})

	var names []string

	for {
		p, err := pk.Next()
		if err != nil {
			break
		}

		names = append(names, p.GetName())
	}

	assert.Len(t, names, 2, "app was deleted; the release-tagged and the fresh images stay")
}

func TestCleanupKeepsImagesReferencedByParentManifest(t *testing.T) {
	t.Parallel()

	clock := newFakeClock(epochTime())
	e := newEnvWith(t, "", func(b *gcpartifactregistry.InMemoryBackend) { b.SetClock(clock.now) })

	repo := dockerRepo()
	repo.CleanupPolicies = map[string]*artifactregistrypb.CleanupPolicy{
		"all": del("all", &artifactregistrypb.CleanupPolicyCondition{OlderThan: durationpb.New(day)}),
	}
	e.createRepo(t, "multi", repo)

	c := e.docker(t)
	amd := pushTo(c, "multi", "app", "amd")
	arm := pushTo(c, "multi", "app", "arm")

	manifests := make([]map[string]any, 0, 2)
	for _, img := range []ocitest.Image{amd, arm} {
		manifests = append(manifests, map[string]any{
			"mediaType": ocitest.ManifestMediaType, "digest": img.Digest, "size": len(img.Body),
		})
	}

	index, err := json.Marshal(map[string]any{
		"schemaVersion": 2, "mediaType": "application/vnd.oci.image.index.v1+json", "manifests": manifests,
	})
	require.NoError(t, err)

	put := c.Do(http.MethodPut, "/v2/"+project+"/multi/app/manifests/multi", index,
		map[string]string{"Content-Type": "application/vnd.oci.image.index.v1+json"})
	require.Equal(t, http.StatusCreated, put.Status, string(put.Body))

	clock.advance(2 * day)

	first := e.handler.Backend.RunCleanup(t.Context())
	require.Len(t, first, 1)
	require.Len(t, first[0].Versions, 1, "only the index goes while it references the others")
	assert.Contains(t, first[0].Versions[0], ocitest.Digest(index))

	second := e.handler.Backend.RunCleanup(t.Context())
	require.Len(t, second, 1)
	assert.Len(t, second[0].Versions, 2, "the children go on the next run")
}
