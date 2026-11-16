package lambdaweb_test

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lwsdk "github.com/aws/aws-sdk-go-v2/service/lambdaweb"
	"github.com/aws/aws-sdk-go-v2/service/lambdaweb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambdaweb"
)

const (
	mrHome    = "us-east-1"
	mrReplica = "eu-west-1"
	mrKeyHome = "arn:aws:kms:us-east-1:123456789012:key/mrk-aaa"
)

var errNoReplica = errors.New("key replica not found")

// replicaKeys has a usable key in every Region except the ones listed in missing.
type replicaKeys struct {
	missing map[string]bool
	mu      sync.Mutex
}

func (k *replicaKeys) setMissing(region string, missing bool) {
	k.mu.Lock()
	defer k.mu.Unlock()

	k.missing[region] = missing
}

func (k *replicaKeys) CheckKey(_ context.Context, region, _ string) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if k.missing[region] {
		return errNoReplica
	}

	return nil
}

// regionLauncher records the Region each environment is launched to serve.
type regionLauncher struct {
	inner   *fakeLauncher
	regions []string
	mu      sync.Mutex
}

func (l *regionLauncher) Launch(ctx context.Context, spec lambdaweb.LaunchSpec) (lambdaweb.Environment, error) {
	l.mu.Lock()
	l.regions = append(l.regions, spec.Region)
	l.mu.Unlock()

	return l.inner.Launch(ctx, spec)
}

func (l *regionLauncher) served() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]string(nil), l.regions...)
}

type mrEnv struct {
	*dataEnv
	keys  *replicaKeys
	spawn *regionLauncher
}

func newMultiRegionEnv(t *testing.T) *mrEnv {
	t.Helper()

	d := newDataEnv(t)
	keys := &replicaKeys{missing: map[string]bool{}}
	d.backend.SetKeyChecker(keys)

	spawn := &regionLauncher{inner: d.launcher}
	d.backend.SetLauncher(spawn)

	return &mrEnv{dataEnv: d, keys: keys, spawn: spawn}
}

func (m *mrEnv) revision(t *testing.T, kms string) string {
	t.Helper()

	rc := revisionConfig()
	in := &lwsdk.CreateWebFunctionRevisionInput{
		FunctionName: aws.String("app"), BuildConfig: rc.BuildConfig, ServiceConfig: rc.ServiceConfig,
	}

	if kms != "" {
		in.KmsKeyArn = aws.String(kms)
	}

	out, err := m.client.CreateWebFunctionRevision(t.Context(), in)
	require.NoError(t, err)
	m.settle()

	return aws.ToString(out.RevisionId)
}

func (m *mrEnv) endpoint(
	t *testing.T, typ types.EndpointType, rev string,
) (*lwsdk.CreateWebFunctionEndpointOutput, error) {
	t.Helper()

	out, err := m.client.CreateWebFunctionEndpoint(t.Context(), &lwsdk.CreateWebFunctionEndpointInput{
		FunctionName: aws.String("app"), EndpointName: aws.String("multi"), EndpointType: typ,
		AuthType: types.AuthTypeApplicationManaged, Regions: []string{mrHome, mrReplica},
		RevisionWeights: []types.RevisionWeight{{RevisionId: aws.String(rev), Weight: aws.Int32(100)}},
	})
	m.settle()

	return out, err
}

func (m *mrEnv) get(t *testing.T) *lwsdk.GetWebFunctionEndpointOutput {
	t.Helper()

	out, err := m.client.GetWebFunctionEndpoint(t.Context(), &lwsdk.GetWebFunctionEndpointInput{
		FunctionName: aws.String("app"), EndpointName: aws.String("multi"),
	})
	require.NoError(t, err)

	return out
}

func TestMultiRegion_ReplicationFailureFailsCreation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantState    types.EndpointState
		wantReplica  types.EndpointState
		wantServable int
		missing      bool
	}{
		{name: "replica present", wantState: types.EndpointStateActive, wantReplica: types.EndpointStateActive,
			wantServable: http.StatusOK},
		{name: "replica missing", missing: true, wantState: types.EndpointStateFailed,
			wantReplica: types.EndpointStateFailed, wantServable: http.StatusConflict},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			m := newMultiRegionEnv(t)
			m.keys.setMissing(mrReplica, tc.missing)

			rev := m.revision(t, mrKeyHome)
			_, err := m.endpoint(t, types.EndpointTypePerRegion, rev)
			require.NoError(t, err)

			got := m.get(t)
			assert.Equal(t, tc.wantState, got.State)
			assert.Equal(t, tc.wantReplica, got.RegionalEndpoints[mrReplica].State)
			assert.Equal(t, types.EndpointStateActive, got.RegionalEndpoints[mrHome].State)

			if tc.missing {
				reason := aws.ToString(got.RegionalEndpoints[mrReplica].StateReason)
				assert.Contains(t, reason, "not usable in "+mrReplica)
				assert.Contains(t, aws.ToString(got.StateReason), mrReplica)
			}

			host := aws.ToString(got.RegionalEndpoints[mrHome].DomainName)
			resp := m.do(t, http.MethodGet, host, "/", nil)
			assert.Equal(t, tc.wantServable, resp.StatusCode, "a failed endpoint is not invokable in any Region")
		})
	}
}

func TestMultiRegion_PerRegionServesFromItsOwnPool(t *testing.T) {
	t.Parallel()

	m := newMultiRegionEnv(t)
	rev := m.revision(t, "")
	_, err := m.endpoint(t, types.EndpointTypePerRegion, rev)
	require.NoError(t, err)

	got := m.get(t)
	home := aws.ToString(got.RegionalEndpoints[mrHome].DomainName)
	replica := aws.ToString(got.RegionalEndpoints[mrReplica].DomainName)
	require.NotEqual(t, home, replica)

	assert.Equal(t, http.StatusOK, m.do(t, http.MethodGet, home, "/", nil).StatusCode)
	assert.Equal(t, http.StatusOK, m.do(t, http.MethodGet, replica, "/", nil).StatusCode)
	assert.Equal(t, []string{mrHome, mrReplica}, m.spawn.served(), "each Region starts its own environment")
}

func TestMultiRegion_GlobalDomainRoutesToTheCallerRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		hint string
		want string
	}{
		{name: "no hint uses home", want: mrHome},
		{name: "target region hint", hint: mrReplica, want: mrReplica},
		{name: "non-target hint uses home", hint: "ap-south-1", want: mrHome},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			m := newMultiRegionEnv(t)
			rev := m.revision(t, "")
			_, err := m.endpoint(t, types.EndpointTypeMultiRegion, rev)
			require.NoError(t, err)

			global := aws.ToString(m.get(t).DomainName)
			resp := m.do(t, http.MethodGet, global, "/", func(r *http.Request) {
				if tc.hint != "" {
					r.Header.Set("X-Gopherstack-Caller-Region", tc.hint)
				}
			})
			require.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Equal(t, []string{tc.want}, m.spawn.served())
		})
	}
}

func TestMultiRegion_UpdateFailureKeepsThePreviousRevision(t *testing.T) {
	t.Parallel()

	m := newMultiRegionEnv(t)
	first := m.revision(t, mrKeyHome)
	_, err := m.endpoint(t, types.EndpointTypePerRegion, first)
	require.NoError(t, err)

	replicaHost := aws.ToString(m.get(t).RegionalEndpoints[mrReplica].DomainName)

	second := m.revision(t, mrKeyHome)
	m.keys.setMissing(mrReplica, true)

	update := func() {
		_, uerr := m.client.UpdateWebFunctionEndpoint(t.Context(), &lwsdk.UpdateWebFunctionEndpointInput{
			FunctionName: aws.String("app"), EndpointName: aws.String("multi"),
			RevisionWeights: []types.RevisionWeight{{RevisionId: aws.String(second), Weight: aws.Int32(100)}},
		})
		require.NoError(t, uerr)
		m.settle()
	}

	update()

	got := m.get(t)
	assert.Equal(t, types.EndpointStateActive, got.State)
	assert.Equal(t, types.EndpointUpdateStatusFailed, got.UpdateStatus)
	assert.Contains(t, aws.ToString(got.UpdateStatusReason), mrReplica)
	assert.Equal(t, types.EndpointUpdateStatusFailed, got.RegionalEndpoints[mrReplica].UpdateStatus)
	assert.Equal(t, types.EndpointUpdateStatusSuccessful, got.RegionalEndpoints[mrHome].UpdateStatus)
	assert.Equal(t, first, aws.ToString(got.RegionalEndpoints[mrReplica].RevisionWeights[0].RevisionId),
		"the failed Region keeps its last good deployment")
	assert.Equal(t, second, aws.ToString(got.RegionalEndpoints[mrHome].RevisionWeights[0].RevisionId))

	resp := m.do(t, http.MethodGet, replicaHost, "/", nil)
	assert.Equal(t, first, resp.Header.Get("X-App-Revision"), "the replica keeps serving the previous revision")

	m.keys.setMissing(mrReplica, false)
	_, err = m.client.UpdateWebFunctionEndpoint(t.Context(), &lwsdk.UpdateWebFunctionEndpointInput{
		FunctionName: aws.String("app"), EndpointName: aws.String("multi"),
	})
	require.NoError(t, err)
	m.settle()

	got = m.get(t)
	assert.Equal(t, types.EndpointUpdateStatusSuccessful, got.UpdateStatus, "an empty update retries the deployment")
	assert.Equal(t, second, aws.ToString(got.RegionalEndpoints[mrReplica].RevisionWeights[0].RevisionId))
}

func TestMultiRegion_ReplicasSurviveSnapshotRestore(t *testing.T) {
	t.Parallel()

	m := newMultiRegionEnv(t)
	rev := m.revision(t, "")
	_, err := m.endpoint(t, types.EndpointTypePerRegion, rev)
	require.NoError(t, err)

	_ = m.get(t)

	snap := m.backend.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	dst := newTestEnv(t)
	require.NoError(t, dst.backend.Restore(t.Context(), snap))

	got, err := dst.client.GetWebFunctionEndpoint(t.Context(), &lwsdk.GetWebFunctionEndpointInput{
		FunctionName: aws.String("app"), EndpointName: aws.String("multi"),
	})
	require.NoError(t, err)

	replica := got.RegionalEndpoints[mrReplica]
	assert.Equal(t, types.EndpointStateActive, replica.State)
	require.Len(t, replica.RevisionWeights, 1)
	assert.Equal(t, rev, aws.ToString(replica.RevisionWeights[0].RevisionId))
}
