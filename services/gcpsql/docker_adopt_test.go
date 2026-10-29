package gcpsql_test

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sqladmin "google.golang.org/api/sqladmin/v1"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

func resourceContainers(t *testing.T, name string) []string {
	t.Helper()

	out, err := exec.CommandContext(t.Context(), "docker", "ps", "-aq",
		"--filter", "label="+container.LabelResource+"="+project+"/"+name).Output()
	require.NoError(t, err)

	return strings.Fields(string(out))
}

// TestDockerOrphanAdoption crashes a backend (no Close) and restores a second one from its snapshot: the
// engine container must be adopted, not duplicated, and the data must survive.
func TestDockerOrphanAdoption(t *testing.T) {
	t.Parallel()

	const name = "dadopt"

	e := dockerEnv(t)
	ctx, cancel := context.WithTimeout(t.Context(), dockerWait)
	defer cancel()

	assertNoContainers(t, name)

	e.createInstance(t, name, "POSTGRES_16", nil)

	op, err := e.svc.Databases.Insert(project, name, &sqladmin.Database{Name: "app"}).Context(ctx).Do()
	e.mustWait(t, op, err)
	op, err = e.svc.Users.Insert(project, name, &sqladmin.User{Name: "alice", Host: "%", Password: "alice-pw"}).
		Context(ctx).Do()
	e.mustWait(t, op, err)

	db := openDB(t, "POSTGRES_16", e.endpoint(t, name), "alice", "alice-pw")
	_, err = db.ExecContext(ctx, "CREATE TABLE items (id int primary key)")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO items VALUES (1), (2)")
	require.NoError(t, err)

	before := resourceContainers(t, name)
	require.Len(t, before, 1)

	snap := e.backend.Snapshot(ctx)

	rt, err := container.NewRuntime(container.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rt.Close() })

	lister, ok := rt.(container.ManagedLister)
	require.True(t, ok)

	orphans, err := container.Reconcile(ctx, lister, container.CurrentIdentity(), nil)
	require.NoError(t, err)

	container.SetOrphans(orphans)
	t.Cleanup(func() { container.SetOrphans(nil) })

	second := newDockerHandler(t)
	t.Cleanup(second.Backend.Reset)
	t.Cleanup(second.Backend.Close)

	require.NoError(t, second.Backend.Restore(ctx, snap))

	require.Eventually(t, func() bool {
		in, getErr := second.Backend.GetInstance(project, name)

		return getErr == nil && in.State == "RUNNABLE" && in.GopherstackEndpoint != ""
	}, dockerWait, time.Second)

	assert.Equal(t, before, resourceContainers(t, name), "the crashed backend's container is reused, not duplicated")

	in, err := second.Backend.GetInstance(project, name)
	require.NoError(t, err)

	restored := openDB(t, "POSTGRES_16", in.GopherstackEndpoint, "alice", "alice-pw")
	assert.Equal(t, "2", scalar(t, restored, "SELECT count(*) FROM items"))
}
