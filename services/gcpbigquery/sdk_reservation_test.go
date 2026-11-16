package gcpbigquery_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	res "google.golang.org/api/bigqueryreservation/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

const resParent = "projects/test-project/locations/US"

func (s *sdkEnv) reservations(t *testing.T) *res.Service {
	t.Helper()

	svc, err := res.NewService(t.Context(), option.WithEndpoint(s.url), option.WithoutAuthentication())
	require.NoError(t, err)

	return svc
}

func apiCode(t *testing.T, err error) int {
	t.Helper()

	var gerr *googleapi.Error

	require.ErrorAs(t, err, &gerr)

	return gerr.Code
}

func TestReservationsLifecycle(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	svc := env.reservations(t)
	ctx := t.Context()
	pr := svc.Projects.Locations.Reservations

	base := &res.Reservation{SlotCapacity: 100, IgnoreIdleSlots: true, Edition: "ENTERPRISE"}
	created, err := pr.Create(resParent, base).ReservationId("team1").Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, resParent+"/reservations/team1", created.Name)
	assert.Equal(t, int64(100), created.SlotCapacity)
	assert.NotEmpty(t, created.CreationTime)

	_, err = pr.Create(resParent, &res.Reservation{}).ReservationId("team1").Context(ctx).Do()
	assert.Equal(t, http.StatusConflict, apiCode(t, err))

	for name, r := range map[string]struct {
		rsv *res.Reservation
		id  string
	}{
		"bad id":        {&res.Reservation{}, "Team"},
		"trailing dash": {&res.Reservation{}, "team-"},
		"mode no max":   {&res.Reservation{ScalingMode: "ALL_SLOTS"}, "a1"},
		"max no mode":   {&res.Reservation{MaxSlots: 500}, "a2"},
		"autoscale ign": {&res.Reservation{MaxSlots: 500, ScalingMode: "AUTOSCALE_ONLY"}, "a3"},
		"capacity max":  {&res.Reservation{SlotCapacity: 500, MaxSlots: 500, ScalingMode: "ALL_SLOTS"}, "a4"},
		"edition":       {&res.Reservation{Edition: "GOLD"}, "a5"},
	} {
		_, cerr := pr.Create(resParent, r.rsv).ReservationId(r.id).Context(ctx).Do()
		assert.Equal(t, http.StatusBadRequest, apiCode(t, cerr), name)
	}

	patch := &res.Reservation{SlotCapacity: 200}
	upd, err := pr.Patch(created.Name, patch).UpdateMask("slot_capacity").Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, int64(200), upd.SlotCapacity)
	assert.True(t, upd.IgnoreIdleSlots)

	list, err := pr.List(resParent).Context(ctx).Do()
	require.NoError(t, err)
	require.Len(t, list.Reservations, 1)

	first := &res.Assignment{Assignee: "projects/p1", JobType: "QUERY"}
	asg, err := pr.Assignments.Create(created.Name, first).Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, "ACTIVE", asg.State)

	_, err = pr.Create(resParent, &res.Reservation{}).ReservationId("team2").Context(ctx).Do()
	require.NoError(t, err)

	dup := &res.Assignment{Assignee: "projects/p1", JobType: "QUERY"}
	_, err = pr.Assignments.Create(resParent+"/reservations/team2", dup).Context(ctx).Do()
	assert.Equal(t, http.StatusConflict, apiCode(t, err))

	_, err = pr.Assignments.Create(created.Name, &res.Assignment{Assignee: "bad", JobType: "QUERY"}).Context(ctx).Do()
	assert.Equal(t, http.StatusBadRequest, apiCode(t, err))

	found, err := svc.Projects.Locations.SearchAssignments(resParent).Query("assignee=projects/p1").Context(ctx).Do()
	require.NoError(t, err)
	require.Len(t, found.Assignments, 1)

	_, err = pr.Delete(created.Name).Context(ctx).Do()
	assert.Equal(t, http.StatusBadRequest, apiCode(t, err))

	mv := &res.MoveAssignmentRequest{DestinationId: resParent + "/reservations/team2"}
	moved, err := pr.Assignments.Move(asg.Name, mv).Context(ctx).Do()
	require.NoError(t, err)
	assert.Contains(t, moved.Name, "/reservations/team2/assignments/")

	all, err := pr.Assignments.List(resParent + "/reservations/-").Context(ctx).Do()
	require.NoError(t, err)
	require.Len(t, all.Assignments, 1)

	_, err = pr.Assignments.Delete(moved.Name).Context(ctx).Do()
	require.NoError(t, err)

	_, err = pr.Delete(created.Name).Context(ctx).Do()
	require.NoError(t, err)

	_, err = pr.Delete(resParent + "/reservations/team2").Context(ctx).Do()
	require.NoError(t, err)

	_, err = pr.Get(created.Name).Context(ctx).Do()
	assert.Equal(t, http.StatusNotFound, apiCode(t, err))
}

func TestCapacityCommitments(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	svc := env.reservations(t)
	ctx := t.Context()
	cc := svc.Projects.Locations.CapacityCommitments

	one := &res.CapacityCommitment{Plan: "MONTHLY", SlotCount: 300}
	a, err := cc.Create(resParent, one).CapacityCommitmentId("c-one").Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, "ACTIVE", a.State)
	assert.NotEmpty(t, a.CommitmentEndTime)

	_, err = cc.Create(resParent, &res.CapacityCommitment{Plan: "MONTHLY", SlotCount: 150}).Context(ctx).Do()
	assert.Equal(t, http.StatusBadRequest, apiCode(t, err))

	_, err = cc.Create(resParent, &res.CapacityCommitment{Plan: "FOREVER", SlotCount: 100}).Context(ctx).Do()
	assert.Equal(t, http.StatusBadRequest, apiCode(t, err))

	b, err := cc.Create(resParent, &res.CapacityCommitment{Plan: "MONTHLY", SlotCount: 100}).Context(ctx).Do()
	require.NoError(t, err)

	merged, err := cc.Merge(resParent, &res.MergeCapacityCommitmentsRequest{
		CapacityCommitmentIds: []string{a.Name, b.Name},
	}).Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, int64(400), merged.SlotCount)

	_, err = cc.Get(a.Name).Context(ctx).Do()
	assert.Equal(t, http.StatusNotFound, apiCode(t, err))

	split, err := cc.Split(merged.Name, &res.SplitCapacityCommitmentRequest{SlotCount: 100}).Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, int64(100), split.First.SlotCount)
	assert.Equal(t, int64(300), split.Second.SlotCount)

	up, err := cc.Patch(split.First.Name, &res.CapacityCommitment{Plan: "ANNUAL"}).UpdateMask("plan").Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, "ANNUAL", up.Plan)

	_, err = cc.Patch(split.First.Name, &res.CapacityCommitment{Plan: "MONTHLY"}).UpdateMask("plan").Context(ctx).Do()
	assert.Equal(t, http.StatusBadRequest, apiCode(t, err))

	_, err = cc.Delete(split.Second.Name).Context(ctx).Do()
	assert.Equal(t, http.StatusBadRequest, apiCode(t, err))

	list, err := cc.List(resParent).Context(ctx).Do()
	require.NoError(t, err)
	assert.Len(t, list.CapacityCommitments, 2)
}

func TestReservationsBiAndPersistence(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	svc := env.reservations(t)
	ctx := t.Context()

	bi, err := svc.Projects.Locations.GetBiReservation(resParent + "/biReservation").Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, int64(0), bi.Size)

	bi, err = svc.Projects.Locations.UpdateBiReservation(resParent+"/biReservation", &res.BiReservation{Size: 1 << 30}).
		UpdateMask("size").Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, int64(1<<30), bi.Size)

	_, err = svc.Projects.Locations.Reservations.Create(resParent, &res.Reservation{SlotCapacity: 100}).
		ReservationId("keep").Context(ctx).Do()
	require.NoError(t, err)

	other := newSDK(t)
	require.NoError(t, other.backend.Restore(ctx, env.backend.Snapshot(ctx)))

	osvc := other.reservations(t)
	got, err := osvc.Projects.Locations.Reservations.Get(resParent + "/reservations/keep").Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, int64(100), got.SlotCapacity)

	bi, err = osvc.Projects.Locations.GetBiReservation(resParent + "/biReservation").Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, int64(1<<30), bi.Size)
}
