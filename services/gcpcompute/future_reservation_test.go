package gcpcompute_test

import (
	"net/http"
	"testing"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/proto"
)

func frBody(name string) *computepb.FutureReservation {
	return &computepb.FutureReservation{
		Name: s(name),
		SpecificSkuProperties: &computepb.FutureReservationSpecificSKUProperties{
			TotalCount: proto.Int64(3),
			InstanceProperties: &computepb.AllocationSpecificSKUAllocationReservedInstanceProperties{
				MachineType: s("e2-medium"),
			},
		},
		TimeWindow: &computepb.FutureReservationTimeWindow{
			StartTime: s("2027-01-01T00:00:00Z"), EndTime: s("2027-02-01T00:00:00Z"),
		},
	}
}

func (e *env) futures(t *testing.T) *compute.FutureReservationsClient {
	t.Helper()

	return client(t, e, compute.NewFutureReservationsRESTClient)
}

func (e *env) futureInsert(t *testing.T, fr *computepb.FutureReservation) error {
	t.Helper()

	op, err := e.futures(t).Insert(t.Context(), &computepb.InsertFutureReservationRequest{
		Project: project, Zone: zone, FutureReservationResource: fr,
	})

	return e.finish(t, op, err)
}

func (e *env) futureGet(t *testing.T, name string) *computepb.FutureReservation {
	t.Helper()

	got, err := e.futures(t).Get(t.Context(), &computepb.GetFutureReservationRequest{
		Project: project, Zone: zone, FutureReservation: name,
	})
	require.NoError(t, err)

	return got
}

func TestFutureReservationValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mod  func(fr *computepb.FutureReservation)
		name string
		code int
	}{
		{func(fr *computepb.FutureReservation) { fr.TimeWindow = nil }, "no window", http.StatusBadRequest},
		{
			func(fr *computepb.FutureReservation) { fr.TimeWindow.StartTime = s("tomorrow") },
			"bad start",
			http.StatusBadRequest,
		},
		{
			func(fr *computepb.FutureReservation) { fr.TimeWindow.EndTime = s("2026-12-01T00:00:00Z") },
			"end before start",
			http.StatusBadRequest,
		},
		{func(fr *computepb.FutureReservation) { fr.TimeWindow.EndTime = nil }, "no end", http.StatusBadRequest},
		{func(fr *computepb.FutureReservation) {
			fr.TimeWindow.Duration = &computepb.Duration{Seconds: proto.Int64(60)}
		}, "end and duration", http.StatusBadRequest},
		{func(fr *computepb.FutureReservation) { fr.SpecificSkuProperties = nil }, "no sku", http.StatusBadRequest},
		{
			func(fr *computepb.FutureReservation) { fr.SpecificSkuProperties.TotalCount = proto.Int64(0) },
			"no count",
			http.StatusBadRequest,
		},
		{func(fr *computepb.FutureReservation) {
			fr.SpecificSkuProperties.InstanceProperties.MachineType = s("nope-1")
		}, "unknown machine", http.StatusNotFound},
		{func(fr *computepb.FutureReservation) {
			fr.SpecificSkuProperties.InstanceProperties = nil
			fr.SpecificSkuProperties.SourceInstanceTemplate = s("global/instanceTemplates/ghost")
		}, "unknown template", http.StatusNotFound},
		{func(fr *computepb.FutureReservation) { fr.ReservationMode = s("OTHER") }, "bad mode", http.StatusBadRequest},
		{
			func(fr *computepb.FutureReservation) { fr.NamePrefix = s("this-prefix-is-far-too-long") },
			"long prefix",
			http.StatusBadRequest,
		},
		{func(*computepb.FutureReservation) {}, "valid", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			fr := frBody("fr")
			tt.mod(fr)

			err := e.futureInsert(t, fr)
			if tt.code == http.StatusOK {
				require.NoError(t, err)

				return
			}

			assertAPIError(t, err, tt.code, "")
		})
	}
}

func TestFutureReservationLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	require.NoError(t, e.futureInsert(t, frBody("fr")))
	assertAPIError(t, e.futureInsert(t, frBody("fr")), http.StatusConflict, "alreadyExists")

	draft := frBody("draft")
	draft.PlanningStatus = s("DRAFT")
	require.NoError(t, e.futureInsert(t, draft))

	got := e.futureGet(t, "fr")
	assert.Equal(t, "APPROVED", got.GetStatus().GetProcurementStatus())
	assert.Contains(t, got.GetSelfLink(), "/zones/"+zone+"/futureReservations/fr")
	assert.Contains(t, got.GetZone(), "/zones/"+zone)
	assert.Equal(t, "DRAFTING", e.futureGet(t, "draft").GetStatus().GetProcurementStatus())

	update := func(name string, fr *computepb.FutureReservation, mask string) error {
		op, err := e.futures(t).Update(t.Context(), &computepb.UpdateFutureReservationRequest{
			Project: project, Zone: zone, FutureReservation: name, FutureReservationResource: fr, UpdateMask: s(mask),
		})

		return e.finish(t, op, err)
	}

	require.NoError(t, update("fr", &computepb.FutureReservation{Description: s("d")}, "description"))

	after := e.futureGet(t, "fr")
	assert.Equal(t, "d", after.GetDescription())
	assert.Equal(t, int64(3), after.GetSpecificSkuProperties().GetTotalCount())
	assert.Equal(t, "AMENDMENT_APPROVED", after.GetStatus().GetAmendmentStatus())

	require.NoError(t, update("draft", &computepb.FutureReservation{PlanningStatus: s("SUBMITTED")}, "planningStatus"))
	assert.Equal(t, "APPROVED", e.futureGet(t, "draft").GetStatus().GetProcurementStatus())
	assertAPIError(t, update("draft", &computepb.FutureReservation{PlanningStatus: s("DRAFT")}, "planningStatus"),
		http.StatusBadRequest, "invalid")
	assertAPIError(t, update("fr", &computepb.FutureReservation{
		SpecificSkuProperties: &computepb.FutureReservationSpecificSKUProperties{TotalCount: proto.Int64(-1)},
	}, "specificSkuProperties"), http.StatusBadRequest, "invalid")
	assertAPIError(t, update("ghost", &computepb.FutureReservation{}, "description"), http.StatusNotFound, "notFound")

	op, err := e.futures(t).Cancel(t.Context(), &computepb.CancelFutureReservationRequest{
		Project: project, Zone: zone, FutureReservation: "fr",
	})
	require.NoError(t, e.finish(t, op, err))
	assert.Equal(t, "CANCELLED", e.futureGet(t, "fr").GetStatus().GetProcurementStatus())

	_, err = e.futures(t).Cancel(t.Context(), &computepb.CancelFutureReservationRequest{
		Project: project, Zone: zone, FutureReservation: "fr",
	})
	assertAPIError(t, err, http.StatusBadRequest, "invalid")
	assertAPIError(t, update("fr", &computepb.FutureReservation{Description: s("x")}, "description"),
		http.StatusBadRequest, "invalid")

	agg := map[string]int{}
	it := e.futures(t).AggregatedList(t.Context(), &computepb.AggregatedListFutureReservationsRequest{Project: project})

	for {
		kv, nerr := it.Next()
		if nerr != nil {
			require.ErrorIs(t, nerr, iterator.Done)

			break
		}

		agg[kv.Key] += len(kv.Value.GetFutureReservations())
	}

	assert.Equal(t, 2, agg["zones/"+zone])

	op, err = e.futures(t).Delete(t.Context(), &computepb.DeleteFutureReservationRequest{
		Project: project, Zone: zone, FutureReservation: "fr",
	})
	require.NoError(t, e.finish(t, op, err))

	_, err = e.futures(t).Get(t.Context(), &computepb.GetFutureReservationRequest{
		Project: project, Zone: zone, FutureReservation: "fr",
	})
	assertAPIError(t, err, http.StatusNotFound, "notFound")
}

func TestFutureReservationRestore(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	require.NoError(t, src.futureInsert(t, frBody("fr")))

	dst := newEnv(t)
	require.NoError(t, dst.backend.Restore(t.Context(), src.backend.Snapshot(t.Context())))
	assert.Equal(t, "APPROVED", dst.futureGet(t, "fr").GetStatus().GetProcurementStatus())
}
