package gcpmonitoring_test

import (
	"errors"
	"testing"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/genproto/googleapis/type/calendarperiod"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func (e *env) svcs(t *testing.T) *monitoring.ServiceMonitoringClient {
	t.Helper()

	c, err := monitoring.NewServiceMonitoringClient(t.Context(), e.opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	return c
}

func customService(display string) *monitoringpb.Service {
	return &monitoringpb.Service{
		DisplayName: display,
		Identifier:  &monitoringpb.Service_Custom_{Custom: &monitoringpb.Service_Custom{}},
	}
}

func (e *env) createService(t *testing.T, id string, s *monitoringpb.Service) *monitoringpb.Service {
	t.Helper()

	got, err := e.svcs(t).CreateService(t.Context(),
		&monitoringpb.CreateServiceRequest{Parent: proj, ServiceId: id, Service: s})
	require.NoError(t, err)

	return got
}

func availabilitySLO(display string, goal float64) *monitoringpb.ServiceLevelObjective {
	return &monitoringpb.ServiceLevelObjective{
		DisplayName: display,
		Goal:        goal,
		Period: &monitoringpb.ServiceLevelObjective_RollingPeriod{
			RollingPeriod: durationpb.New(7 * 24 * time.Hour),
		},
		ServiceLevelIndicator: &monitoringpb.ServiceLevelIndicator{
			Type: &monitoringpb.ServiceLevelIndicator_BasicSli{BasicSli: &monitoringpb.BasicSli{
				SliCriteria: &monitoringpb.BasicSli_Availability{
					Availability: &monitoringpb.BasicSli_AvailabilityCriteria{},
				},
			}},
		},
	}
}

func (e *env) createSLO(t *testing.T, parent, id string, s *monitoringpb.ServiceLevelObjective,
) *monitoringpb.ServiceLevelObjective {
	t.Helper()

	got, err := e.svcs(t).CreateServiceLevelObjective(t.Context(),
		&monitoringpb.CreateServiceLevelObjectiveRequest{
			Parent:                  parent,
			ServiceLevelObjectiveId: id,
			ServiceLevelObjective:   s,
		})
	require.NoError(t, err)

	return got
}

func TestServiceCreateGetDelete(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	s := e.createService(t, "checkout", &monitoringpb.Service{
		DisplayName: "Checkout",
		Identifier: &monitoringpb.Service_CloudRun_{
			CloudRun: &monitoringpb.Service_CloudRun{ServiceName: "co", Location: "us-central1"},
		},
		UserLabels: map[string]string{"team": "pay"},
	})
	assert.Equal(t, proj+"/services/checkout", s.GetName())

	got, err := e.svcs(t).GetService(t.Context(), &monitoringpb.GetServiceRequest{Name: s.GetName()})
	require.NoError(t, err)
	assert.Equal(t, "co", got.GetCloudRun().GetServiceName())
	assert.Equal(t, "pay", got.GetUserLabels()["team"])

	gen := e.createService(t, "", customService("generated"))
	assert.Regexp(t, `^projects/test-project/services/[a-z0-9-]+$`, gen.GetName())

	require.NoError(t, e.svcs(t).DeleteService(t.Context(), &monitoringpb.DeleteServiceRequest{Name: s.GetName()}))

	_, err = e.svcs(t).GetService(t.Context(), &monitoringpb.GetServiceRequest{Name: s.GetName()})
	assert.Equal(t, codes.NotFound, codeOf(err))
	assert.Equal(t, codes.NotFound,
		codeOf(e.svcs(t).DeleteService(t.Context(), &monitoringpb.DeleteServiceRequest{Name: s.GetName()})))
}

func TestServiceCreateErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createService(t, "dup", customService("d"))

	tests := []struct {
		svc    *monitoringpb.Service
		name   string
		parent string
		id     string
		code   codes.Code
	}{
		{customService("x"), "duplicate", proj, "dup", codes.AlreadyExists},
		{customService("x"), "uppercase id", proj, "Bad", codes.InvalidArgument},
		{customService("x"), "id with slash", proj, "a/b", codes.InvalidArgument},
		{customService("x"), "bad parent", "folders/1", "ok", codes.InvalidArgument},
		{&monitoringpb.Service{DisplayName: "x"}, "no identifier", proj, "ok", codes.InvalidArgument},
		{nil, "no service", proj, "ok", codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.svcs(t).CreateService(t.Context(),
				&monitoringpb.CreateServiceRequest{Parent: tt.parent, ServiceId: tt.id, Service: tt.svc})
			assert.Equal(t, tt.code, codeOf(err))
		})
	}
}

func TestServiceGetInvalidName(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	_, err := e.svcs(t).GetService(t.Context(), &monitoringpb.GetServiceRequest{Name: "services/x"})
	assert.Equal(t, codes.InvalidArgument, codeOf(err))
}

func TestServiceList(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createService(t, "a", customService("alpha"))
	e.createService(t, "b", &monitoringpb.Service{
		DisplayName: "beta",
		Identifier: &monitoringpb.Service_MeshIstio_{
			MeshIstio: &monitoringpb.Service_MeshIstio{MeshUid: "123", ServiceName: "s"},
		},
	})
	e.createService(t, "c", &monitoringpb.Service{
		DisplayName: "gamma",
		Identifier:  &monitoringpb.Service_AppEngine_{AppEngine: &monitoringpb.Service_AppEngine{ModuleId: "m"}},
	})

	tests := []struct {
		name   string
		filter string
		want   []string
		code   codes.Code
	}{
		{"all", "", []string{"alpha", "beta", "gamma"}, codes.OK},
		{"identifier case", `identifier_case = "CUSTOM"`, []string{"alpha"}, codes.OK},
		{"mesh uid", `mesh_istio.mesh_uid = "123"`, []string{"beta"}, codes.OK},
		{"mesh uid miss", `mesh_istio.mesh_uid = "999"`, nil, codes.OK},
		{"or", `identifier_case = "APP_ENGINE" OR identifier_case = "CUSTOM"`, []string{"alpha", "gamma"}, codes.OK},
		{"unknown field", `bogus = "x"`, nil, codes.InvalidArgument},
		{"unknown attribute", `mesh_istio.bogus = "x"`, nil, codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			it := e.svcs(t).
				ListServices(t.Context(), &monitoringpb.ListServicesRequest{Parent: proj, Filter: tt.filter})

			var got []string

			for {
				s, err := it.Next()
				if errors.Is(err, iterator.Done) {
					break
				}

				if tt.code != codes.OK {
					assert.Equal(t, tt.code, codeOf(err))

					return
				}

				require.NoError(t, err)

				got = append(got, s.GetDisplayName())
			}

			require.Equal(t, codes.OK, tt.code)
			assert.Equal(t, tt.want, got)
		})
	}

	t.Run("pagination", func(t *testing.T) {
		t.Parallel()

		it := e.svcs(t).ListServices(t.Context(), &monitoringpb.ListServicesRequest{Parent: proj, PageSize: 2})

		var names []string

		for {
			s, err := it.Next()
			if errors.Is(err, iterator.Done) {
				break
			}

			require.NoError(t, err)

			names = append(names, s.GetName())
		}

		assert.Equal(t, []string{proj + "/services/a", proj + "/services/b", proj + "/services/c"}, names)
	})
}

func TestServiceUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		want  string
		label string
		paths []string
		code  codes.Code
	}{
		{"display name only", "new", "keep", []string{"display_name"}, codes.OK},
		{"labels only", "old", "new", []string{"user_labels"}, codes.OK},
		{"empty mask", "new", "new", nil, codes.OK},
		{"identifier", "", "", []string{"custom"}, codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			s := e.createService(t, "svc", &monitoringpb.Service{
				DisplayName: "old", UserLabels: map[string]string{"l": "keep"},
				Identifier: &monitoringpb.Service_Custom_{Custom: &monitoringpb.Service_Custom{}},
			})
			s.DisplayName, s.UserLabels = "new", map[string]string{"l": "new"}

			got, err := e.svcs(t).UpdateService(t.Context(), &monitoringpb.UpdateServiceRequest{
				Service: s, UpdateMask: &fieldmaskpb.FieldMask{Paths: tt.paths},
			})
			require.Equal(t, tt.code, codeOf(err))

			if tt.code == codes.OK {
				assert.Equal(t, tt.want, got.GetDisplayName())
				assert.Equal(t, tt.label, got.GetUserLabels()["l"])
				assert.NotNil(t, got.GetCustom(), "identifier preserved")
			}
		})
	}

	t.Run("unknown service", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		s := customService("x")
		s.Name = proj + "/services/none"
		_, err := e.svcs(t).UpdateService(t.Context(), &monitoringpb.UpdateServiceRequest{Service: s})
		assert.Equal(t, codes.NotFound, codeOf(err))
	})
}

func TestSLOCreateGetDelete(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	svc := e.createService(t, "svc", customService("s")).GetName()

	slo := e.createSLO(t, svc, "avail", availabilitySLO("99%", 0.99))
	assert.Equal(t, svc+"/serviceLevelObjectives/avail", slo.GetName())

	gen := e.createSLO(t, svc, "", availabilitySLO("gen", 0.9))
	assert.Contains(t, gen.GetName(), svc+"/serviceLevelObjectives/")

	got, err := e.svcs(t).GetServiceLevelObjective(t.Context(),
		&monitoringpb.GetServiceLevelObjectiveRequest{Name: slo.GetName()})
	require.NoError(t, err)
	assert.InDelta(t, 0.99, got.GetGoal(), 1e-9)
	assert.NotNil(t, got.GetServiceLevelIndicator().GetBasicSli().GetAvailability())

	require.NoError(t, e.svcs(t).DeleteServiceLevelObjective(t.Context(),
		&monitoringpb.DeleteServiceLevelObjectiveRequest{Name: slo.GetName()}))

	_, err = e.svcs(t).GetServiceLevelObjective(t.Context(),
		&monitoringpb.GetServiceLevelObjectiveRequest{Name: slo.GetName()})
	assert.Equal(t, codes.NotFound, codeOf(err))
	assert.Equal(t, codes.NotFound, codeOf(e.svcs(t).DeleteServiceLevelObjective(t.Context(),
		&monitoringpb.DeleteServiceLevelObjectiveRequest{Name: slo.GetName()})))
}

func TestSLOCreateErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	svc := e.createService(t, "svc", customService("s")).GetName()
	e.createSLO(t, svc, "dup", availabilitySLO("d", 0.9))

	mut := func(f func(s *monitoringpb.ServiceLevelObjective)) *monitoringpb.ServiceLevelObjective {
		s := availabilitySLO("x", 0.9)
		f(s)

		return s
	}

	tests := []struct {
		slo    *monitoringpb.ServiceLevelObjective
		name   string
		parent string
		id     string
		code   codes.Code
	}{
		{availabilitySLO("x", 0.9), "duplicate", svc, "dup", codes.AlreadyExists},
		{availabilitySLO("x", 0.9), "missing service", proj + "/services/none", "a", codes.NotFound},
		{availabilitySLO("x", 0.9), "bad id", svc, "bad id", codes.InvalidArgument},
		{availabilitySLO("x", 0), "goal zero", svc, "a", codes.InvalidArgument},
		{availabilitySLO("x", 1), "goal one", svc, "a", codes.InvalidArgument},
		{
			mut(func(s *monitoringpb.ServiceLevelObjective) { s.Period = nil }),
			"no period",
			svc,
			"a",
			codes.InvalidArgument,
		},
		{mut(func(s *monitoringpb.ServiceLevelObjective) {
			s.Period = &monitoringpb.ServiceLevelObjective_RollingPeriod{
				RollingPeriod: durationpb.New(31 * 24 * time.Hour),
			}
		}), "rolling too long", svc, "a", codes.InvalidArgument},
		{mut(func(s *monitoringpb.ServiceLevelObjective) {
			s.Period = &monitoringpb.ServiceLevelObjective_RollingPeriod{RollingPeriod: durationpb.New(36 * time.Hour)}
		}), "rolling not whole days", svc, "a", codes.InvalidArgument},
		{mut(func(s *monitoringpb.ServiceLevelObjective) {
			s.Period = &monitoringpb.ServiceLevelObjective_CalendarPeriod{
				CalendarPeriod: calendarperiod.CalendarPeriod_YEAR,
			}
		}), "calendar year", svc, "a", codes.InvalidArgument},
		{
			mut(func(s *monitoringpb.ServiceLevelObjective) { s.ServiceLevelIndicator = nil }),
			"no sli",
			svc,
			"a",
			codes.InvalidArgument,
		},
		{mut(func(s *monitoringpb.ServiceLevelObjective) {
			s.ServiceLevelIndicator = &monitoringpb.ServiceLevelIndicator{
				Type: &monitoringpb.ServiceLevelIndicator_RequestBased{
					RequestBased: &monitoringpb.RequestBasedSli{Method: &monitoringpb.RequestBasedSli_GoodTotalRatio{
						GoodTotalRatio: &monitoringpb.TimeSeriesRatio{GoodServiceFilter: `metric.type = "a"`},
					}},
				},
			}
		}), "ratio with one filter", svc, "a", codes.InvalidArgument},
		{mut(func(s *monitoringpb.ServiceLevelObjective) {
			s.ServiceLevelIndicator = &monitoringpb.ServiceLevelIndicator{
				Type: &monitoringpb.ServiceLevelIndicator_WindowsBased{
					WindowsBased: &monitoringpb.WindowsBasedSli{
						WindowPeriod: durationpb.New(7 * time.Second),
						WindowCriterion: &monitoringpb.WindowsBasedSli_GoodBadMetricFilter{
							GoodBadMetricFilter: `metric.type = "a"`,
						},
					},
				},
			}
		}), "window too short", svc, "a", codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.svcs(t).
				CreateServiceLevelObjective(t.Context(), &monitoringpb.CreateServiceLevelObjectiveRequest{
					Parent: tt.parent, ServiceLevelObjectiveId: tt.id, ServiceLevelObjective: tt.slo,
				})
			assert.Equal(t, tt.code, codeOf(err))
		})
	}
}

func TestSLOSliKinds(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	svc := e.createService(t, "svc", customService("s")).GetName()
	const f = `metric.type = "custom.googleapis.com/x"`

	tests := []struct {
		sli  *monitoringpb.ServiceLevelIndicator
		name string
	}{
		{
			&monitoringpb.ServiceLevelIndicator{
				Type: &monitoringpb.ServiceLevelIndicator_BasicSli{BasicSli: &monitoringpb.BasicSli{
					SliCriteria: &monitoringpb.BasicSli_Latency{
						Latency: &monitoringpb.BasicSli_LatencyCriteria{Threshold: durationpb.New(time.Second)},
					},
				}},
			},
			"latency",
		},
		{
			&monitoringpb.ServiceLevelIndicator{
				Type: &monitoringpb.ServiceLevelIndicator_RequestBased{RequestBased: &monitoringpb.RequestBasedSli{
					Method: &monitoringpb.RequestBasedSli_GoodTotalRatio{GoodTotalRatio: &monitoringpb.TimeSeriesRatio{
						GoodServiceFilter: f, TotalServiceFilter: f,
					}},
				}},
			},
			"good total ratio",
		},
		{
			&monitoringpb.ServiceLevelIndicator{
				Type: &monitoringpb.ServiceLevelIndicator_RequestBased{RequestBased: &monitoringpb.RequestBasedSli{
					Method: &monitoringpb.RequestBasedSli_DistributionCut{
						DistributionCut: &monitoringpb.DistributionCut{
							DistributionFilter: f, Range: &monitoringpb.Range{Max: 5},
						},
					},
				}},
			},
			"distribution cut",
		},
		{
			&monitoringpb.ServiceLevelIndicator{
				Type: &monitoringpb.ServiceLevelIndicator_WindowsBased{WindowsBased: &monitoringpb.WindowsBasedSli{
					WindowPeriod: durationpb.New(time.Minute),
					WindowCriterion: &monitoringpb.WindowsBasedSli_MetricMeanInRange{
						MetricMeanInRange: &monitoringpb.WindowsBasedSli_MetricRange{
							TimeSeries: f, Range: &monitoringpb.Range{Max: 5},
						},
					},
				}},
			},
			"windows mean in range",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := availabilitySLO("x", 0.9)
			s.ServiceLevelIndicator = tt.sli
			s.Period = &monitoringpb.ServiceLevelObjective_CalendarPeriod{
				CalendarPeriod: calendarperiod.CalendarPeriod_WEEK,
			}

			created := e.createSLO(t, svc, "", s)
			got, err := e.svcs(t).GetServiceLevelObjective(t.Context(),
				&monitoringpb.GetServiceLevelObjectiveRequest{Name: created.GetName()})
			require.NoError(t, err)
			assert.Equal(t, calendarperiod.CalendarPeriod_WEEK, got.GetCalendarPeriod())
			assert.Equal(
				t,
				tt.sli.ProtoReflect().WhichOneof(tt.sli.ProtoReflect().Descriptor().Oneofs().ByName("type")).Name(),
				got.GetServiceLevelIndicator().ProtoReflect().WhichOneof(
					got.GetServiceLevelIndicator().ProtoReflect().Descriptor().Oneofs().ByName("type")).Name(),
			)
		})
	}
}

func TestSLOList(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	s1 := e.createService(t, "one", customService("1")).GetName()
	s2 := e.createService(t, "two", customService("2")).GetName()
	e.createSLO(t, s1, "a", availabilitySLO("alpha", 0.9))
	e.createSLO(t, s1, "b", availabilitySLO("beta", 0.9))
	e.createSLO(t, s2, "c", availabilitySLO("alpha", 0.95))

	tests := []struct {
		name   string
		parent string
		filter string
		want   []string
		code   codes.Code
	}{
		{"service one", s1, "", []string{"a", "b"}, codes.OK},
		{"service two", s2, "", []string{"c"}, codes.OK},
		{"wildcard", proj + "/services/-", "", []string{"a", "b", "c"}, codes.OK},
		{"filter display name", proj + "/services/-", `display_name = "alpha"`, []string{"a", "c"}, codes.OK},
		{"missing service", proj + "/services/none", "", nil, codes.NotFound},
		{"bad filter", s1, `goal > 1`, nil, codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			it := e.svcs(t).ListServiceLevelObjectives(t.Context(),
				&monitoringpb.ListServiceLevelObjectivesRequest{Parent: tt.parent, Filter: tt.filter, PageSize: 2})

			var got []string

			for {
				s, err := it.Next()
				if errors.Is(err, iterator.Done) {
					break
				}

				if tt.code != codes.OK {
					assert.Equal(t, tt.code, codeOf(err))

					return
				}

				require.NoError(t, err)

				got = append(got, s.GetName()[len(s.GetName())-1:])
			}

			require.Equal(t, codes.OK, tt.code)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSLOUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		edit  func(s *monitoringpb.ServiceLevelObjective)
		check func(t *testing.T, got *monitoringpb.ServiceLevelObjective)
		name  string
		paths []string
		code  codes.Code
	}{
		{
			name: "goal", paths: []string{"goal"},
			edit: func(s *monitoringpb.ServiceLevelObjective) { s.Goal, s.DisplayName = 0.5, "ignored" },
			check: func(t *testing.T, got *monitoringpb.ServiceLevelObjective) {
				t.Helper()
				assert.InDelta(t, 0.5, got.GetGoal(), 1e-9)
				assert.Equal(t, "orig", got.GetDisplayName())
			},
		},
		{
			name: "calendar period", paths: []string{"calendar_period"},
			edit: func(s *monitoringpb.ServiceLevelObjective) {
				s.Period = &monitoringpb.ServiceLevelObjective_CalendarPeriod{
					CalendarPeriod: calendarperiod.CalendarPeriod_MONTH,
				}
			},
			check: func(t *testing.T, got *monitoringpb.ServiceLevelObjective) {
				t.Helper()
				assert.Equal(t, calendarperiod.CalendarPeriod_MONTH, got.GetCalendarPeriod())
				assert.Nil(t, got.GetRollingPeriod())
			},
		},
		{
			name: "empty mask replaces", paths: nil,
			edit: func(s *monitoringpb.ServiceLevelObjective) { s.DisplayName, s.Goal = "all", 0.8 },
			check: func(t *testing.T, got *monitoringpb.ServiceLevelObjective) {
				t.Helper()
				assert.Equal(t, "all", got.GetDisplayName())
				assert.InDelta(t, 0.8, got.GetGoal(), 1e-9)
			},
		},
		{
			name: "invalid goal", paths: []string{"goal"},
			edit: func(s *monitoringpb.ServiceLevelObjective) { s.Goal = 2 },
			code: codes.InvalidArgument,
		},
		{
			name: "unknown path", paths: []string{"name"},
			edit: func(*monitoringpb.ServiceLevelObjective) {},
			code: codes.InvalidArgument,
		},
		{
			name: "unknown slo", paths: []string{"goal"},
			edit: func(s *monitoringpb.ServiceLevelObjective) { s.Name += "x" },
			code: codes.NotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			svc := e.createService(t, "svc", customService("s")).GetName()
			slo := e.createSLO(t, svc, "o", availabilitySLO("orig", 0.9))
			tt.edit(slo)

			got, err := e.svcs(t).
				UpdateServiceLevelObjective(t.Context(), &monitoringpb.UpdateServiceLevelObjectiveRequest{
					ServiceLevelObjective: slo, UpdateMask: &fieldmaskpb.FieldMask{Paths: tt.paths},
				})
			require.Equal(t, tt.code, codeOf(err))

			if tt.check != nil {
				tt.check(t, got)
			}
		})
	}
}

func TestDeleteServiceRemovesSLOsAndPersists(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	keep := src.createService(t, "keep", &monitoringpb.Service{
		DisplayName: "keep", Identifier: &monitoringpb.Service_Custom_{Custom: &monitoringpb.Service_Custom{}},
		UserLabels: map[string]string{"a": "b"},
	}).GetName()
	gone := src.createService(t, "gone", customService("gone")).GetName()
	slo := src.createSLO(t, keep, "o", availabilitySLO("o", 0.9)).GetName()
	src.createSLO(t, gone, "o", availabilitySLO("o", 0.9))
	require.NoError(t, src.svcs(t).DeleteService(t.Context(), &monitoringpb.DeleteServiceRequest{Name: gone}))

	dst := newEnv(t)
	require.NoError(t, dst.handler.Backend.Restore(t.Context(), src.handler.Backend.Snapshot(t.Context())))

	svc, err := dst.svcs(t).GetService(t.Context(), &monitoringpb.GetServiceRequest{Name: keep})
	require.NoError(t, err)
	assert.Equal(t, "b", svc.GetUserLabels()["a"])

	_, err = dst.svcs(t).GetServiceLevelObjective(t.Context(), &monitoringpb.GetServiceLevelObjectiveRequest{Name: slo})
	require.NoError(t, err)

	it := dst.svcs(t).ListServiceLevelObjectives(t.Context(),
		&monitoringpb.ListServiceLevelObjectivesRequest{Parent: proj + "/services/-"})
	_, err = it.Next()
	require.NoError(t, err)
	_, err = it.Next()
	assert.Equal(t, iterator.Done, err, "the deleted service's objective is gone")

	dst.handler.Reset()

	_, err = dst.svcs(t).GetService(t.Context(), &monitoringpb.GetServiceRequest{Name: keep})
	assert.Equal(t, codes.NotFound, codeOf(err))
}
