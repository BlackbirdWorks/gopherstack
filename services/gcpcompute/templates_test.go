package gcpcompute_test

import (
	"net/http"
	"testing"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
)

func TestInstanceTemplates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		regional bool
	}{
		{name: "global"},
		{name: "regional", regional: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)

			insert := func(spec *computepb.InstanceTemplate) error {
				if tt.regional {
					op, err := e.regionTemplates(t).Insert(t.Context(), &computepb.InsertRegionInstanceTemplateRequest{
						Project: project, Region: region, InstanceTemplateResource: spec,
					})
					if err == nil {
						e.settle()
						err = op.Wait(t.Context())
					}

					return err
				}

				op, err := e.templates(t).Insert(t.Context(),
					&computepb.InsertInstanceTemplateRequest{Project: project, InstanceTemplateResource: spec})
				if err == nil {
					e.settle()
					err = op.Wait(t.Context())
				}

				return err
			}

			get := func(name string) (*computepb.InstanceTemplate, error) {
				if tt.regional {
					return e.regionTemplates(t).Get(t.Context(),
						&computepb.GetRegionInstanceTemplateRequest{Project: project, Region: region, InstanceTemplate: name})
				}

				return e.templates(t).Get(t.Context(),
					&computepb.GetInstanceTemplateRequest{Project: project, InstanceTemplate: name})
			}

			require.NoError(t, insert(tmplSpec("tpl")))

			got, err := get("tpl")
			require.NoError(t, err)
			assert.Equal(t, "tpl", got.GetName())
			assert.Equal(t, "e2-medium", got.GetProperties().GetMachineType())
			assert.Equal(t, "test", got.GetProperties().GetLabels()["env"])
			assert.NotEmpty(t, got.GetProperties().GetMetadata().GetFingerprint())
			assert.Equal(t, "PERSISTENT", got.GetProperties().GetDisks()[0].GetType())
			assert.Equal(t, tt.regional, got.GetRegion() != "")

			if tt.regional {
				assert.Contains(t, got.GetSelfLink(), "/regions/"+region+"/instanceTemplates/tpl")
			} else {
				assert.Contains(t, got.GetSelfLink(), "/global/instanceTemplates/tpl")
			}

			bad := tmplSpec("nomachine")
			bad.Properties.MachineType = nil
			nodisk := tmplSpec("nodisk")
			nodisk.Properties.Disks = nil

			for _, c := range []struct {
				spec   *computepb.InstanceTemplate
				reason string
				status int
			}{
				{spec: tmplSpec("tpl"), status: http.StatusConflict, reason: "alreadyExists"},
				{spec: bad, status: http.StatusBadRequest, reason: "required"},
				{spec: nodisk, status: http.StatusBadRequest, reason: "required"},
				{spec: tmplSpec("Bad_Name"), status: http.StatusBadRequest, reason: "invalid"},
			} {
				assertAPIError(t, insert(c.spec), c.status, c.reason)
			}

			_, err = get("missing")
			assertAPIError(t, err, http.StatusNotFound, "notFound")
		})
	}
}

func TestInstanceTemplateListAndDelete(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.template(t, "a")
	e.template(t, "b")

	op, err := e.regionTemplates(t).Insert(t.Context(), &computepb.InsertRegionInstanceTemplateRequest{
		Project: project, Region: region, InstanceTemplateResource: tmplSpec("r1"),
	})
	e.done(t, op, err)

	names := func(filter string) []string {
		it := e.templates(t).List(t.Context(), &computepb.ListInstanceTemplatesRequest{Project: project, Filter: &filter})

		var out []string

		for {
			tp, nerr := it.Next()
			if nerr == iterator.Done {
				return out
			}

			require.NoError(t, nerr)

			out = append(out, tp.GetName())
		}
	}

	assert.Equal(t, []string{"a", "b"}, names(""))
	assert.Equal(t, []string{"b"}, names("name = b"))

	agg := e.templates(t).AggregatedList(t.Context(), &computepb.AggregatedListInstanceTemplatesRequest{Project: project})
	total := 0

	for {
		pair, nerr := agg.Next()
		if nerr == iterator.Done {
			break
		}

		require.NoError(t, nerr)

		total += len(pair.Value.GetInstanceTemplates())
	}

	assert.Equal(t, 3, total)

	op, err = e.templates(t).Delete(t.Context(), &computepb.DeleteInstanceTemplateRequest{Project: project, InstanceTemplate: "a"})
	e.done(t, op, err)

	_, err = e.templates(t).Get(t.Context(), &computepb.GetInstanceTemplateRequest{Project: project, InstanceTemplate: "a"})
	assertAPIError(t, err, http.StatusNotFound, "notFound")
	assert.Equal(t, []string{"b"}, names(""))
}

func TestInsertInstanceFromTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		override *computepb.Instance
		wantEnv  string
		wantMach string
		wantErr  int
	}{
		{name: "defaults", override: &computepb.Instance{Name: s("from-tpl")}, wantEnv: "test", wantMach: "e2-medium"},
		{
			name: "override", wantEnv: "prod", wantMach: "e2-small",
			override: &computepb.Instance{
				Name: s("from-tpl"), Labels: map[string]string{"env": "prod"},
				MachineType: s("zones/" + zone + "/machineTypes/e2-small"),
			},
		},
		{name: "no name", override: &computepb.Instance{}, wantErr: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)
			tmpl := e.template(t, "tpl")

			op, err := e.instances(t).Insert(t.Context(), &computepb.InsertInstanceRequest{
				Project: project, Zone: zone, InstanceResource: tt.override, SourceInstanceTemplate: s(tmpl),
			})
			if tt.wantErr != 0 {
				assertAPIError(t, err, tt.wantErr, "")

				return
			}

			e.done(t, op, err)

			vm := e.getVM(t, "from-tpl")
			assert.Equal(t, tt.wantEnv, vm.GetLabels()["env"])
			assert.Equal(t, tt.wantMach, lastPart(vm.GetMachineType()))
			assert.Equal(t, []string{"web"}, vm.GetTags().GetItems())
			require.Len(t, vm.GetDisks(), 1)
			assert.True(t, vm.GetDisks()[0].GetBoot())
		})
	}

	t.Run("unknown template", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		_, err := e.instances(t).Insert(t.Context(), &computepb.InsertInstanceRequest{
			Project: project, Zone: zone, InstanceResource: &computepb.Instance{Name: s("x")},
			SourceInstanceTemplate: s(globalTmplPath + "none"),
		})
		assertAPIError(t, err, http.StatusNotFound, "notFound")
	})
}

func lastPart(s string) string {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '/' {
			return s[i+1:]
		}
	}

	return s
}
