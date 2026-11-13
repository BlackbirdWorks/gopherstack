package gcpspanner_test

import (
	"testing"

	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestListInstancesFilter(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	for id, labels := range map[string]map[string]string{
		"howl-dev":  {"env": "dev"},
		"howl-prod": {"env": "production"},
		"other":     nil,
	} {
		op, err := e.instances.CreateInstance(t.Context(), &instancepb.CreateInstanceRequest{
			Parent: parent, InstanceId: id,
			Instance: &instancepb.Instance{
				Config: configRef, DisplayName: "Disp " + id, NodeCount: 1, Labels: labels,
			},
		})
		require.NoError(t, err)

		_, err = await(t, op)
		require.NoError(t, err)
	}

	tests := []struct {
		name   string
		filter string
		want   []string
		code   codes.Code
	}{
		{name: "name any", filter: "name:*", want: []string{"howl-dev", "howl-prod", "other"}},
		{name: "name contains", filter: "name:HOWL", want: []string{"howl-dev", "howl-prod"}},
		{name: "field case", filter: "NAME:howl", want: []string{"howl-dev", "howl-prod"}},
		{name: "label present", filter: "labels.env:*", want: []string{"howl-dev", "howl-prod"}},
		{name: "label value", filter: "labels.env:dev", want: []string{"howl-dev"}},
		{name: "implicit and", filter: "name:howl labels.env:prod", want: []string{"howl-prod"}},
		{name: "or", filter: "name:other OR labels.env:dev", want: []string{"howl-dev", "other"}},
		{name: "not", filter: "name:howl AND NOT labels.env:dev", want: []string{"howl-prod"}},
		{name: "display name", filter: `display_name:"disp other"`, want: []string{"other"}},
		{name: "missing label", filter: "labels.nope:*", want: nil},
		{name: "bad field", filter: "colour:red", code: codes.InvalidArgument},
		{name: "bad syntax", filter: "name:", code: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			it := e.instances.ListInstances(t.Context(), &instancepb.ListInstancesRequest{
				Parent: parent, Filter: tt.filter,
			})

			var got []string

			for {
				inst, err := it.Next()
				if err != nil {
					if tt.code != codes.OK {
						assert.Equal(t, tt.code, status.Code(err))

						return
					}

					require.Equal(t, "no more items in iterator", err.Error())

					break
				}

				got = append(got, inst.GetName()[len(parent+"/instances/"):])
			}

			require.Equal(t, codes.OK, tt.code)
			assert.Equal(t, tt.want, got)
		})
	}
}
