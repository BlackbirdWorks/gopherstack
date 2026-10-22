package gcpmonitoring_test

import (
	"errors"
	"testing"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/genproto/googleapis/api/monitoredres"
)

func (e *env) createGroup(t *testing.T, name, parent, filter string) *monitoringpb.Group {
	t.Helper()

	g, err := e.groups(t).CreateGroup(t.Context(), &monitoringpb.CreateGroupRequest{
		Name: proj, Group: &monitoringpb.Group{DisplayName: name, ParentName: parent, Filter: filter},
	})
	require.NoError(t, err)

	return g
}

func (e *env) members(t *testing.T, group, filter string) []string {
	t.Helper()

	it := e.groups(t).ListGroupMembers(t.Context(), &monitoringpb.ListGroupMembersRequest{Name: group, Filter: filter})

	var ids []string

	for {
		r, nextErr := it.Next()
		if errors.Is(nextErr, iterator.Done) {
			return ids
		}

		require.NoError(t, nextErr)

		ids = append(ids, r.GetLabels()["instance_id"])
	}
}

func TestGroupMembershipByFilter(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.writeAt(t, epoch(), "gold", map[string]float64{"inst-1": 1, "inst-2": 2})
	e.write(t, gaugeDouble(queueDepth, nil, &monitoredres.MonitoredResource{
		Type: "gce_instance", Labels: map[string]string{"instance_id": "inst-3", "zone": "europe-west1-b"},
	}, epoch(), 3))

	tests := []struct {
		name   string
		filter string
		extra  string
		want   []string
	}{
		{"by type", `resource.type = "gce_instance"`, "", []string{"inst-1", "inst-2", "inst-3"}},
		{"by zone", `resource.labels.zone = "` + zoneA + `"`, "", []string{"inst-1", "inst-2"}},
		{
			"starts with",
			`resource.labels.instance_id = starts_with("inst-")`,
			"",
			[]string{"inst-1", "inst-2", "inst-3"},
		},
		{
			"member filter",
			`resource.type = "gce_instance"`,
			`resource.labels.instance_id = "inst-2"`,
			[]string{"inst-2"},
		},
		{"no match", `resource.type = "cloud_function"`, "", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			g := e.createGroup(t, tt.name, "", tt.filter)
			assert.Equal(t, tt.want, e.members(t, g.GetName(), tt.extra))
		})
	}
}

func TestListTimeSeriesByGroup(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.writeAt(t, epoch(), "gold", map[string]float64{"inst-1": 1, "inst-2": 2})
	g := e.createGroup(t, "one", "", `resource.labels.instance_id = "inst-2"`)

	got := e.list(t, &monitoringpb.ListTimeSeriesRequest{
		Filter:   metricFilter + ` AND group.id = "` + g.GetName()[len(proj+"/groups/"):] + `"`,
		Interval: interval(epoch().Add(-time.Minute), epoch()),
	})
	require.Len(t, got, 1)
	assert.Equal(t, "inst-2", got[0].GetResource().GetLabels()["instance_id"])
}

func TestGroupHierarchy(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	c := e.groups(t)
	root := e.createGroup(t, "root", "", `resource.type = "gce_instance"`)
	child := e.createGroup(t, "child", root.GetName(), `resource.labels.zone = "`+zoneA+`"`)
	leaf := e.createGroup(t, "leaf", child.GetName(), `resource.labels.instance_id = "i"`)

	names := func(req *monitoringpb.ListGroupsRequest) []string {
		req.Name = proj
		it := c.ListGroups(t.Context(), req)

		var out []string

		for {
			g, nextErr := it.Next()
			if errors.Is(nextErr, iterator.Done) {
				return out
			}

			require.NoError(t, nextErr)

			out = append(out, g.GetDisplayName())
		}
	}

	assert.ElementsMatch(t, []string{"root", "child", "leaf"}, names(&monitoringpb.ListGroupsRequest{}))
	assert.Equal(t, []string{"child"}, names(&monitoringpb.ListGroupsRequest{
		Filter: &monitoringpb.ListGroupsRequest_ChildrenOfGroup{ChildrenOfGroup: root.GetName()},
	}))
	assert.ElementsMatch(t, []string{"child", "leaf"}, names(&monitoringpb.ListGroupsRequest{
		Filter: &monitoringpb.ListGroupsRequest_DescendantsOfGroup{DescendantsOfGroup: root.GetName()},
	}))
	assert.ElementsMatch(t, []string{"child", "root"}, names(&monitoringpb.ListGroupsRequest{
		Filter: &monitoringpb.ListGroupsRequest_AncestorsOfGroup{AncestorsOfGroup: leaf.GetName()},
	}))

	err := c.DeleteGroup(t.Context(), &monitoringpb.DeleteGroupRequest{Name: root.GetName()})
	assert.Equal(t, "FailedPrecondition", codeOf(err).String())

	require.NoError(
		t,
		c.DeleteGroup(t.Context(), &monitoringpb.DeleteGroupRequest{Name: root.GetName(), Recursive: true}),
	)
	assert.Empty(t, names(&monitoringpb.ListGroupsRequest{}))
}

func TestGroupUpdateAndErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	c := e.groups(t)
	a := e.createGroup(t, "a", "", `resource.type = "gce_instance"`)
	b := e.createGroup(t, "b", a.GetName(), `resource.type = "gce_instance"`)

	a.ParentName = b.GetName()
	_, err := c.UpdateGroup(t.Context(), &monitoringpb.UpdateGroupRequest{Group: a})
	assert.Equal(t, "InvalidArgument", codeOf(err).String(), "cycle")

	b.DisplayName = "renamed"
	upd, err := c.UpdateGroup(t.Context(), &monitoringpb.UpdateGroupRequest{Group: b})
	require.NoError(t, err)
	assert.Equal(t, "renamed", upd.GetDisplayName())

	tests := []struct {
		g    *monitoringpb.Group
		name string
	}{
		{&monitoringpb.Group{Filter: `resource.type = "x"`}, "no display name"},
		{&monitoringpb.Group{DisplayName: "x"}, "no filter"},
		{&monitoringpb.Group{DisplayName: "x", Filter: `metric.type = "x"`}, "non resource filter"},
		{&monitoringpb.Group{DisplayName: "x", Filter: `resource.type =`}, "bad filter"},
		{
			&monitoringpb.Group{DisplayName: "x", Filter: `resource.type = "x"`, ParentName: proj + "/groups/404"},
			"missing parent",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, createErr := c.CreateGroup(t.Context(), &monitoringpb.CreateGroupRequest{Name: proj, Group: tt.g})
			require.Error(t, createErr)
		})
	}

	_, err = c.GetGroup(t.Context(), &monitoringpb.GetGroupRequest{Name: proj + "/groups/404"})
	assert.Equal(t, "NotFound", codeOf(err).String())
}
