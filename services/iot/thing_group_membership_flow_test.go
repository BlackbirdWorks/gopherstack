package iot_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

func TestListThingsInThingGroup_Recursive_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		group     string
		want      []string
		recursive bool
	}{
		{name: "root_direct", group: "root", want: []string{}},
		{name: "root_recursive", group: "root", recursive: true, want: []string{"t1", "t2"}},
		{name: "child_direct", group: "child", want: []string{"t1"}},
		{name: "child_recursive", group: "child", recursive: true, want: []string{"t1", "t2"}},
		{name: "leaf_recursive", group: "leaf", recursive: true, want: []string{"t2"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestIoTClient(t, iot.NewHandler(iot.NewInMemoryBackend(), nil))
			ctx := t.Context()

			for _, g := range []struct{ name, parent string }{{"root", ""}, {"child", "root"}, {"leaf", "child"}} {
				in := &iotsdk.CreateThingGroupInput{ThingGroupName: aws.String(g.name)}
				if g.parent != "" {
					in.ParentGroupName = aws.String(g.parent)
				}

				_, err := client.CreateThingGroup(ctx, in)
				require.NoError(t, err)
			}

			for thing, group := range map[string]string{"t1": "child", "t2": "leaf"} {
				_, err := client.CreateThing(ctx, &iotsdk.CreateThingInput{ThingName: aws.String(thing)})
				require.NoError(t, err)

				_, err = client.AddThingToThingGroup(ctx, &iotsdk.AddThingToThingGroupInput{
					ThingName: aws.String(thing), ThingGroupName: aws.String(group),
				})
				require.NoError(t, err)
			}

			out, err := client.ListThingsInThingGroup(ctx, &iotsdk.ListThingsInThingGroupInput{
				ThingGroupName: aws.String(tt.group), Recursive: tt.recursive,
			})
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.want, out.Things)
		})
	}
}
