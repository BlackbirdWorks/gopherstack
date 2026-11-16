package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	connectsdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type workspaceShape struct {
	pages []map[string]any
	media []map[string]any
	assoc bool
}

func connectWorkspaceExtrasTemplate(t *testing.T, s workspaceShape) string {
	t.Helper()

	inst := cfnRef("Instance")
	props := map[string]any{"InstanceArn": inst, "Name": "ws", "Title": "T", "Visibility": "ALL"}

	if s.pages != nil {
		props["Pages"] = s.pages
	}

	if s.media != nil {
		props["Media"] = s.media
	}

	if s.assoc {
		props["Associations"] = []any{cfnGetAtt("Profile", "RoutingProfileArn")}
	}

	return stackTemplate(t, map[string]any{
		"Instance": cfnRes("AWS::Connect::Instance", map[string]any{
			"IdentityManagementType": "CONNECT_MANAGED", "InstanceAlias": "cfn-wsx",
			"Attributes": map[string]any{"InboundCalls": true, "OutboundCalls": true},
		}),
		"Hours": cfnRes("AWS::Connect::HoursOfOperation", map[string]any{
			"InstanceArn": inst, "Name": "h", "TimeZone": "UTC",
			"Config": []any{map[string]any{
				"Day": "MONDAY", "StartTime": map[string]any{"Hours": 9, "Minutes": 0},
				"EndTime": map[string]any{"Hours": 17, "Minutes": 0},
			}},
		}),
		"Queue": cfnRes("AWS::Connect::Queue", map[string]any{
			"InstanceArn": inst, "Name": "q", "HoursOfOperationArn": cfnGetAtt("Hours", "HoursOfOperationArn"),
		}),
		"Profile": cfnRes("AWS::Connect::RoutingProfile", map[string]any{
			"InstanceArn": inst, "Name": "rp", "Description": "rp",
			"DefaultOutboundQueueArn": cfnGetAtt("Queue", "QueueArn"),
			"MediaConcurrencies":      []any{map[string]any{"Channel": "VOICE", "Concurrency": 1}},
		}),
		"View": cfnRes("AWS::Connect::View", map[string]any{
			"InstanceArn": inst, "Name": "view",
			"Template": map[string]any{"Head": map[string]any{"Title": "v"}}, "Actions": []string{"Go"},
		}),
		"Workspace": cfnRes("AWS::Connect::Workspace", props),
	}, map[string]any{
		"InstanceId": cfnGetAtt("Instance", "Id"), "WorkspaceId": cfnGetAtt("Workspace", "Id"),
		"ViewArn": cfnRef("View"), "ProfileArn": cfnGetAtt("Profile", "RoutingProfileArn"),
	})
}

func TestCreateStack_ConnectWorkspacePagesMediaAssociations(t *testing.T) {
	t.Parallel()

	env, cn := newConnectEnv(t)

	// View ARN is a stack output, so seed the first stack without pages to learn it.
	out := env.createStack(t, "wsx", connectWorkspaceExtrasTemplate(t, workspaceShape{}))
	inst, ws, view := aws.String(out["InstanceId"]), aws.String(out["WorkspaceId"]), out["ViewArn"]

	shape := func(data string, media string, assoc bool, extra bool) workspaceShape {
		pages := []map[string]any{{"Page": "HOME", "ResourceArn": view, "InputData": data}}
		if extra {
			pages = append(pages, map[string]any{"Page": "Extra", "ResourceArn": view, "Slug": "extra-page"})
		}

		return workspaceShape{
			pages: pages, assoc: assoc,
			media: []map[string]any{{"Type": media, "Source": "https://example.com/logo.png"}},
		}
	}

	tests := []struct {
		name      string
		wantMedia string
		wantPages []string
		shape     workspaceShape
		wantAssoc int
	}{
		{
			name: "created", shape: shape(`{"a":"1"}`, "IMAGE_LOGO_LIGHT_FAVICON", true, true),
			wantPages: []string{"Extra", "HOME"}, wantMedia: "IMAGE_LOGO_LIGHT_FAVICON", wantAssoc: 1,
		},
		{
			name: "reduced", shape: shape(`{"a":"2"}`, "IMAGE_LOGO_DARK_FAVICON", false, false),
			wantPages: []string{"HOME"}, wantMedia: "IMAGE_LOGO_DARK_FAVICON", wantAssoc: 0,
		},
	}

	for _, tc := range tests {
		env.updateStack(t, "wsx", connectWorkspaceExtrasTemplate(t, tc.shape))

		pages, err := cn.ListWorkspacePages(t.Context(), &connectsdk.ListWorkspacePagesInput{
			InstanceId: inst, WorkspaceId: ws,
		})
		require.NoError(t, err, tc.name)

		names := make([]string, 0, len(pages.WorkspacePageList))

		for _, p := range pages.WorkspacePageList {
			names = append(names, aws.ToString(p.Page))
		}

		assert.ElementsMatch(t, tc.wantPages, names, tc.name)

		for _, p := range pages.WorkspacePageList {
			if aws.ToString(p.Page) == "HOME" {
				assert.Equal(t, tc.shape.pages[0]["InputData"], aws.ToString(p.InputData), tc.name)
			}
		}

		media, err := cn.ListWorkspaceMedia(t.Context(), &connectsdk.ListWorkspaceMediaInput{
			InstanceId: inst, WorkspaceId: ws,
		})
		require.NoError(t, err, tc.name)
		require.Len(t, media.Media, 1, tc.name)
		assert.Equal(t, tc.wantMedia, string(media.Media[0].Type), tc.name)

		assoc, err := cn.SearchWorkspaceAssociations(t.Context(), &connectsdk.SearchWorkspaceAssociationsInput{
			InstanceId: inst,
		})
		require.NoError(t, err, tc.name)
		assert.Len(t, assoc.WorkspaceAssociations, tc.wantAssoc, tc.name)
	}
}
