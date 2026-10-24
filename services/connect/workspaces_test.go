package connect_test

import (
	"encoding/base64"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (f *fixture) newWorkspace(t *testing.T, inst *string, name string) *sdk.CreateWorkspaceOutput {
	t.Helper()

	out, err := f.client.CreateWorkspace(t.Context(), &sdk.CreateWorkspaceInput{
		InstanceId: inst, Name: aws.String(name), Tags: map[string]string{"stage": name},
	})
	require.NoError(t, err)

	return out
}

func TestWorkspaceLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "ws"))
	theme := &types.WorkspaceTheme{Light: &types.WorkspaceThemeConfig{
		Palette: &types.WorkspaceThemePalette{Header: &types.PaletteHeader{Background: aws.String("#ffffff")}},
	}}

	made, err := f.client.CreateWorkspace(t.Context(), &sdk.CreateWorkspaceInput{
		InstanceId: inst, Name: aws.String("Agents"), Title: aws.String("Agent desk"),
		Description: aws.String("main"), Theme: theme, Tags: map[string]string{"team": "care"},
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(made.WorkspaceArn), "/workspace/"+aws.ToString(made.WorkspaceId))

	_, err = f.client.CreateWorkspace(
		t.Context(),
		&sdk.CreateWorkspaceInput{InstanceId: inst, Name: aws.String("AGENTS")},
	)
	assert.Equal(t, "DuplicateResourceException", apiCode(err))

	got, err := f.client.DescribeWorkspace(t.Context(),
		&sdk.DescribeWorkspaceInput{InstanceId: inst, WorkspaceId: made.WorkspaceId})
	require.NoError(t, err)
	assert.Equal(t, "Agent desk", aws.ToString(got.Workspace.Title))
	assert.Equal(t, "main", aws.ToString(got.Workspace.Description))
	assert.Equal(t, types.VisibilityAssigned, got.Workspace.Visibility)
	assert.Equal(t, map[string]string{"team": "care"}, got.Workspace.Tags)
	assert.Equal(t, "#ffffff", aws.ToString(got.Workspace.Theme.Light.Palette.Header.Background))
	assert.Nil(t, got.Workspace.Theme.Dark)
	assert.NotNil(t, got.Workspace.LastModifiedTime)

	_, err = f.client.UpdateWorkspaceMetadata(t.Context(), &sdk.UpdateWorkspaceMetadataInput{
		InstanceId: inst, WorkspaceId: made.WorkspaceId, Name: aws.String("Desk"), Title: aws.String("New title"),
	})
	require.NoError(t, err)

	other := f.newWorkspace(t, inst, "Other")
	_, err = f.client.UpdateWorkspaceMetadata(t.Context(), &sdk.UpdateWorkspaceMetadataInput{
		InstanceId: inst, WorkspaceId: other.WorkspaceId, Name: aws.String("desk"),
	})
	assert.Equal(t, "DuplicateResourceException", apiCode(err))

	_, err = f.client.UpdateWorkspaceVisibility(t.Context(), &sdk.UpdateWorkspaceVisibilityInput{
		InstanceId: inst, WorkspaceId: made.WorkspaceId, Visibility: types.VisibilityAll,
	})
	require.NoError(t, err)
	_, err = f.client.UpdateWorkspaceVisibility(t.Context(), &sdk.UpdateWorkspaceVisibilityInput{
		InstanceId: inst, WorkspaceId: made.WorkspaceId, Visibility: "SOME",
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	_, err = f.client.UpdateWorkspaceTheme(t.Context(), &sdk.UpdateWorkspaceThemeInput{
		InstanceId: inst, WorkspaceId: made.WorkspaceId, Theme: &types.WorkspaceTheme{
			Dark: &types.WorkspaceThemeConfig{Palette: &types.WorkspaceThemePalette{
				Header: &types.PaletteHeader{Background: aws.String("#000000")},
			}},
		},
	})
	require.NoError(t, err)

	got, err = f.client.DescribeWorkspace(t.Context(),
		&sdk.DescribeWorkspaceInput{InstanceId: inst, WorkspaceId: made.WorkspaceId})
	require.NoError(t, err)
	assert.Equal(t, "Desk", aws.ToString(got.Workspace.Name))
	assert.Equal(t, "New title", aws.ToString(got.Workspace.Title))
	assert.Equal(t, "main", aws.ToString(got.Workspace.Description), "untouched members stay")
	assert.Equal(t, types.VisibilityAll, got.Workspace.Visibility)
	assert.Nil(t, got.Workspace.Theme.Light, "the theme is replaced, not merged")
	assert.Equal(t, "#000000", aws.ToString(got.Workspace.Theme.Dark.Palette.Header.Background))

	list, err := f.client.ListWorkspaces(
		t.Context(),
		&sdk.ListWorkspacesInput{InstanceId: inst, MaxResults: aws.Int32(1)},
	)
	require.NoError(t, err)
	assert.Len(t, list.WorkspaceSummaryList, 1)
	require.NotEmpty(t, aws.ToString(list.NextToken))

	rest, err := f.client.ListWorkspaces(
		t.Context(),
		&sdk.ListWorkspacesInput{InstanceId: inst, NextToken: list.NextToken},
	)
	require.NoError(t, err)
	assert.Len(t, rest.WorkspaceSummaryList, 1)

	tags, err := f.client.ListTagsForResource(
		t.Context(),
		&sdk.ListTagsForResourceInput{ResourceArn: made.WorkspaceArn},
	)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "care"}, tags.Tags)

	dst := f.restored(t)
	got, err = dst.client.DescribeWorkspace(t.Context(),
		&sdk.DescribeWorkspaceInput{InstanceId: inst, WorkspaceId: made.WorkspaceId})
	require.NoError(t, err)
	assert.Equal(t, "#000000", aws.ToString(got.Workspace.Theme.Dark.Palette.Header.Background))

	_, err = f.client.DeleteWorkspace(
		t.Context(),
		&sdk.DeleteWorkspaceInput{InstanceId: inst, WorkspaceId: made.WorkspaceId},
	)
	require.NoError(t, err)

	for name, call := range map[string]func() error{
		"describe": func() error {
			_, e := f.client.DescribeWorkspace(t.Context(),
				&sdk.DescribeWorkspaceInput{InstanceId: inst, WorkspaceId: made.WorkspaceId})

			return e
		},
		"delete": func() error {
			_, e := f.client.DeleteWorkspace(t.Context(),
				&sdk.DeleteWorkspaceInput{InstanceId: inst, WorkspaceId: made.WorkspaceId})

			return e
		},
		"visibility": func() error {
			_, e := f.client.UpdateWorkspaceVisibility(t.Context(), &sdk.UpdateWorkspaceVisibilityInput{
				InstanceId: inst, WorkspaceId: made.WorkspaceId, Visibility: types.VisibilityNone,
			})

			return e
		},
		"pages": func() error {
			_, e := f.client.ListWorkspacePages(t.Context(),
				&sdk.ListWorkspacePagesInput{InstanceId: inst, WorkspaceId: made.WorkspaceId})

			return e
		},
	} {
		assert.Equal(t, "ResourceNotFoundException", apiCode(call()), name)
	}
}

func TestWorkspacePages(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "wspages"))
	ws := f.newWorkspace(t, inst, "pages").WorkspaceId

	view, err := f.client.CreateView(t.Context(), &sdk.CreateViewInput{
		InstanceId: inst, Name: aws.String("greeting"), Status: types.ViewStatusPublished,
		Content: &types.ViewInputContent{Template: aws.String(viewTemplate), Actions: []string{"Go"}},
	})
	require.NoError(t, err)

	arn := view.View.Arn

	_, err = f.client.CreateWorkspacePage(t.Context(), &sdk.CreateWorkspacePageInput{
		InstanceId: inst, WorkspaceId: ws, Page: aws.String("HOME"), ResourceArn: arn,
		InputData: aws.String(`{"Name":"Ada"}`), Slug: aws.String("home"),
	})
	require.NoError(t, err)

	tests := []struct {
		in   sdk.CreateWorkspacePageInput
		name string
		code string
	}{
		{
			name: "duplicate page",
			in:   sdk.CreateWorkspacePageInput{Page: aws.String("home"), ResourceArn: arn},
			code: "DuplicateResourceException",
		},
		{
			name: "reserved prefix",
			in:   sdk.CreateWorkspacePageInput{Page: aws.String("aws:special"), ResourceArn: arn},
			code: "InvalidParameterException",
		},
		{
			name: "unknown view",
			in: sdk.CreateWorkspacePageInput{
				Page:        aws.String("faq"),
				ResourceArn: aws.String("arn:aws:connect:us-east-1:123456789012:instance/x/view/missing"),
			},
			code: "ResourceNotFoundException",
		},
		{
			name: "input is not an object",
			in:   sdk.CreateWorkspacePageInput{Page: aws.String("faq"), ResourceArn: arn, InputData: aws.String("[1]")},
			code: "InvalidParameterException",
		},
	}
	for _, tt := range tests {
		in := tt.in
		in.InstanceId, in.WorkspaceId = inst, ws
		_, cerr := f.client.CreateWorkspacePage(t.Context(), &in)
		assert.Equal(t, tt.code, apiCode(cerr), tt.name)
	}

	_, err = f.client.CreateWorkspacePage(t.Context(), &sdk.CreateWorkspacePageInput{
		InstanceId: inst, WorkspaceId: ws, Page: aws.String("faq"), ResourceArn: arn,
	})
	require.NoError(t, err)

	_, err = f.client.UpdateWorkspacePage(t.Context(), &sdk.UpdateWorkspacePageInput{
		InstanceId: inst, WorkspaceId: ws, Page: aws.String("faq"), NewPage: aws.String("help"), Slug: aws.String("h"),
	})
	require.NoError(t, err)
	_, err = f.client.UpdateWorkspacePage(t.Context(), &sdk.UpdateWorkspacePageInput{
		InstanceId: inst, WorkspaceId: ws, Page: aws.String("help"), NewPage: aws.String("HOME"),
	})
	assert.Equal(t, "DuplicateResourceException", apiCode(err), "rename onto an existing page")
	_, err = f.client.UpdateWorkspacePage(t.Context(), &sdk.UpdateWorkspacePageInput{
		InstanceId: inst, WorkspaceId: ws, Page: aws.String("ghost"), Slug: aws.String("g"),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	pages, err := f.client.ListWorkspacePages(
		t.Context(),
		&sdk.ListWorkspacePagesInput{InstanceId: inst, WorkspaceId: ws},
	)
	require.NoError(t, err)
	require.Len(t, pages.WorkspacePageList, 2)
	assert.Equal(t, "HOME", aws.ToString(pages.WorkspacePageList[0].Page))
	assert.JSONEq(t, `{"Name":"Ada"}`, aws.ToString(pages.WorkspacePageList[0].InputData))
	assert.Equal(t, "home", aws.ToString(pages.WorkspacePageList[0].Slug))
	assert.Equal(t, "help", aws.ToString(pages.WorkspacePageList[1].Page))
	assert.Equal(t, "h", aws.ToString(pages.WorkspacePageList[1].Slug))
	assert.Equal(t, aws.ToString(arn), aws.ToString(pages.WorkspacePageList[1].ResourceArn))

	first, err := f.client.ListWorkspacePages(t.Context(),
		&sdk.ListWorkspacePagesInput{InstanceId: inst, WorkspaceId: ws, MaxResults: aws.Int32(1)})
	require.NoError(t, err)
	assert.Len(t, first.WorkspacePageList, 1)
	assert.NotEmpty(t, aws.ToString(first.NextToken))

	_, err = f.client.DeleteWorkspacePage(t.Context(),
		&sdk.DeleteWorkspacePageInput{InstanceId: inst, WorkspaceId: ws, Page: aws.String("help")})
	require.NoError(t, err)
	_, err = f.client.DeleteWorkspacePage(t.Context(),
		&sdk.DeleteWorkspacePageInput{InstanceId: inst, WorkspaceId: ws, Page: aws.String("help")})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	dst := f.restored(t)
	pages, err = dst.client.ListWorkspacePages(
		t.Context(),
		&sdk.ListWorkspacePagesInput{InstanceId: inst, WorkspaceId: ws},
	)
	require.NoError(t, err)
	assert.Len(t, pages.WorkspacePageList, 1)
}

func TestWorkspaceMedia(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "wsmedia"))
	ws := f.newWorkspace(t, inst, "media").WorkspaceId

	logo := base64.StdEncoding.EncodeToString([]byte("png-bytes"))
	for _, src := range []string{"https://example.com/logo.png", logo} {
		_, err := f.client.ImportWorkspaceMedia(t.Context(), &sdk.ImportWorkspaceMediaInput{
			InstanceId:  inst,
			WorkspaceId: ws,
			MediaType:   types.MediaTypeImageLogoLightHorizontal,
			MediaSource: aws.String(src),
		})
		require.NoError(t, err, src)
	}

	_, err := f.client.ImportWorkspaceMedia(t.Context(), &sdk.ImportWorkspaceMediaInput{
		InstanceId:  inst,
		WorkspaceId: ws,
		MediaType:   types.MediaTypeImageLogoDarkFavicon,
		MediaSource: aws.String("%%%"),
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err), "neither URL nor base64")
	_, err = f.client.ImportWorkspaceMedia(t.Context(), &sdk.ImportWorkspaceMediaInput{
		InstanceId: inst, WorkspaceId: ws, MediaType: "IMAGE_LOGO_FAVICON", MediaSource: aws.String(logo),
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err), "unknown media type")

	_, err = f.client.ImportWorkspaceMedia(t.Context(), &sdk.ImportWorkspaceMediaInput{
		InstanceId:  inst,
		WorkspaceId: ws,
		MediaType:   types.MediaTypeImageLogoDarkFavicon,
		MediaSource: aws.String(logo),
	})
	require.NoError(t, err)

	media, err := f.client.ListWorkspaceMedia(
		t.Context(),
		&sdk.ListWorkspaceMediaInput{InstanceId: inst, WorkspaceId: ws},
	)
	require.NoError(t, err)
	require.Len(t, media.Media, 2, "importing the same type again replaces the asset")
	assert.Equal(t, types.MediaTypeImageLogoLightHorizontal, media.Media[0].Type)
	assert.Equal(t, logo, aws.ToString(media.Media[0].Source))

	_, err = f.client.DeleteWorkspaceMedia(t.Context(), &sdk.DeleteWorkspaceMediaInput{
		InstanceId: inst, WorkspaceId: ws, MediaType: types.MediaTypeImageLogoLightHorizontal,
	})
	require.NoError(t, err)
	_, err = f.client.DeleteWorkspaceMedia(t.Context(), &sdk.DeleteWorkspaceMediaInput{
		InstanceId: inst, WorkspaceId: ws, MediaType: types.MediaTypeImageLogoLightHorizontal,
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	dst := f.restored(t)
	media, err = dst.client.ListWorkspaceMedia(
		t.Context(),
		&sdk.ListWorkspaceMediaInput{InstanceId: inst, WorkspaceId: ws},
	)
	require.NoError(t, err)
	require.Len(t, media.Media, 1)
	assert.Equal(t, types.MediaTypeImageLogoDarkFavicon, media.Media[0].Type)
}

func TestWorkspaceAssociations(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	instID := f.newInstance(t, "wsassoc")
	inst := aws.String(instID)
	_, _, profileID := f.defaultQueue(t, instID)
	user := f.newUser(t, instID, "ada", profileID)
	profile, err := f.client.DescribeRoutingProfile(t.Context(),
		&sdk.DescribeRoutingProfileInput{InstanceId: inst, RoutingProfileId: aws.String(profileID)})
	require.NoError(t, err)

	ws := f.newWorkspace(t, inst, "assigned")
	other := f.newWorkspace(t, inst, "unassigned")
	queueARN := aws.String("arn:aws:connect:us-east-1:123456789012:instance/" + instID + "/queue/q1")
	ghostUser := aws.String("arn:aws:connect:us-east-1:123456789012:instance/" + instID + "/agent/ghost")

	out, err := f.client.AssociateWorkspace(t.Context(), &sdk.AssociateWorkspaceInput{
		InstanceId:  inst,
		WorkspaceId: ws.WorkspaceId,
		ResourceArns: []string{
			aws.ToString(user.UserArn),
			aws.ToString(profile.RoutingProfile.RoutingProfileArn),
			aws.ToString(queueARN),
			aws.ToString(ghostUser),
		},
	})
	require.NoError(t, err)
	assert.Len(t, out.SuccessfulList, 2)
	require.Len(t, out.FailedList, 2)
	assert.Equal(t, "InvalidParameterException", aws.ToString(out.FailedList[0].ErrorCode))
	assert.Equal(t, aws.ToString(queueARN), aws.ToString(out.FailedList[0].ResourceArn))
	assert.Equal(t, "ResourceNotFoundException", aws.ToString(out.FailedList[1].ErrorCode))

	again, err := f.client.AssociateWorkspace(t.Context(), &sdk.AssociateWorkspaceInput{
		InstanceId: inst, WorkspaceId: ws.WorkspaceId, ResourceArns: []string{aws.ToString(user.UserArn)},
	})
	require.NoError(t, err)
	assert.Len(t, again.SuccessfulList, 1, "re-associating is a no-op")

	all, err := f.client.SearchWorkspaceAssociations(
		t.Context(),
		&sdk.SearchWorkspaceAssociationsInput{InstanceId: inst},
	)
	require.NoError(t, err)
	require.Len(t, all.WorkspaceAssociations, 2)
	assert.EqualValues(t, 2, aws.ToInt64(all.ApproximateTotalCount))

	byType, err := f.client.SearchWorkspaceAssociations(t.Context(), &sdk.SearchWorkspaceAssociationsInput{
		InstanceId: inst,
		SearchCriteria: &types.WorkspaceAssociationSearchCriteria{StringCondition: &types.StringCondition{
			FieldName: aws.String(
				"ResourceType",
			),
			Value:          aws.String("USER"),
			ComparisonType: types.StringComparisonTypeExact,
		}},
	})
	require.NoError(t, err)
	require.Len(t, byType.WorkspaceAssociations, 1)
	got := byType.WorkspaceAssociations[0]
	assert.Equal(t, "ada", aws.ToString(got.ResourceName))
	assert.Equal(t, aws.ToString(user.UserId), aws.ToString(got.ResourceId))
	assert.Equal(t, "USER", aws.ToString(got.ResourceType))
	assert.Equal(t, aws.ToString(ws.WorkspaceId), aws.ToString(got.WorkspaceId))
	assert.Equal(t, aws.ToString(ws.WorkspaceArn), aws.ToString(got.WorkspaceArn))

	byTag, err := f.client.SearchWorkspaceAssociations(t.Context(), &sdk.SearchWorkspaceAssociationsInput{
		InstanceId: inst, SearchFilter: &types.WorkspaceAssociationSearchFilter{
			AttributeFilter: &types.ControlPlaneAttributeFilter{
				TagCondition: &types.TagCondition{TagKey: aws.String("stage"), TagValue: aws.String("unassigned")},
			},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, byTag.WorkspaceAssociations, "the unassigned workspace has none")

	_ = other

	dis, err := f.client.DisassociateWorkspace(t.Context(), &sdk.DisassociateWorkspaceInput{
		InstanceId: inst, WorkspaceId: ws.WorkspaceId,
		ResourceArns: []string{aws.ToString(user.UserArn), aws.ToString(user.UserArn)},
	})
	require.NoError(t, err)
	assert.Len(t, dis.SuccessfulList, 1)
	require.Len(t, dis.FailedList, 1, "second removal finds nothing")
	assert.Equal(t, "ResourceNotFoundException", aws.ToString(dis.FailedList[0].ErrorCode))

	left, err := f.client.SearchWorkspaceAssociations(
		t.Context(),
		&sdk.SearchWorkspaceAssociationsInput{InstanceId: inst},
	)
	require.NoError(t, err)
	require.Len(t, left.WorkspaceAssociations, 1)
	assert.Equal(t, "ROUTING_PROFILE", aws.ToString(left.WorkspaceAssociations[0].ResourceType))

	dst := f.restored(t)
	left, err = dst.client.SearchWorkspaceAssociations(
		t.Context(),
		&sdk.SearchWorkspaceAssociationsInput{InstanceId: inst},
	)
	require.NoError(t, err)
	assert.Len(t, left.WorkspaceAssociations, 1)

	_, err = f.client.DeleteWorkspace(
		t.Context(),
		&sdk.DeleteWorkspaceInput{InstanceId: inst, WorkspaceId: ws.WorkspaceId},
	)
	require.NoError(t, err)
	left, err = f.client.SearchWorkspaceAssociations(
		t.Context(),
		&sdk.SearchWorkspaceAssociationsInput{InstanceId: inst},
	)
	require.NoError(t, err)
	assert.Empty(t, left.WorkspaceAssociations, "assignments go with the workspace")
}

func TestSearchWorkspaces(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "wssearch"))

	for _, name := range []string{"north-desk", "north-lab", "south-desk"} {
		out := f.newWorkspace(t, inst, name)

		if name == "south-desk" {
			_, err := f.client.UpdateWorkspaceVisibility(t.Context(), &sdk.UpdateWorkspaceVisibilityInput{
				InstanceId: inst, WorkspaceId: out.WorkspaceId, Visibility: types.VisibilityNone,
			})
			require.NoError(t, err)
		}
	}

	str := func(field, value string, cmp types.StringComparisonType) *types.WorkspaceSearchCriteria {
		return &types.WorkspaceSearchCriteria{StringCondition: &types.StringCondition{
			FieldName: aws.String(field), Value: aws.String(value), ComparisonType: cmp,
		}}
	}
	tests := []struct {
		criteria *types.WorkspaceSearchCriteria
		filter   *types.WorkspaceSearchFilter
		name     string
		want     []string
	}{
		{
			name:     "prefix",
			criteria: str("Name", "north", types.StringComparisonTypeStartsWith),
			want:     []string{"north-desk", "north-lab"},
		},
		{
			name:     "visibility",
			criteria: str("Visibility", "NONE", types.StringComparisonTypeExact),
			want:     []string{"south-desk"},
		},
		{
			name: "or", criteria: &types.WorkspaceSearchCriteria{OrConditions: []types.WorkspaceSearchCriteria{
				*str("Name", "-lab", types.StringComparisonTypeContains),
				*str("Name", "south", types.StringComparisonTypeStartsWith),
			}},
			want: []string{"north-lab", "south-desk"},
		},
		{
			name: "tag", filter: &types.WorkspaceSearchFilter{AttributeFilter: &types.ControlPlaneAttributeFilter{
				TagCondition: &types.TagCondition{TagKey: aws.String("stage"), TagValue: aws.String("north-lab")},
			}},
			want: []string{"north-lab"},
		},
	}
	for _, tt := range tests {
		got, err := f.client.SearchWorkspaces(t.Context(), &sdk.SearchWorkspacesInput{
			InstanceId: inst, SearchCriteria: tt.criteria, SearchFilter: tt.filter,
		})
		require.NoError(t, err, tt.name)

		var names []string
		for _, w := range got.Workspaces {
			names = append(names, aws.ToString(w.Name))
			assert.NotNil(t, w.CreatedAt, tt.name)
		}

		assert.ElementsMatch(t, tt.want, names, tt.name)
		assert.EqualValues(t, len(tt.want), aws.ToInt64(got.ApproximateTotalCount), tt.name)
	}

	page, err := f.client.SearchWorkspaces(
		t.Context(),
		&sdk.SearchWorkspacesInput{InstanceId: inst, MaxResults: aws.Int32(2)},
	)
	require.NoError(t, err)
	assert.Len(t, page.Workspaces, 2)
	assert.NotEmpty(t, aws.ToString(page.NextToken))
}
