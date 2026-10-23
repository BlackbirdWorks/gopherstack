package connect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func strCond(field, value string, cmp types.StringComparisonType) *types.StringCondition {
	return &types.StringCondition{FieldName: aws.String(field), Value: aws.String(value), ComparisonType: cmp}
}

func TestSearchQueues(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "searchq"))
	_, hours, _ := f.defaultQueue(t, aws.ToString(inst))

	for _, n := range []string{"sales-east", "sales-west", "support"} {
		_, err := f.client.CreateQueue(t.Context(), &sdk.CreateQueueInput{
			InstanceId: inst, Name: aws.String(n), HoursOfOperationId: aws.String(hours),
			Description: aws.String("queue " + n), Tags: map[string]string{"dept": n[:3]},
		})
		require.NoError(t, err)
	}

	tests := []struct {
		criteria *types.QueueSearchCriteria
		filter   *types.QueueSearchFilter
		name     string
		want     int
	}{
		{name: "none", want: 4},
		{
			name: "starts with",
			criteria: &types.QueueSearchCriteria{
				StringCondition: strCond("name", "SALES", types.StringComparisonTypeStartsWith),
			},
			want: 2,
		},
		{
			name: "contains",
			criteria: &types.QueueSearchCriteria{
				StringCondition: strCond("name", "-w", types.StringComparisonTypeContains),
			},
			want: 1,
		},
		{
			name: "exact",
			criteria: &types.QueueSearchCriteria{
				StringCondition: strCond("name", "support", types.StringComparisonTypeExact),
			},
			want: 1,
		},
		{
			name: "description",
			criteria: &types.QueueSearchCriteria{
				StringCondition: strCond("description", "queue sup", types.StringComparisonTypeStartsWith),
			},
			want: 1,
		},
		{name: "and", criteria: &types.QueueSearchCriteria{AndConditions: []types.QueueSearchCriteria{
			{StringCondition: strCond("name", "sales", types.StringComparisonTypeStartsWith)},
			{StringCondition: strCond("name", "east", types.StringComparisonTypeContains)},
		}}, want: 1},
		{name: "or", criteria: &types.QueueSearchCriteria{OrConditions: []types.QueueSearchCriteria{
			{StringCondition: strCond("name", "support", types.StringComparisonTypeExact)},
			{StringCondition: strCond("name", "sales-east", types.StringComparisonTypeExact)},
		}}, want: 2},
		{
			name:     "queue type",
			criteria: &types.QueueSearchCriteria{QueueTypeCondition: types.SearchableQueueTypeStandard},
			want:     4,
		},
		{name: "tag filter", filter: &types.QueueSearchFilter{TagFilter: &types.ControlPlaneTagFilter{
			TagCondition: &types.TagCondition{TagKey: aws.String("dept"), TagValue: aws.String("sal")},
		}}, want: 2},
		{name: "tag or groups", filter: &types.QueueSearchFilter{TagFilter: &types.ControlPlaneTagFilter{
			OrConditions: [][]types.TagCondition{
				{{TagKey: aws.String("dept"), TagValue: aws.String("sup")}},
				{{TagKey: aws.String("dept"), TagValue: aws.String("zzz")}},
			},
		}}, want: 1},
		{name: "tag and misses", filter: &types.QueueSearchFilter{TagFilter: &types.ControlPlaneTagFilter{
			AndConditions: []types.TagCondition{{TagKey: aws.String("dept")}, {TagKey: aws.String("absent")}},
		}}, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := f.client.SearchQueues(t.Context(), &sdk.SearchQueuesInput{
				InstanceId: inst, SearchCriteria: tt.criteria, SearchFilter: tt.filter,
			})
			require.NoError(t, err)
			assert.Len(t, out.Queues, tt.want)
			assert.Equal(t, int64(tt.want), aws.ToInt64(out.ApproximateTotalCount))
		})
	}

	page, err := f.client.SearchQueues(t.Context(), &sdk.SearchQueuesInput{InstanceId: inst, MaxResults: aws.Int32(3)})
	require.NoError(t, err)
	assert.Len(t, page.Queues, 3)
	assert.NotEmpty(t, aws.ToString(page.NextToken))
	assert.Equal(t, int64(4), aws.ToInt64(page.ApproximateTotalCount))

	_, err = f.client.SearchQueues(t.Context(), &sdk.SearchQueuesInput{
		InstanceId: inst,
		SearchCriteria: &types.QueueSearchCriteria{
			StringCondition: strCond("bogus", "x", types.StringComparisonTypeExact),
		},
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	_, err = f.client.SearchQueues(t.Context(), &sdk.SearchQueuesInput{InstanceId: aws.String("missing")})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestSearchResources(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "searchall"))
	_, _, profile := f.defaultQueue(t, aws.ToString(inst))
	user := f.newUser(t, aws.ToString(inst), "ada", profile)
	f.newFlow(t, aws.ToString(inst), "welcome")

	_, err := f.client.CreatePredefinedAttribute(t.Context(), &sdk.CreatePredefinedAttributeInput{
		InstanceId: inst, Name: aws.String("language"),
		Values: &types.PredefinedAttributeValuesMemberStringList{Value: []string{"en"}},
	})
	require.NoError(t, err)

	_, err = f.client.AssociateUserProficiencies(t.Context(), &sdk.AssociateUserProficienciesInput{
		InstanceId: inst, UserId: user.UserId,
		UserProficiencies: []types.UserProficiency{{
			AttributeName: aws.String("language"), AttributeValue: aws.String("en"), Level: aws.Float32(4),
		}},
	})
	require.NoError(t, err)

	t.Run("users", func(t *testing.T) {
		t.Parallel()

		byName, uerr := f.client.SearchUsers(t.Context(), &sdk.SearchUsersInput{
			InstanceId: inst,
			SearchCriteria: &types.UserSearchCriteria{
				StringCondition: strCond("FirstName", "ad", types.StringComparisonTypeStartsWith),
			},
		})
		require.NoError(t, uerr)
		require.Len(t, byName.Users, 1)
		assert.Equal(t, "ada", aws.ToString(byName.Users[0].Username))

		byLevel, uerr := f.client.SearchUsers(t.Context(), &sdk.SearchUsersInput{
			InstanceId: inst,
			SearchCriteria: &types.UserSearchCriteria{ListCondition: &types.ListCondition{
				TargetListType: types.TargetListTypeProficiencies,
				Conditions: []types.Condition{
					{StringCondition: strCond("ProficiencyName", "language", types.StringComparisonTypeExact)},
					{NumberCondition: &types.NumberCondition{
						FieldName: aws.String(
							"ProficiencyLevel",
						),
						ComparisonType: types.NumberComparisonTypeGreaterOrEqual,
						MinValue:       aws.Int32(4),
					}},
				},
			}},
		})
		require.NoError(t, uerr)
		assert.Len(t, byLevel.Users, 1)

		tooHigh, uerr := f.client.SearchUsers(t.Context(), &sdk.SearchUsersInput{
			InstanceId: inst,
			SearchCriteria: &types.UserSearchCriteria{ListCondition: &types.ListCondition{
				TargetListType: types.TargetListTypeProficiencies,
				Conditions: []types.Condition{{NumberCondition: &types.NumberCondition{
					FieldName: aws.String(
						"ProficiencyLevel",
					),
					ComparisonType: types.NumberComparisonTypeGreater,
					MinValue:       aws.Int32(4),
				}}},
			}},
		})
		require.NoError(t, uerr)
		assert.Empty(t, tooHigh.Users)
	})

	t.Run("flows", func(t *testing.T) {
		t.Parallel()

		out, ferr := f.client.SearchContactFlows(t.Context(), &sdk.SearchContactFlowsInput{
			InstanceId: inst,
			SearchCriteria: &types.ContactFlowSearchCriteria{
				StringCondition: strCond("name", "welcome", types.StringComparisonTypeExact),
				TypeCondition:   types.ContactFlowTypeContactFlow,
			},
		})
		require.NoError(t, ferr)
		assert.Len(t, out.ContactFlows, 1)

		none, ferr := f.client.SearchContactFlows(t.Context(), &sdk.SearchContactFlowsInput{
			InstanceId:     inst,
			SearchCriteria: &types.ContactFlowSearchCriteria{StatusCondition: types.ContactFlowStatusSaved},
		})
		require.NoError(t, ferr)
		assert.Empty(t, none.ContactFlows)
	})

	t.Run("simple", func(t *testing.T) {
		t.Parallel()

		hours, herr := f.client.SearchHoursOfOperations(t.Context(), &sdk.SearchHoursOfOperationsInput{
			InstanceId: inst,
			SearchCriteria: &types.HoursOfOperationSearchCriteria{
				StringCondition: strCond("name", "basic", types.StringComparisonTypeStartsWith),
			},
		})
		require.NoError(t, herr)
		assert.Len(t, hours.HoursOfOperations, 1)

		sp, perr := f.client.SearchSecurityProfiles(t.Context(), &sdk.SearchSecurityProfilesInput{
			InstanceId: inst,
			SearchCriteria: &types.SecurityProfileSearchCriteria{
				StringCondition: strCond("name", "admin", types.StringComparisonTypeExact),
			},
		})
		require.NoError(t, perr)
		require.Len(t, sp.SecurityProfiles, 1)
		assert.Equal(t, "Admin", aws.ToString(sp.SecurityProfiles[0].SecurityProfileName))

		as, aerr := f.client.SearchAgentStatuses(t.Context(), &sdk.SearchAgentStatusesInput{InstanceId: inst})
		require.NoError(t, aerr)
		assert.Len(t, as.AgentStatuses, 2)

		rp, rerr := f.client.SearchRoutingProfiles(t.Context(), &sdk.SearchRoutingProfilesInput{InstanceId: inst})
		require.NoError(t, rerr)
		require.Len(t, rp.RoutingProfiles, 1)
		assert.Equal(t, int64(1), aws.ToInt64(rp.RoutingProfiles[0].NumberOfAssociatedUsers))

		pa, perr := f.client.SearchPredefinedAttributes(t.Context(), &sdk.SearchPredefinedAttributesInput{
			InstanceId: inst,
			SearchCriteria: &types.PredefinedAttributeSearchCriteria{
				StringCondition: strCond("name", "lang", types.StringComparisonTypeStartsWith),
			},
		})
		require.NoError(t, perr)
		assert.Len(t, pa.PredefinedAttributes, 1)

		for name, call := range map[string]func() (int, error){
			"prompts": func() (int, error) {
				o, e := f.client.SearchPrompts(t.Context(), &sdk.SearchPromptsInput{InstanceId: inst})

				return len(o.Prompts), e
			},
			"quick connects": func() (int, error) {
				o, e := f.client.SearchQuickConnects(t.Context(), &sdk.SearchQuickConnectsInput{InstanceId: inst})

				return len(o.QuickConnects), e
			},
			"modules": func() (int, error) {
				o, e := f.client.SearchContactFlowModules(t.Context(), &sdk.SearchContactFlowModulesInput{InstanceId: inst})

				return len(o.ContactFlowModules), e
			},
			"groups": func() (int, error) {
				o, e := f.client.SearchUserHierarchyGroups(t.Context(), &sdk.SearchUserHierarchyGroupsInput{InstanceId: inst})

				return len(o.UserHierarchyGroups), e
			},
			"views": func() (int, error) {
				o, e := f.client.SearchViews(t.Context(), &sdk.SearchViewsInput{InstanceId: inst})

				return len(o.Views), e
			},
			"rules": func() (int, error) {
				o, e := f.client.SearchRules(t.Context(), &sdk.SearchRulesInput{InstanceId: inst})

				return len(o.Rules), e
			},
		} {
			n, e := call()
			require.NoError(t, e, name)
			assert.Zero(t, n, name)
		}
	})

	t.Run("resource tags", func(t *testing.T) {
		t.Parallel()

		all, terr := f.client.SearchResourceTags(t.Context(), &sdk.SearchResourceTagsInput{InstanceId: inst})
		require.NoError(t, terr)
		assert.Contains(t, all.Tags, types.TagSet{Key: aws.String("team"), Value: aws.String("a")})

		typed, terr := f.client.SearchResourceTags(t.Context(), &sdk.SearchResourceTagsInput{
			InstanceId: inst, ResourceTypes: []string{"queue"},
		})
		require.NoError(t, terr)
		assert.Empty(t, typed.Tags)

		byKey, terr := f.client.SearchResourceTags(t.Context(), &sdk.SearchResourceTagsInput{
			InstanceId: inst,
			SearchCriteria: &types.ResourceTagsSearchCriteria{TagSearchCondition: &types.TagSearchCondition{
				TagKey: aws.String("tea"), TagKeyComparisonType: types.StringComparisonTypeStartsWith,
			}},
		})
		require.NoError(t, terr)
		assert.Len(t, byKey.Tags, 1)
	})
}

func TestSearchHoursOfOperationOverridesAndRules(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "searchovr"))
	hours := f.newHours(t, aws.ToString(inst), "base")

	for _, d := range []struct{ name, from string }{{"early", "2026-01-10"}, {"late", "2026-09-10"}} {
		_, err := f.client.CreateHoursOfOperationOverride(t.Context(), &sdk.CreateHoursOfOperationOverrideInput{
			InstanceId: inst, HoursOfOperationId: hours.HoursOfOperationId, Name: aws.String(d.name),
			EffectiveFrom: aws.String(d.from), EffectiveTill: aws.String(d.from),
			Config: []types.HoursOfOperationOverrideConfig{overrideCfg(types.OverrideDaysMonday, 9, 10)},
		})
		require.NoError(t, err)
	}

	out, err := f.client.SearchHoursOfOperationOverrides(t.Context(), &sdk.SearchHoursOfOperationOverridesInput{
		InstanceId: inst,
		SearchCriteria: &types.HoursOfOperationOverrideSearchCriteria{DateCondition: &types.DateCondition{
			FieldName: aws.String(
				"EffectiveFrom",
			),
			Value:          aws.String("2026-06-01"),
			ComparisonType: types.DateComparisonTypeGreaterThan,
		}},
	})
	require.NoError(t, err)
	require.Len(t, out.HoursOfOperationOverrides, 1)
	assert.Equal(t, "late", aws.ToString(out.HoursOfOperationOverrides[0].Name))

	_, err = f.client.CreateRule(t.Context(), &sdk.CreateRuleInput{
		InstanceId:    inst,
		Name:          aws.String("evt"),
		Function:      aws.String("{}"),
		PublishStatus: types.RulePublishStatusDraft,
		TriggerEventSource: &types.RuleTriggerEventSource{
			EventSourceName: types.EventSourceNameOnPostCallAnalysisAvailable,
		},
		Actions: []types.RuleAction{eventAction()},
	})
	require.NoError(t, err)

	rules, err := f.client.SearchRules(t.Context(), &sdk.SearchRulesInput{
		InstanceId: inst,
		SearchCriteria: &types.RulesSearchCriteria{
			StringCondition: strCond("name", "ev", types.StringComparisonTypeStartsWith),
		},
	})
	require.NoError(t, err)
	require.Len(t, rules.Rules, 1)
	assert.Len(t, rules.Rules[0].ActionSummaries, 1)
	assert.Equal(t, types.EventSourceNameOnPostCallAnalysisAvailable, rules.Rules[0].TriggerEventSource.EventSourceName)
}
