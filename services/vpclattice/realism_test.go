package vpclattice_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	vpclatticesdk "github.com/aws/aws-sdk-go-v2/service/vpclattice"
	"github.com/aws/aws-sdk-go-v2/service/vpclattice/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/vpclattice"
)

func TestRealism_NameAndRangeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(c *vpclatticesdk.Client) error
		name string
	}{
		{name: "uppercase service", call: func(c *vpclatticesdk.Client) error {
			_, err := c.CreateService(t.Context(), &vpclatticesdk.CreateServiceInput{Name: aws.String("Bad_Name")})

			return err
		}},
		{name: "service id prefix", call: func(c *vpclatticesdk.Client) error {
			_, err := c.CreateService(t.Context(), &vpclatticesdk.CreateServiceInput{Name: aws.String("svc-abc")})

			return err
		}},
		{name: "service too long", call: func(c *vpclatticesdk.Client) error {
			long := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			_, err := c.CreateService(t.Context(), &vpclatticesdk.CreateServiceInput{Name: aws.String(long)})

			return err
		}},
		{name: "double hyphen network", call: func(c *vpclatticesdk.Client) error {
			_, err := c.CreateServiceNetwork(
				t.Context(),
				&vpclatticesdk.CreateServiceNetworkInput{Name: aws.String("a--b")},
			)

			return err
		}},
		{name: "trailing hyphen group", call: func(c *vpclatticesdk.Client) error {
			_, err := c.CreateTargetGroup(t.Context(), &vpclatticesdk.CreateTargetGroupInput{
				Name: aws.String("tgrp-"), Type: types.TargetGroupTypeInstance,
				Config: realismTGConfig(),
			})

			return err
		}},
		{name: "list bad token", call: func(c *vpclatticesdk.Client) error {
			_, err := c.ListServices(t.Context(), &vpclatticesdk.ListServicesInput{NextToken: aws.String("%%%garbage")})

			return err
		}},
		{name: "list max results", call: func(c *vpclatticesdk.Client) error {
			_, err := c.ListServices(t.Context(), &vpclatticesdk.ListServicesInput{MaxResults: aws.Int32(101)})

			return err
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.call(newRealClient(t))
			require.Error(t, err)

			var ae smithy.APIError
			require.ErrorAs(t, err, &ae)
			assert.Equal(t, "ValidationException", ae.ErrorCode())
			assert.NotEqual(t, "ValidationException", ae.ErrorMessage())
		})
	}
}

func TestRealism_RulePriorityAndTargetGroupRefs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate  func(in *vpclatticesdk.CreateRuleInput)
		name    string
		errCode string
	}{
		{
			name:    "duplicate priority",
			mutate:  func(in *vpclatticesdk.CreateRuleInput) { in.Name = aws.String("ruletwo") },
			errCode: "ConflictException",
		},
		{name: "priority too high", mutate: func(in *vpclatticesdk.CreateRuleInput) {
			in.Name = aws.String("ruletwo")
			in.Priority = aws.Int32(2001)
		}, errCode: "ValidationException"},
		{name: "unknown target group", mutate: func(in *vpclatticesdk.CreateRuleInput) {
			in.Name = aws.String("ruletwo")
			in.Priority = aws.Int32(5)
			in.Action = &types.RuleActionMemberForward{Value: types.ForwardAction{
				TargetGroups: []types.WeightedTargetGroup{{TargetGroupIdentifier: aws.String("tg-0123456789abcdef0")}},
			}}
		}, errCode: "ResourceNotFoundException"},
		{name: "distinct priority up to 2000", mutate: func(in *vpclatticesdk.CreateRuleInput) {
			in.Name = aws.String("ruletwo")
			in.Priority = aws.Int32(2000)
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newRealClient(t)
			ctx := t.Context()

			tg, err := c.CreateTargetGroup(ctx, &vpclatticesdk.CreateTargetGroupInput{
				Name: aws.String("tgrp"), Type: types.TargetGroupTypeInstance,
				Config: realismTGConfig(),
			})
			require.NoError(t, err)

			svc, err := c.CreateService(ctx, &vpclatticesdk.CreateServiceInput{Name: aws.String("svcone")})
			require.NoError(t, err)

			fwd := &types.RuleActionMemberForward{Value: types.ForwardAction{
				TargetGroups: []types.WeightedTargetGroup{{TargetGroupIdentifier: tg.Id}},
			}}

			lis, err := c.CreateListener(ctx, &vpclatticesdk.CreateListenerInput{
				ServiceIdentifier: svc.Id, Name: aws.String("lisone"), Protocol: types.ListenerProtocolHttp,
				DefaultAction: fwd,
			})
			require.NoError(t, err)

			match := &types.RuleMatchMemberHttpMatch{Value: types.HttpMatch{
				PathMatch: &types.PathMatch{Match: &types.PathMatchTypeMemberPrefix{Value: "/a"}},
			}}
			_, err = c.CreateRule(ctx, &vpclatticesdk.CreateRuleInput{
				ServiceIdentifier: svc.Id, ListenerIdentifier: lis.Id, Name: aws.String("ruleone"),
				Priority: aws.Int32(10), Action: fwd, Match: match,
			})
			require.NoError(t, err)

			in := &vpclatticesdk.CreateRuleInput{
				ServiceIdentifier: svc.Id, ListenerIdentifier: lis.Id,
				Priority: aws.Int32(10), Action: fwd, Match: match,
			}
			tc.mutate(in)

			_, err = c.CreateRule(ctx, in)
			if tc.errCode == "" {
				require.NoError(t, err)

				return
			}

			var ae smithy.APIError
			require.ErrorAs(t, err, &ae)
			assert.Equal(t, tc.errCode, ae.ErrorCode())
		})
	}
}

func TestRealism_NotFoundNamesResource(t *testing.T) {
	t.Parallel()

	_, err := newRealClient(t).GetService(t.Context(), &vpclatticesdk.GetServiceInput{
		ServiceIdentifier: aws.String("svc-0123456789abcdef0"),
	})

	var nf *types.ResourceNotFoundException
	require.ErrorAs(t, err, &nf)
	assert.Contains(t, aws.ToString(nf.Message), "svc-0123456789abcdef0")
	assert.Equal(t, "svc-0123456789abcdef0", aws.ToString(nf.ResourceId))
	assert.Equal(t, "Service", aws.ToString(nf.ResourceType))
}

func TestRealism_LifecycleDelay(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := vpclattice.NewInMemoryBackend("000000000000", "us-east-1")
		b.SetLifecycleDelay(time.Minute)

		svc, err := b.CreateService(t.Context(), "svcone", "", "", "", 0, nil)
		require.NoError(t, err)
		assert.Equal(t, "CREATE_IN_PROGRESS", svc.Status)

		tg, err := b.CreateTargetGroup(t.Context(), "tgrp", "INSTANCE", nil, nil)
		require.NoError(t, err)
		assert.Equal(t, "CREATE_IN_PROGRESS", tg.Status)

		_, err = b.RegisterTargets(tg.ID, []*vpclattice.Target{{ID: "i-1", Port: 80}})
		require.NoError(t, err)

		items, _, err := b.ListTargets(t.Context(), tg.ID, nil, 0, "")
		require.NoError(t, err)
		require.Len(t, items, 1)
		assert.Equal(t, "INITIAL", items[0].Status)

		time.Sleep(time.Minute)

		got, err := b.GetService(svc.ID)
		require.NoError(t, err)
		assert.Equal(t, "ACTIVE", got.Status)

		gotTG, err := b.GetTargetGroup(tg.ID)
		require.NoError(t, err)
		assert.Equal(t, "ACTIVE", gotTG.Status)

		items, _, _ = b.ListTargets(t.Context(), tg.ID, nil, 0, "")
		assert.Equal(t, "HEALTHY", items[0].Status)

		_, err = b.DeregisterTargets(tg.ID, []*vpclattice.Target{{ID: "i-1", Port: 80}})
		require.NoError(t, err)

		items, _, _ = b.ListTargets(t.Context(), tg.ID, nil, 0, "")
		require.Len(t, items, 1)
		assert.Equal(t, "DRAINING", items[0].Status)

		out, err := b.DeleteService(svc.ID)
		require.NoError(t, err)
		assert.Equal(t, "DELETE_IN_PROGRESS", out.Status)

		time.Sleep(time.Minute)

		_, err = b.GetService(svc.ID)
		require.ErrorIs(t, err, vpclattice.ErrNotFound)

		items, _, _ = b.ListTargets(t.Context(), tg.ID, nil, 0, "")
		assert.Empty(t, items)
	})
}

func realismTGConfig() *types.TargetGroupConfig {
	return &types.TargetGroupConfig{
		Port: aws.Int32(80), Protocol: types.TargetGroupProtocolHttp, VpcIdentifier: aws.String("vpc-1"),
	}
}
