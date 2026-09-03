package elbv2_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elbv2sdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	elbv2types "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

const unknownTGArn = "arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/ghost/0123456789abcdef"

// AWS: DeleteTargetGroup's own error switch models only ResourceInUse -- no
// TargetGroupNotFound -- so it is idempotent on a missing target group.
func Test_SDKRoundTrip_DeleteTargetGroup_UnknownArn_Idempotent(t *testing.T) {
	t.Parallel()

	b := elbv2.NewInMemoryBackend("123456789012", "us-east-1")
	h := elbv2.NewHandler(b)
	client := newTestELBv2Client(t, h)

	_, err := client.DeleteTargetGroup(t.Context(), &elbv2sdk.DeleteTargetGroupInput{
		TargetGroupArn: aws.String(unknownTGArn),
	})
	require.NoError(t, err, "DeleteTargetGroup must be idempotent on a missing target group")
}

// AWS: AddTags/RemoveTags/DescribeTags each model
// LoadBalancerNotFound/TargetGroupNotFound/ListenerNotFound/RuleNotFound/
// TrustStoreNotFound for a resource ARN that does not exist. The backend
// previously silently skipped unknown ARNs (AddTags/RemoveTags no-op'd,
// DescribeTags returned an empty tag list) instead of raising.
func Test_SDKRoundTrip_Tags_UnknownResourceArn_NotFound(t *testing.T) {
	t.Parallel()

	t.Run("AddTags", func(t *testing.T) {
		t.Parallel()

		b := elbv2.NewInMemoryBackend("123456789012", "us-east-1")
		h := elbv2.NewHandler(b)
		client := newTestELBv2Client(t, h)

		_, err := client.AddTags(t.Context(), &elbv2sdk.AddTagsInput{
			ResourceArns: []string{unknownTGArn},
			Tags:         []elbv2types.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
		})
		require.Error(t, err)

		var tgnf *elbv2types.TargetGroupNotFoundException
		require.ErrorAs(t, err, &tgnf, "expected a real TargetGroupNotFoundException from the SDK deserializer")
	})

	t.Run("RemoveTags", func(t *testing.T) {
		t.Parallel()

		b := elbv2.NewInMemoryBackend("123456789012", "us-east-1")
		h := elbv2.NewHandler(b)
		client := newTestELBv2Client(t, h)

		_, err := client.RemoveTags(t.Context(), &elbv2sdk.RemoveTagsInput{
			ResourceArns: []string{unknownTGArn},
			TagKeys:      []string{"k"},
		})
		require.Error(t, err)

		var tgnf *elbv2types.TargetGroupNotFoundException
		require.ErrorAs(t, err, &tgnf, "expected a real TargetGroupNotFoundException from the SDK deserializer")
	})

	t.Run("DescribeTags", func(t *testing.T) {
		t.Parallel()

		b := elbv2.NewInMemoryBackend("123456789012", "us-east-1")
		h := elbv2.NewHandler(b)
		client := newTestELBv2Client(t, h)

		_, err := client.DescribeTags(t.Context(), &elbv2sdk.DescribeTagsInput{
			ResourceArns: []string{unknownTGArn},
		})
		require.Error(t, err)

		var tgnf *elbv2types.TargetGroupNotFoundException
		require.ErrorAs(t, err, &tgnf, "expected a real TargetGroupNotFoundException from the SDK deserializer")
	})
}

// AWS: CreateListener/ModifyListener/CreateRule/ModifyRule each model
// TargetGroupNotFound for a forward action referencing a target group that
// does not exist. The backend previously never checked forward-action
// target group references at all, so a listener or rule could be created
// (or modified) pointing at a target group that was never created --
// missing-error: success where AWS raises.
func Test_SDKRoundTrip_ForwardAction_UnknownTargetGroup_NotFound(t *testing.T) {
	t.Parallel()

	t.Run("CreateListener", func(t *testing.T) {
		t.Parallel()

		b := elbv2.NewInMemoryBackend("123456789012", "us-east-1")
		h := elbv2.NewHandler(b)
		client := newTestELBv2Client(t, h)
		lbArn := mustCreateLB(t, h, "cl-fwd-tg-lb")

		_, err := client.CreateListener(t.Context(), &elbv2sdk.CreateListenerInput{
			LoadBalancerArn: aws.String(lbArn),
			Protocol:        elbv2types.ProtocolEnumHttp,
			Port:            aws.Int32(80),
			DefaultActions: []elbv2types.Action{{
				Type:           elbv2types.ActionTypeEnumForward,
				TargetGroupArn: aws.String(unknownTGArn),
			}},
		})
		require.Error(t, err)

		var tgnf *elbv2types.TargetGroupNotFoundException
		require.ErrorAs(t, err, &tgnf, "expected a real TargetGroupNotFoundException from the SDK deserializer")
	})

	t.Run("ModifyListener", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler()
		client := newTestELBv2Client(t, h)
		lbArn := mustCreateLB(t, h, "ml-fwd-tg-lb")
		tgArn := mustCreateTG(t, h, "ml-fwd-tg")
		listenerArn := mustCreateListener(t, h, lbArn, tgArn)

		_, err := client.ModifyListener(t.Context(), &elbv2sdk.ModifyListenerInput{
			ListenerArn: aws.String(listenerArn),
			DefaultActions: []elbv2types.Action{{
				Type:           elbv2types.ActionTypeEnumForward,
				TargetGroupArn: aws.String(unknownTGArn),
			}},
		})
		require.Error(t, err)

		var tgnf *elbv2types.TargetGroupNotFoundException
		require.ErrorAs(t, err, &tgnf, "expected a real TargetGroupNotFoundException from the SDK deserializer")
	})

	t.Run("CreateRule", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler()
		client := newTestELBv2Client(t, h)
		lbArn := mustCreateLB(t, h, "cr-fwd-tg-lb")
		tgArn := mustCreateTG(t, h, "cr-fwd-tg")
		listenerArn := mustCreateListener(t, h, lbArn, tgArn)

		_, err := client.CreateRule(t.Context(), &elbv2sdk.CreateRuleInput{
			ListenerArn: aws.String(listenerArn),
			Priority:    aws.Int32(1),
			Conditions: []elbv2types.RuleCondition{{
				Field:  aws.String("path-pattern"),
				Values: []string{"/foo"},
			}},
			Actions: []elbv2types.Action{{
				Type:           elbv2types.ActionTypeEnumForward,
				TargetGroupArn: aws.String(unknownTGArn),
			}},
		})
		require.Error(t, err)

		var tgnf *elbv2types.TargetGroupNotFoundException
		require.ErrorAs(t, err, &tgnf, "expected a real TargetGroupNotFoundException from the SDK deserializer")
	})

	t.Run("ModifyRule", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler()
		lbArn := mustCreateLB(t, h, "mr-fwd-tg-lb")
		tgArn := mustCreateTG(t, h, "mr-fwd-tg")
		listenerArn := mustCreateListener(t, h, lbArn, tgArn)

		createRec := doELBv2(t, h, url.Values{
			"Action":                              {"CreateRule"},
			"Version":                             {"2015-12-01"},
			"ListenerArn":                         {listenerArn},
			"Priority":                            {"5"},
			"Conditions.member.1.Field":           {"path-pattern"},
			"Conditions.member.1.Values.member.1": {"/bar"},
			"Actions.member.1.Type":               {"forward"},
			"Actions.member.1.TargetGroupArn":     {tgArn},
		})
		require.Equal(t, http.StatusOK, createRec.Code)

		var createResp struct {
			Result struct {
				Rules struct {
					Members []struct {
						RuleArn string `xml:"RuleArn"`
					} `xml:"member"`
				} `xml:"Rules"`
			} `xml:"CreateRuleResult"`
		}
		parseXMLBody(t, createRec, &createResp)
		require.Len(t, createResp.Result.Rules.Members, 1)
		ruleArn := createResp.Result.Rules.Members[0].RuleArn

		modifyRec := doELBv2(t, h, url.Values{
			"Action":                          {"ModifyRule"},
			"Version":                         {"2015-12-01"},
			"RuleArn":                         {ruleArn},
			"Actions.member.1.Type":           {"forward"},
			"Actions.member.1.TargetGroupArn": {unknownTGArn},
		})
		assert.Equal(t, http.StatusBadRequest, modifyRec.Code)
	})
}
