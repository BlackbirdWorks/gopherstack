package xray_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	xraysdk "github.com/aws/aws-sdk-go-v2/service/xray"
	xraytypes "github.com/aws/aws-sdk-go-v2/service/xray/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateTags_SDK(t *testing.T) {
	t.Parallel()

	cases := []struct {
		create func(t *testing.T, c *xraysdk.Client, tags []xraytypes.Tag) (string, error)
		name   string
		tags   int
		fail   bool
	}{
		{
			name: "group",
			tags: 2,
			create: func(t *testing.T, c *xraysdk.Client, tags []xraytypes.Tag) (string, error) {
				t.Helper()
				out, err := c.CreateGroup(t.Context(), &xraysdk.CreateGroupInput{
					GroupName: aws.String("g1"), Tags: tags,
				})
				if err != nil {
					return "", err
				}

				return aws.ToString(out.Group.GroupARN), nil
			},
		},
		{
			name: "sampling rule",
			tags: 1,
			create: func(t *testing.T, c *xraysdk.Client, tags []xraytypes.Tag) (string, error) {
				t.Helper()
				out, err := c.CreateSamplingRule(t.Context(), &xraysdk.CreateSamplingRuleInput{
					SamplingRule: &xraytypes.SamplingRule{
						RuleName: aws.String("r1"), ResourceARN: aws.String("*"), Priority: aws.Int32(10),
						FixedRate: 0.1, ReservoirSize: 1, ServiceName: aws.String("*"),
						ServiceType: aws.String("*"), Host: aws.String("*"), HTTPMethod: aws.String("*"),
						URLPath: aws.String("*"), Version: aws.Int32(1),
					},
					Tags: tags,
				})
				if err != nil {
					return "", err
				}

				return aws.ToString(out.SamplingRuleRecord.SamplingRule.RuleARN), nil
			},
		},
		{
			name: "group too many tags",
			tags: 51,
			fail: true,
			create: func(t *testing.T, c *xraysdk.Client, tags []xraytypes.Tag) (string, error) {
				t.Helper()
				out, err := c.CreateGroup(t.Context(), &xraysdk.CreateGroupInput{
					GroupName: aws.String("g2"), Tags: tags,
				})
				if err != nil {
					return "", err
				}

				return aws.ToString(out.Group.GroupARN), nil
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestXRayClient(t)
			tags := make([]xraytypes.Tag, 0, tc.tags)
			for i := range tc.tags {
				tags = append(tags, xraytypes.Tag{Key: aws.String(fmt.Sprintf("k%d", i)), Value: aws.String("v")})
			}

			arn, err := tc.create(t, c, tags)
			if tc.fail {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			got, err := c.ListTagsForResource(t.Context(), &xraysdk.ListTagsForResourceInput{
				ResourceARN: aws.String(arn),
			})
			require.NoError(t, err)
			assert.Len(t, got.Tags, tc.tags)
		})
	}
}
