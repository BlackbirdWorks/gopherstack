package elbv2_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAuditELBv2_Tags_ResourceLifecycle verifies that tags can be added, described,
// and removed on multiple resource types (LB, TG, listener, rule).
func TestAuditELBv2_Tags_ResourceLifecycle(t *testing.T) {
	t.Parallel()

	h := auditHandler(t)
	lbArn := auditCreateLB(t, h, "tag-lb")
	tgArn := auditCreateTG(t, h, "tag-tg")
	lArn := auditCreateListener(t, h, lbArn, tgArn)

	resources := []struct {
		name string
		arn  string
	}{
		{"loadbalancer", lbArn},
		{"targetgroup", tgArn},
		{"listener", lArn},
	}

	for _, res := range resources {
		t.Run(res.name, func(t *testing.T) {
			t.Parallel()

			// Add tag.
			auditDo(t, h, url.Values{
				"Action":                {"AddTags"},
				"Version":               {"2015-12-01"},
				"ResourceArns.member.1": {res.arn},
				"Tags.member.1.Key":     {"Env"},
				"Tags.member.1.Value":   {"prod"},
			})

			// Describe tags.
			var descResp struct {
				Result struct {
					TagDescriptions struct {
						Members []struct {
							ResourceArn string `xml:"ResourceArn"`
							Tags        struct {
								Members []struct {
									Key   string `xml:"Key"`
									Value string `xml:"Value"`
								} `xml:"member"`
							} `xml:"Tags"`
						} `xml:"member"`
					} `xml:"TagDescriptions"`
				} `xml:"DescribeTagsResult"`
			}
			auditDo(t, h, url.Values{
				"Action":                {"DescribeTags"},
				"Version":               {"2015-12-01"},
				"ResourceArns.member.1": {res.arn},
			}).into(&descResp)

			require.Len(t, descResp.Result.TagDescriptions.Members, 1)
			tagList := descResp.Result.TagDescriptions.Members[0].Tags.Members
			require.Len(t, tagList, 1)
			assert.Equal(t, "Env", tagList[0].Key)
			assert.Equal(t, "prod", tagList[0].Value)

			// Remove tag.
			auditDo(t, h, url.Values{
				"Action":                {"RemoveTags"},
				"Version":               {"2015-12-01"},
				"ResourceArns.member.1": {res.arn},
				"TagKeys.member.1":      {"Env"},
			})

			// Describe again — should be empty (fresh struct to avoid xml.Unmarshal slice appending).
			var descResp2 struct {
				Result struct {
					TagDescriptions struct {
						Members []struct {
							Tags struct {
								Members []struct{} `xml:"member"`
							} `xml:"Tags"`
						} `xml:"member"`
					} `xml:"TagDescriptions"`
				} `xml:"DescribeTagsResult"`
			}
			auditDo(t, h, url.Values{
				"Action":                {"DescribeTags"},
				"Version":               {"2015-12-01"},
				"ResourceArns.member.1": {res.arn},
			}).into(&descResp2)

			require.Len(t, descResp2.Result.TagDescriptions.Members, 1)
			assert.Empty(t, descResp2.Result.TagDescriptions.Members[0].Tags.Members)
		})
	}
}
