package elbv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elbv2sdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDescribeSSLPoliciesAndAccountLimits_PageSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		op       string
		pageSize int32
		wantAll  int
	}{
		{name: "ssl policies pages of two", op: "ssl", pageSize: 2, wantAll: 45},
		{name: "ssl policies one page", op: "ssl", pageSize: 100, wantAll: 45},
		{name: "account limits pages of five", op: "limits", pageSize: 5, wantAll: 12},
		{name: "account limits one page", op: "limits", wantAll: 12},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestELBv2Backend(t)

			var pages, total int
			var marker *string
			seen := map[string]bool{}

			for {
				var size *int32
				if tt.pageSize > 0 {
					size = aws.Int32(tt.pageSize)
				}

				var next *string

				if tt.op == "ssl" {
					out, err := client.DescribeSSLPolicies(
						t.Context(),
						&elbv2sdk.DescribeSSLPoliciesInput{Marker: marker, PageSize: size},
					)
					require.NoError(t, err)

					for _, p := range out.SslPolicies {
						assert.False(t, seen[aws.ToString(p.Name)], "duplicate %s", aws.ToString(p.Name))
						seen[aws.ToString(p.Name)] = true
					}

					total += len(out.SslPolicies)
					next = out.NextMarker
				} else {
					out, err := client.DescribeAccountLimits(
						t.Context(),
						&elbv2sdk.DescribeAccountLimitsInput{Marker: marker, PageSize: size},
					)
					require.NoError(t, err)

					for _, l := range out.Limits {
						assert.False(t, seen[aws.ToString(l.Name)], "duplicate %s", aws.ToString(l.Name))
						seen[aws.ToString(l.Name)] = true
					}

					total += len(out.Limits)
					next = out.NextMarker
				}

				pages++

				if next == nil {
					break
				}

				marker = next
			}

			assert.Equal(t, tt.wantAll, total)

			if tt.pageSize > 0 && int(tt.pageSize) < tt.wantAll {
				assert.Greater(t, pages, 1)
			} else {
				assert.Equal(t, 1, pages)
			}
		})
	}
}
