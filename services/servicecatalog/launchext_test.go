package servicecatalog_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/servicecatalog"
	sctypes "github.com/aws/aws-sdk-go-v2/service/servicecatalog/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errNotTrusted = errors.New("role does not trust servicecatalog.amazonaws.com")

type fakeRoles struct{ denied string }

func (f fakeRoles) CheckLaunchRole(_ context.Context, roleARN string) error {
	if roleARN == f.denied {
		return errNotTrusted
	}

	return nil
}

func addConstraint(t *testing.T, c *sdk.Client, cat catalog, typ, params string) {
	t.Helper()

	_, err := c.CreateConstraint(t.Context(), &sdk.CreateConstraintInput{
		PortfolioId: aws.String(cat.portfolioID), ProductId: aws.String(cat.productID),
		Type: aws.String(typ), Parameters: aws.String(params), IdempotencyToken: aws.String(token(t)),
	})
	require.NoError(t, err)
}

func TestLaunchRoleIsChecked(t *testing.T) {
	t.Parallel()

	const bad = "arn:aws:iam::123456789012:role/untrusted"

	tests := []struct {
		name     string
		role     string
		wantCode string
	}{
		{name: "trusted", role: "arn:aws:iam::123456789012:role/ok"},
		{name: "not_assumable", role: bad, wantCode: "InvalidParametersException"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			f.backend.SetRoleChecker(fakeRoles{denied: bad})
			c := f.client(t)
			cat := createCatalog(t, c, templateURL)
			addConstraint(t, c, cat, "LAUNCH", `{"RoleArn":"`+tc.role+`"}`)

			_, err := c.ProvisionProduct(t.Context(), &sdk.ProvisionProductInput{
				ProvisionToken:         aws.String(token(t)),
				ProvisionedProductName: aws.String("r"),
				ProductId:              aws.String(cat.productID),
				ProvisioningArtifactId: aws.String(cat.artifactID),
				ProvisioningParameters: []sctypes.ProvisioningParameter{
					{Key: aws.String("Size"), Value: aws.String("s")},
				},
			})
			assert.Equal(t, tc.wantCode, apiCode(err))

			if tc.wantCode != "" {
				assert.Contains(t, err.Error(), "cannot assume")

				list, lerr := c.ScanProvisionedProducts(t.Context(), &sdk.ScanProvisionedProductsInput{})
				require.NoError(t, lerr)
				assert.Empty(t, list.ProvisionedProducts, "a refused launch creates nothing")

				return
			}

			assert.Equal(t, tc.role, f.engine.lastRequest(t).RoleARN)
		})
	}
}

func TestTagOptionsBecomeStackTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		user   map[string]string
		want   map[string]string
		name   string
		values []string
		active bool
	}{
		{name: "single_value", values: []string{"prod"}, active: true, want: map[string]string{"env": "prod"}},
		{name: "user_wins", values: []string{"prod"}, active: true, user: map[string]string{"env": "dev"},
			want: map[string]string{"env": "dev"}},
		{name: "several_values_not_applied", values: []string{"a", "b"}, active: true, want: map[string]string{}},
		{name: "inactive", values: []string{"prod"}, active: false, want: map[string]string{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			c := f.client(t)
			cat := createCatalog(t, c, templateURL)
			ctx := t.Context()

			for i, v := range tc.values {
				target := []string{cat.portfolioID, cat.productID}[i]

				opt, err := c.CreateTagOption(
					ctx,
					&sdk.CreateTagOptionInput{Key: aws.String("env"), Value: aws.String(v)},
				)
				require.NoError(t, err)

				_, err = c.AssociateTagOptionWithResource(ctx, &sdk.AssociateTagOptionWithResourceInput{
					ResourceId: aws.String(target), TagOptionId: opt.TagOptionDetail.Id,
				})
				require.NoError(t, err)

				if !tc.active {
					_, err = c.UpdateTagOption(ctx, &sdk.UpdateTagOptionInput{
						Id: opt.TagOptionDetail.Id, Active: aws.Bool(false),
					})
					require.NoError(t, err)
				}
			}

			var tags []sctypes.Tag
			for k, v := range tc.user {
				tags = append(tags, sctypes.Tag{Key: aws.String(k), Value: aws.String(v)})
			}

			provision(t, c, cat, "to", map[string]string{"Size": "s"}, tags...)

			got := map[string]string{}

			for k, v := range f.engine.lastRequest(t).Tags {
				if !strings.HasPrefix(k, "aws:") && k == "env" {
					got[k] = v
				}
			}

			assert.Equal(t, tc.want, got)
			assert.Contains(t, f.engine.lastRequest(t).Tags, "aws:servicecatalog:productArn")
			assert.Contains(t, f.engine.lastRequest(t).Tags, "aws:servicecatalog:portfolioArn")
		})
	}
}

func TestDescribeProvisionedProductDashboards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		extra []string
		want  []string
	}{
		{name: "none", want: []string{}},
		{name: "two_dashboards", extra: []string{"ops", "billing"}, want: []string{"ops", "billing"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)

			for _, d := range tc.extra {
				f.engine.extra = append(f.engine.extra, stackResource(d, "AWS::CloudWatch::Dashboard"))
			}

			c := f.client(t)
			cat := createCatalog(t, c, templateURL)
			out := provision(t, c, cat, "dash", map[string]string{"Size": "s"})

			pp, err := c.DescribeProvisionedProduct(
				t.Context(), &sdk.DescribeProvisionedProductInput{Id: out.RecordDetail.ProvisionedProductId},
			)
			require.NoError(t, err)

			got := make([]string, 0, len(pp.CloudWatchDashboards))
			for _, d := range pp.CloudWatchDashboards {
				got = append(got, aws.ToString(d.Name))
			}

			assert.Equal(t, tc.want, got)
		})
	}
}
