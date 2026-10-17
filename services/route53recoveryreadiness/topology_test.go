package route53recoveryreadiness_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/route53recoveryreadiness"
	"github.com/aws/aws-sdk-go-v2/service/route53recoveryreadiness/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCellAndRecoveryGroupHierarchy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture)
		name string
	}{
		{name: "nested cells record their parent", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			child, err := f.client.CreateCell(ctx, &sdk.CreateCellInput{CellName: aws.String("child")})
			require.NoError(t, err)
			assert.Equal(t, "arn:aws:route53-recovery-readiness::123456789012:cell/child", aws.ToString(child.CellArn))
			assert.Empty(t, child.ParentReadinessScopes)

			parent, err := f.client.CreateCell(ctx, &sdk.CreateCellInput{
				CellName: aws.String(
					"parent",
				),
				Cells: []string{aws.ToString(child.CellArn)},
				Tags:  map[string]string{"t": "1"},
			})
			require.NoError(t, err)
			assert.Equal(t, []string{aws.ToString(child.CellArn)}, parent.Cells)

			got, err := f.client.GetCell(ctx, &sdk.GetCellInput{CellName: aws.String("child")})
			require.NoError(t, err)
			assert.Equal(t, []string{aws.ToString(parent.CellArn)}, got.ParentReadinessScopes)

			_, err = f.client.DeleteCell(ctx, &sdk.DeleteCellInput{CellName: aws.String("child")})
			assert.Equal(t, "ConflictException", apiCode(err), "a nested cell cannot be deleted")

			_, err = f.client.DeleteCell(ctx, &sdk.DeleteCellInput{CellName: aws.String("parent")})
			require.NoError(t, err)

			got, err = f.client.GetCell(ctx, &sdk.GetCellInput{CellName: aws.String("child")})
			require.NoError(t, err)
			assert.Empty(t, got.ParentReadinessScopes, "deleting the parent frees the child")
		}},
		{name: "a cell has one parent", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			child, err := f.client.CreateCell(ctx, &sdk.CreateCellInput{CellName: aws.String("child")})
			require.NoError(t, err)
			_, err = f.client.CreateCell(
				ctx,
				&sdk.CreateCellInput{CellName: aws.String("p1"), Cells: []string{aws.ToString(child.CellArn)}},
			)
			require.NoError(t, err)
			_, err = f.client.CreateCell(
				ctx,
				&sdk.CreateCellInput{CellName: aws.String("p2"), Cells: []string{aws.ToString(child.CellArn)}},
			)
			assert.Equal(t, "ConflictException", apiCode(err))
		}},
		{name: "cycles are rejected", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			a, err := f.client.CreateCell(ctx, &sdk.CreateCellInput{CellName: aws.String("a")})
			require.NoError(t, err)
			b, err := f.client.CreateCell(
				ctx,
				&sdk.CreateCellInput{CellName: aws.String("b"), Cells: []string{aws.ToString(a.CellArn)}},
			)
			require.NoError(t, err)
			_, err = f.client.UpdateCell(
				ctx,
				&sdk.UpdateCellInput{CellName: aws.String("a"), Cells: []string{aws.ToString(b.CellArn)}},
			)
			assert.Equal(t, "ValidationException", apiCode(err))
			_, err = f.client.UpdateCell(
				ctx,
				&sdk.UpdateCellInput{CellName: aws.String("a"), Cells: []string{aws.ToString(a.CellArn)}},
			)
			assert.Equal(t, "ValidationException", apiCode(err))
		}},
		{name: "update replaces the child list", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			c1, _ := f.client.CreateCell(ctx, &sdk.CreateCellInput{CellName: aws.String("c1")})
			c2, _ := f.client.CreateCell(ctx, &sdk.CreateCellInput{CellName: aws.String("c2")})
			_, err := f.client.CreateCell(
				ctx,
				&sdk.CreateCellInput{CellName: aws.String("p"), Cells: []string{aws.ToString(c1.CellArn)}},
			)
			require.NoError(t, err)

			out, err := f.client.UpdateCell(
				ctx,
				&sdk.UpdateCellInput{CellName: aws.String("p"), Cells: []string{aws.ToString(c2.CellArn)}},
			)
			require.NoError(t, err)
			assert.Equal(t, []string{aws.ToString(c2.CellArn)}, out.Cells)

			got, err := f.client.GetCell(ctx, &sdk.GetCellInput{CellName: aws.String("c1")})
			require.NoError(t, err)
			assert.Empty(t, got.ParentReadinessScopes)
		}},
		{name: "duplicate and invalid names and unknown children", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			_, err := f.client.CreateCell(ctx, &sdk.CreateCellInput{CellName: aws.String("x")})
			require.NoError(t, err)
			_, err = f.client.CreateCell(ctx, &sdk.CreateCellInput{CellName: aws.String("x")})
			assert.Equal(t, "ConflictException", apiCode(err))
			_, err = f.client.CreateCell(ctx, &sdk.CreateCellInput{CellName: aws.String("bad name!")})
			assert.Equal(t, "ValidationException", apiCode(err))
			_, err = f.client.CreateCell(
				ctx,
				&sdk.CreateCellInput{
					CellName: aws.String("y"),
					Cells:    []string{"arn:aws:route53-recovery-readiness::123456789012:cell/none"},
				},
			)
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))
			_, err = f.client.GetCell(ctx, &sdk.GetCellInput{CellName: aws.String("none")})
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))
		}},
		{name: "recovery group owns top-level cells", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			c1, _ := f.client.CreateCell(ctx, &sdk.CreateCellInput{CellName: aws.String("c1")})
			c2, _ := f.client.CreateCell(ctx, &sdk.CreateCellInput{CellName: aws.String("c2")})

			g, err := f.client.CreateRecoveryGroup(ctx, &sdk.CreateRecoveryGroupInput{
				RecoveryGroupName: aws.String("g"), Cells: []string{aws.ToString(c1.CellArn), aws.ToString(c2.CellArn)},
			})
			require.NoError(t, err)
			assert.Equal(
				t,
				"arn:aws:route53-recovery-readiness::123456789012:recovery-group/g",
				aws.ToString(g.RecoveryGroupArn),
			)

			got, err := f.client.GetCell(ctx, &sdk.GetCellInput{CellName: aws.String("c1")})
			require.NoError(t, err)
			assert.Equal(t, []string{aws.ToString(g.RecoveryGroupArn)}, got.ParentReadinessScopes)

			_, err = f.client.CreateRecoveryGroup(ctx, &sdk.CreateRecoveryGroupInput{
				RecoveryGroupName: aws.String("g2"), Cells: []string{aws.ToString(c1.CellArn)},
			})
			assert.Equal(t, "ConflictException", apiCode(err))

			upd, err := f.client.UpdateRecoveryGroup(ctx, &sdk.UpdateRecoveryGroupInput{
				RecoveryGroupName: aws.String("g"), Cells: []string{aws.ToString(c2.CellArn)},
			})
			require.NoError(t, err)
			assert.Equal(t, []string{aws.ToString(c2.CellArn)}, upd.Cells)

			_, err = f.client.DeleteCell(ctx, &sdk.DeleteCellInput{CellName: aws.String("c2")})
			assert.Equal(t, "ConflictException", apiCode(err))

			_, err = f.client.DeleteRecoveryGroup(
				ctx,
				&sdk.DeleteRecoveryGroupInput{RecoveryGroupName: aws.String("g")},
			)
			require.NoError(t, err)
			_, err = f.client.DeleteCell(ctx, &sdk.DeleteCellInput{CellName: aws.String("c2")})
			require.NoError(t, err)
		}},
		{name: "lists paginate and sort", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			for _, n := range []string{"c3", "c1", "c2"} {
				_, err := f.client.CreateCell(ctx, &sdk.CreateCellInput{CellName: aws.String(n)})
				require.NoError(t, err)
			}

			p := sdk.NewListCellsPaginator(f.client, &sdk.ListCellsInput{MaxResults: aws.Int32(2)})
			var names []string

			for p.HasMorePages() {
				pg, err := p.NextPage(ctx)
				require.NoError(t, err)

				for _, c := range pg.Cells {
					names = append(names, aws.ToString(c.CellName))
				}
			}

			assert.Equal(t, []string{"c1", "c2", "c3"}, names)

			_, err := f.client.ListCells(ctx, &sdk.ListCellsInput{NextToken: aws.String("!!not-a-token")})
			assert.Equal(t, "ValidationException", apiCode(err))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, newFixture(t))
		})
	}
}

func TestResourceSetsAndChecks(t *testing.T) {
	t.Parallel()

	arnRes := func(a string) []types.Resource { return []types.Resource{{ResourceArn: aws.String(a)}} }

	tests := []struct {
		run  func(t *testing.T, f *fixture)
		name string
	}{
		{name: "resource set round trip", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			out, err := f.client.CreateResourceSet(ctx, &sdk.CreateResourceSetInput{
				ResourceSetName: aws.String("rs"), ResourceSetType: aws.String("AWS::SQS::Queue"),
				Resources: []types.Resource{{ResourceArn: aws.String(queueARN("q")), ReadinessScopes: []string{"s"}}},
				Tags:      map[string]string{"k": "v"},
			})
			require.NoError(t, err)
			assert.Equal(
				t,
				"arn:aws:route53-recovery-readiness::123456789012:resource-set/rs",
				aws.ToString(out.ResourceSetArn),
			)

			got, err := f.client.GetResourceSet(ctx, &sdk.GetResourceSetInput{ResourceSetName: aws.String("rs")})
			require.NoError(t, err)
			require.Len(t, got.Resources, 1)
			assert.Equal(t, queueARN("q"), aws.ToString(got.Resources[0].ResourceArn))
			assert.Equal(t, []string{"s"}, got.Resources[0].ReadinessScopes)

			upd, err := f.client.UpdateResourceSet(ctx, &sdk.UpdateResourceSetInput{
				ResourceSetName: aws.String(
					"rs",
				),
				ResourceSetType: aws.String("AWS::SNS::Topic"),
				Resources:       arnRes("arn:aws:sns:us-east-1:123456789012:t"),
			})
			require.NoError(t, err)
			assert.Equal(t, "AWS::SNS::Topic", aws.ToString(upd.ResourceSetType))

			list, err := f.client.ListResourceSets(ctx, &sdk.ListResourceSetsInput{})
			require.NoError(t, err)
			assert.Len(t, list.ResourceSets, 1)
		}},
		{name: "resource set validation", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			bad := []*sdk.CreateResourceSetInput{
				{
					ResourceSetName: aws.String("a"),
					ResourceSetType: aws.String("AWS::Nope::Thing"),
					Resources:       arnRes("arn:aws:x:::y"),
				},
				{
					ResourceSetName: aws.String("b"),
					ResourceSetType: aws.String("AWS::SQS::Queue"),
					Resources:       arnRes("not-an-arn"),
				},
				{
					ResourceSetName: aws.String("c"),
					ResourceSetType: aws.String("AWS::SQS::Queue"),
					Resources: []types.Resource{
						{ResourceArn: aws.String(queueARN("q"))}, {ResourceArn: aws.String(queueARN("q"))},
					},
				},
				{
					ResourceSetName: aws.String("d"),
					ResourceSetType: aws.String("AWS::Route53RecoveryReadiness::DNSTargetResource"),
					Resources:       arnRes(queueARN("q")),
				},
				{
					ResourceSetName: aws.String("e"),
					ResourceSetType: aws.String("AWS::SQS::Queue"),
					Resources: []types.Resource{
						{DnsTargetResource: &types.DNSTargetResource{DomainName: aws.String("x.example.com")}},
					},
				},
			}
			for _, in := range bad {
				_, err := f.client.CreateResourceSet(ctx, in)
				assert.Equal(t, "ValidationException", apiCode(err), aws.ToString(in.ResourceSetName))
			}
		}},
		{name: "dns target gets a component id", run: func(t *testing.T, f *fixture) {
			t.Helper()

			out, err := f.client.CreateResourceSet(t.Context(), &sdk.CreateResourceSetInput{
				ResourceSetName: aws.String(
					"dns",
				),
				ResourceSetType: aws.String("AWS::Route53RecoveryReadiness::DNSTargetResource"),
				Resources: []types.Resource{{DnsTargetResource: &types.DNSTargetResource{
					DomainName: aws.String(
						"app.example.com",
					),
					RecordType:    aws.String("A"),
					RecordSetId:   aws.String("primary"),
					HostedZoneArn: aws.String("arn:aws:route53:::hostedzone/Z1"),
				}}},
			})
			require.NoError(t, err)
			assert.NotEmpty(t, aws.ToString(out.Resources[0].ComponentId))
		}},
		{name: "readiness check lifecycle", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			for _, n := range []string{"rs1", "rs2"} {
				_, err := f.client.CreateResourceSet(ctx, &sdk.CreateResourceSetInput{
					ResourceSetName: aws.String(
						n,
					),
					ResourceSetType: aws.String("AWS::SQS::Queue"),
					Resources:       arnRes(queueARN(n)),
				})
				require.NoError(t, err)
			}

			_, err := f.client.CreateReadinessCheck(
				ctx,
				&sdk.CreateReadinessCheckInput{
					ReadinessCheckName: aws.String("rc"),
					ResourceSetName:    aws.String("nope"),
				},
			)
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))

			out, err := f.client.CreateReadinessCheck(
				ctx,
				&sdk.CreateReadinessCheckInput{
					ReadinessCheckName: aws.String("rc"),
					ResourceSetName:    aws.String("rs1"),
				},
			)
			require.NoError(t, err)
			assert.Equal(t, "rs1", aws.ToString(out.ResourceSet))

			_, err = f.client.CreateReadinessCheck(
				ctx,
				&sdk.CreateReadinessCheckInput{
					ReadinessCheckName: aws.String("rc"),
					ResourceSetName:    aws.String("rs1"),
				},
			)
			assert.Equal(t, "ConflictException", apiCode(err))

			_, err = f.client.DeleteResourceSet(ctx, &sdk.DeleteResourceSetInput{ResourceSetName: aws.String("rs1")})
			assert.Equal(t, "ConflictException", apiCode(err), "a resource set used by a check cannot be deleted")

			upd, err := f.client.UpdateReadinessCheck(
				ctx,
				&sdk.UpdateReadinessCheckInput{
					ReadinessCheckName: aws.String("rc"),
					ResourceSetName:    aws.String("rs2"),
				},
			)
			require.NoError(t, err)
			assert.Equal(t, "rs2", aws.ToString(upd.ResourceSet))

			list, err := f.client.ListReadinessChecks(ctx, &sdk.ListReadinessChecksInput{})
			require.NoError(t, err)
			assert.Len(t, list.ReadinessChecks, 1)

			_, err = f.client.DeleteReadinessCheck(
				ctx,
				&sdk.DeleteReadinessCheckInput{ReadinessCheckName: aws.String("rc")},
			)
			require.NoError(t, err)
			_, err = f.client.DeleteResourceSet(ctx, &sdk.DeleteResourceSetInput{ResourceSetName: aws.String("rs1")})
			require.NoError(t, err)
		}},
		{name: "tags", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			c, err := f.client.CreateCell(
				ctx,
				&sdk.CreateCellInput{CellName: aws.String("c"), Tags: map[string]string{"a": "1"}},
			)
			require.NoError(t, err)

			_, err = f.client.TagResource(
				ctx,
				&sdk.TagResourceInput{ResourceArn: c.CellArn, Tags: map[string]string{"b": "2"}},
			)
			require.NoError(t, err)
			_, err = f.client.UntagResource(
				ctx,
				&sdk.UntagResourceInput{ResourceArn: c.CellArn, TagKeys: []string{"a"}},
			)
			require.NoError(t, err)

			out, err := f.client.ListTagsForResources(ctx, &sdk.ListTagsForResourcesInput{ResourceArn: c.CellArn})
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"b": "2"}, out.Tags)

			_, err = f.client.TagResource(
				ctx,
				&sdk.TagResourceInput{
					ResourceArn: aws.String("arn:aws:route53-recovery-readiness::123456789012:cell/none"),
					Tags:        map[string]string{"x": "y"},
				},
			)
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))
		}},
		{name: "cross account authorizations", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			for range 2 {
				_, err := f.client.CreateCrossAccountAuthorization(
					ctx,
					&sdk.CreateCrossAccountAuthorizationInput{
						CrossAccountAuthorization: aws.String("arn:aws:iam::999999999999:root"),
					},
				)
				require.NoError(t, err)
			}

			list, err := f.client.ListCrossAccountAuthorizations(ctx, &sdk.ListCrossAccountAuthorizationsInput{})
			require.NoError(t, err)
			assert.Equal(t, []string{"arn:aws:iam::999999999999:root"}, list.CrossAccountAuthorizations)

			_, err = f.client.DeleteCrossAccountAuthorization(
				ctx,
				&sdk.DeleteCrossAccountAuthorizationInput{
					CrossAccountAuthorization: aws.String("arn:aws:iam::999999999999:root"),
				},
			)
			require.NoError(t, err)
			_, err = f.client.DeleteCrossAccountAuthorization(
				ctx,
				&sdk.DeleteCrossAccountAuthorizationInput{
					CrossAccountAuthorization: aws.String("arn:aws:iam::999999999999:root"),
				},
			)
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, newFixture(t))
		})
	}
}
