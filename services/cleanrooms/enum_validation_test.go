package cleanrooms_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cleanroomssdk "github.com/aws/aws-sdk-go-v2/service/cleanrooms"
	crtypes "github.com/aws/aws-sdk-go-v2/service/cleanrooms/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func enumStrings[T ~string](vs []T) []string {
	out := make([]string, len(vs))
	for i, v := range vs {
		out[i] = string(v)
	}

	return out
}

func TestSDK_EnumInputValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call   func(ctx context.Context, c *cleanroomssdk.Client, v string) error
		name   string
		field  string
		values []string
	}{
		{
			name:   "CreateAnalysisTemplate.Format",
			field:  "format",
			values: enumStrings(crtypes.AnalysisFormat("").Values()),
			call: func(ctx context.Context, c *cleanroomssdk.Client, v string) error {
				in := &cleanroomssdk.CreateAnalysisTemplateInput{
					MembershipIdentifier: aws.String("x"),
					Name:                 aws.String("x"),
					Format:               crtypes.AnalysisFormat(v),
				}
				_, err := c.CreateAnalysisTemplate(ctx, in)

				return err
			},
		},
		{
			name:   "CreateCollaboration.CreatorMemberAbilities",
			field:  "creatorMemberAbilities",
			values: enumStrings(crtypes.MemberAbility("").Values()),
			call: func(ctx context.Context, c *cleanroomssdk.Client, v string) error {
				in := &cleanroomssdk.CreateCollaborationInput{
					CreatorDisplayName:     aws.String("x"),
					Name:                   aws.String("x"),
					CreatorMemberAbilities: []crtypes.MemberAbility{crtypes.MemberAbility(v)},
				}
				_, err := c.CreateCollaboration(ctx, in)

				return err
			},
		},
		{
			name:   "CreateCollaboration.QueryLogStatus",
			field:  "queryLogStatus",
			values: enumStrings(crtypes.CollaborationQueryLogStatus("").Values()),
			call: func(ctx context.Context, c *cleanroomssdk.Client, v string) error {
				in := &cleanroomssdk.CreateCollaborationInput{
					CreatorDisplayName: aws.String("x"),
					Name:               aws.String("x"),
					QueryLogStatus:     crtypes.CollaborationQueryLogStatus(v),
				}
				_, err := c.CreateCollaboration(ctx, in)

				return err
			},
		},
		{
			name:   "CreateCollaboration.JobLogStatus",
			field:  "jobLogStatus",
			values: enumStrings(crtypes.CollaborationJobLogStatus("").Values()),
			call: func(ctx context.Context, c *cleanroomssdk.Client, v string) error {
				in := &cleanroomssdk.CreateCollaborationInput{
					CreatorDisplayName: aws.String("x"),
					Name:               aws.String("x"),
					JobLogStatus:       crtypes.CollaborationJobLogStatus(v),
				}
				_, err := c.CreateCollaboration(ctx, in)

				return err
			},
		},
		{
			name:   "CreateConfiguredTableAnalysisRule.AnalysisRuleType",
			field:  "analysisRuleType",
			values: enumStrings(crtypes.ConfiguredTableAnalysisRuleType("").Values()),
			call: func(ctx context.Context, c *cleanroomssdk.Client, v string) error {
				in := &cleanroomssdk.CreateConfiguredTableAnalysisRuleInput{
					ConfiguredTableIdentifier: aws.String("x"),
					AnalysisRuleType:          crtypes.ConfiguredTableAnalysisRuleType(v),
				}
				_, err := c.CreateConfiguredTableAnalysisRule(ctx, in)

				return err
			},
		},
		{
			name:   "CreateConfiguredTableAssociationAnalysisRule.AnalysisRuleType",
			field:  "analysisRuleType",
			values: enumStrings(crtypes.ConfiguredTableAssociationAnalysisRuleType("").Values()),
			call: func(ctx context.Context, c *cleanroomssdk.Client, v string) error {
				in := &cleanroomssdk.CreateConfiguredTableAssociationAnalysisRuleInput{
					ConfiguredTableAssociationIdentifier: aws.String("x"),
					MembershipIdentifier:                 aws.String("x"),
					AnalysisRuleType:                     crtypes.ConfiguredTableAssociationAnalysisRuleType(v),
				}
				_, err := c.CreateConfiguredTableAssociationAnalysisRule(ctx, in)

				return err
			},
		},
		{
			name:   "CreateIntermediateTableAnalysisRule.AnalysisRuleType",
			field:  "analysisRuleType",
			values: enumStrings(crtypes.IntermediateTableAnalysisRuleType("").Values()),
			call: func(ctx context.Context, c *cleanroomssdk.Client, v string) error {
				in := &cleanroomssdk.CreateIntermediateTableAnalysisRuleInput{
					IntermediateTableIdentifier: aws.String("x"),
					MembershipIdentifier:        aws.String("x"),
					AnalysisRuleType:            crtypes.IntermediateTableAnalysisRuleType(v),
				}
				_, err := c.CreateIntermediateTableAnalysisRule(ctx, in)

				return err
			},
		},
		{
			name:   "CreateMembership.QueryLogStatus",
			field:  "queryLogStatus",
			values: enumStrings(crtypes.MembershipQueryLogStatus("").Values()),
			call: func(ctx context.Context, c *cleanroomssdk.Client, v string) error {
				in := &cleanroomssdk.CreateMembershipInput{
					CollaborationIdentifier: aws.String("x"),
					QueryLogStatus:          crtypes.MembershipQueryLogStatus(v),
				}
				_, err := c.CreateMembership(ctx, in)

				return err
			},
		},
		{
			name:   "CreateMembership.JobLogStatus",
			field:  "jobLogStatus",
			values: enumStrings(crtypes.MembershipJobLogStatus("").Values()),
			call: func(ctx context.Context, c *cleanroomssdk.Client, v string) error {
				in := &cleanroomssdk.CreateMembershipInput{
					CollaborationIdentifier: aws.String("x"),
					JobLogStatus:            crtypes.MembershipJobLogStatus(v),
				}
				_, err := c.CreateMembership(ctx, in)

				return err
			},
		},
		{
			name:   "CreatePrivacyBudgetTemplate.PrivacyBudgetType",
			field:  "privacyBudgetType",
			values: enumStrings(crtypes.PrivacyBudgetType("").Values()),
			call: func(ctx context.Context, c *cleanroomssdk.Client, v string) error {
				in := &cleanroomssdk.CreatePrivacyBudgetTemplateInput{
					MembershipIdentifier: aws.String("x"),
					PrivacyBudgetType:    crtypes.PrivacyBudgetType(v),
				}
				_, err := c.CreatePrivacyBudgetTemplate(ctx, in)

				return err
			},
		},
		{
			name:   "DeleteConfiguredTableAnalysisRule.AnalysisRuleType",
			field:  "analysisRuleType",
			values: enumStrings(crtypes.ConfiguredTableAnalysisRuleType("").Values()),
			call: func(ctx context.Context, c *cleanroomssdk.Client, v string) error {
				in := &cleanroomssdk.DeleteConfiguredTableAnalysisRuleInput{
					ConfiguredTableIdentifier: aws.String("x"),
					AnalysisRuleType:          crtypes.ConfiguredTableAnalysisRuleType(v),
				}
				_, err := c.DeleteConfiguredTableAnalysisRule(ctx, in)

				return err
			},
		},
		{
			name:   "DeleteConfiguredTableAssociationAnalysisRule.AnalysisRuleType",
			field:  "analysisRuleType",
			values: enumStrings(crtypes.ConfiguredTableAssociationAnalysisRuleType("").Values()),
			call: func(ctx context.Context, c *cleanroomssdk.Client, v string) error {
				in := &cleanroomssdk.DeleteConfiguredTableAssociationAnalysisRuleInput{
					ConfiguredTableAssociationIdentifier: aws.String("x"),
					MembershipIdentifier:                 aws.String("x"),
					AnalysisRuleType:                     crtypes.ConfiguredTableAssociationAnalysisRuleType(v),
				}
				_, err := c.DeleteConfiguredTableAssociationAnalysisRule(ctx, in)

				return err
			},
		},
		{
			name:   "DeleteIntermediateTableAnalysisRule.AnalysisRuleType",
			field:  "analysisRuleType",
			values: enumStrings(crtypes.IntermediateTableAnalysisRuleType("").Values()),
			call: func(ctx context.Context, c *cleanroomssdk.Client, v string) error {
				in := &cleanroomssdk.DeleteIntermediateTableAnalysisRuleInput{
					IntermediateTableIdentifier: aws.String("x"),
					MembershipIdentifier:        aws.String("x"),
					AnalysisRuleType:            crtypes.IntermediateTableAnalysisRuleType(v),
				}
				_, err := c.DeleteIntermediateTableAnalysisRule(ctx, in)

				return err
			},
		},
		{
			name:   "GetConfiguredTableAnalysisRule.AnalysisRuleType",
			field:  "analysisRuleType",
			values: enumStrings(crtypes.ConfiguredTableAnalysisRuleType("").Values()),
			call: func(ctx context.Context, c *cleanroomssdk.Client, v string) error {
				in := &cleanroomssdk.GetConfiguredTableAnalysisRuleInput{
					ConfiguredTableIdentifier: aws.String("x"),
					AnalysisRuleType:          crtypes.ConfiguredTableAnalysisRuleType(v),
				}
				_, err := c.GetConfiguredTableAnalysisRule(ctx, in)

				return err
			},
		},
		{
			name:   "GetConfiguredTableAssociationAnalysisRule.AnalysisRuleType",
			field:  "analysisRuleType",
			values: enumStrings(crtypes.ConfiguredTableAssociationAnalysisRuleType("").Values()),
			call: func(ctx context.Context, c *cleanroomssdk.Client, v string) error {
				in := &cleanroomssdk.GetConfiguredTableAssociationAnalysisRuleInput{
					ConfiguredTableAssociationIdentifier: aws.String("x"),
					MembershipIdentifier:                 aws.String("x"),
					AnalysisRuleType:                     crtypes.ConfiguredTableAssociationAnalysisRuleType(v),
				}
				_, err := c.GetConfiguredTableAssociationAnalysisRule(ctx, in)

				return err
			},
		},
		{
			name:   "GetSchemaAnalysisRule.Type",
			field:  "type",
			values: enumStrings(crtypes.AnalysisRuleType("").Values()),
			call: func(ctx context.Context, c *cleanroomssdk.Client, v string) error {
				in := &cleanroomssdk.GetSchemaAnalysisRuleInput{
					CollaborationIdentifier: aws.String("x"),
					Name:                    aws.String("x"),
					Type:                    crtypes.AnalysisRuleType(v),
				}
				_, err := c.GetSchemaAnalysisRule(ctx, in)

				return err
			},
		},
		{
			name:   "UpdateConfiguredTableAnalysisRule.AnalysisRuleType",
			field:  "analysisRuleType",
			values: enumStrings(crtypes.ConfiguredTableAnalysisRuleType("").Values()),
			call: func(ctx context.Context, c *cleanroomssdk.Client, v string) error {
				in := &cleanroomssdk.UpdateConfiguredTableAnalysisRuleInput{
					ConfiguredTableIdentifier: aws.String("x"),
					AnalysisRuleType:          crtypes.ConfiguredTableAnalysisRuleType(v),
				}
				_, err := c.UpdateConfiguredTableAnalysisRule(ctx, in)

				return err
			},
		},
		{
			name:   "UpdateConfiguredTableAssociationAnalysisRule.AnalysisRuleType",
			field:  "analysisRuleType",
			values: enumStrings(crtypes.ConfiguredTableAssociationAnalysisRuleType("").Values()),
			call: func(ctx context.Context, c *cleanroomssdk.Client, v string) error {
				in := &cleanroomssdk.UpdateConfiguredTableAssociationAnalysisRuleInput{
					ConfiguredTableAssociationIdentifier: aws.String("x"),
					MembershipIdentifier:                 aws.String("x"),
					AnalysisRuleType:                     crtypes.ConfiguredTableAssociationAnalysisRuleType(v),
				}
				_, err := c.UpdateConfiguredTableAssociationAnalysisRule(ctx, in)

				return err
			},
		},
		{
			name:   "UpdateIntermediateTableAnalysisRule.AnalysisRuleType",
			field:  "analysisRuleType",
			values: enumStrings(crtypes.IntermediateTableAnalysisRuleType("").Values()),
			call: func(ctx context.Context, c *cleanroomssdk.Client, v string) error {
				in := &cleanroomssdk.UpdateIntermediateTableAnalysisRuleInput{
					IntermediateTableIdentifier: aws.String("x"),
					MembershipIdentifier:        aws.String("x"),
					AnalysisRuleType:            crtypes.IntermediateTableAnalysisRuleType(v),
				}
				_, err := c.UpdateIntermediateTableAnalysisRule(ctx, in)

				return err
			},
		},
		{
			name:   "UpdateMembership.JobLogStatus",
			field:  "jobLogStatus",
			values: enumStrings(crtypes.MembershipJobLogStatus("").Values()),
			call: func(ctx context.Context, c *cleanroomssdk.Client, v string) error {
				in := &cleanroomssdk.UpdateMembershipInput{
					MembershipIdentifier: aws.String("x"),
					JobLogStatus:         crtypes.MembershipJobLogStatus(v),
				}
				_, err := c.UpdateMembership(ctx, in)

				return err
			},
		},
		{
			name:   "UpdateMembership.QueryLogStatus",
			field:  "queryLogStatus",
			values: enumStrings(crtypes.MembershipQueryLogStatus("").Values()),
			call: func(ctx context.Context, c *cleanroomssdk.Client, v string) error {
				in := &cleanroomssdk.UpdateMembershipInput{
					MembershipIdentifier: aws.String("x"),
					QueryLogStatus:       crtypes.MembershipQueryLogStatus(v),
				}
				_, err := c.UpdateMembership(ctx, in)

				return err
			},
		},
	}

	base := newRoundTripTestClient(t)
	client := cleanroomssdk.New(base.Options(), func(o *cleanroomssdk.Options) {
		o.APIOptions = append(o.APIOptions, func(s *middleware.Stack) error {
			_, _ = s.Initialize.Remove("OperationInputValidation")

			return nil
		})
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.NotEmpty(t, tt.values)

			t.Run("invalid", func(t *testing.T) {
				t.Parallel()

				err := tt.call(t.Context(), client, "NOT_A_REAL_VALUE")

				var invalid *crtypes.ValidationException

				require.ErrorAs(t, err, &invalid)
				assert.Contains(t, invalid.ErrorMessage(), "invalid "+tt.field)
			})

			t.Run("every sdk value accepted", func(t *testing.T) {
				t.Parallel()

				for _, v := range tt.values {
					err := tt.call(t.Context(), client, v)

					if invalid, ok := errors.AsType[*crtypes.ValidationException](err); ok {
						assert.NotContains(t, invalid.ErrorMessage(), "invalid "+tt.field,
							"%s rejected sdk value %q: %v", tt.field, v, err)
					}
				}
			})
		})
	}
}
