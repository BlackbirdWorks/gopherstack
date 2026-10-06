package glue_test

import (
	"context"
	"errors"
	"testing"

	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
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
		call   func(ctx context.Context, c *gluesdk.Client, v string) error
		name   string
		field  string
		values []string
	}{
		{
			name:   "CreateDevEndpoint.WorkerType",
			field:  "WorkerType",
			values: enumStrings(gluetypes.WorkerType("").Values()),
			call: func(ctx context.Context, c *gluesdk.Client, v string) error {
				in := &gluesdk.CreateDevEndpointInput{
					WorkerType: gluetypes.WorkerType(v),
				}
				_, err := c.CreateDevEndpoint(ctx, in)

				return err
			},
		},
		{
			name:   "CreateJob.ExecutionClass",
			field:  "ExecutionClass",
			values: enumStrings(gluetypes.ExecutionClass("").Values()),
			call: func(ctx context.Context, c *gluesdk.Client, v string) error {
				in := &gluesdk.CreateJobInput{
					ExecutionClass: gluetypes.ExecutionClass(v),
				}
				_, err := c.CreateJob(ctx, in)

				return err
			},
		},
		{
			name:   "CreateJob.JobMode",
			field:  "JobMode",
			values: enumStrings(gluetypes.JobMode("").Values()),
			call: func(ctx context.Context, c *gluesdk.Client, v string) error {
				in := &gluesdk.CreateJobInput{
					JobMode: gluetypes.JobMode(v),
				}
				_, err := c.CreateJob(ctx, in)

				return err
			},
		},
		{
			name:   "CreateJob.WorkerType",
			field:  "WorkerType",
			values: enumStrings(gluetypes.WorkerType("").Values()),
			call: func(ctx context.Context, c *gluesdk.Client, v string) error {
				in := &gluesdk.CreateJobInput{
					WorkerType: gluetypes.WorkerType(v),
				}
				_, err := c.CreateJob(ctx, in)

				return err
			},
		},
		{
			name:   "CreateMLTransform.WorkerType",
			field:  "WorkerType",
			values: enumStrings(gluetypes.WorkerType("").Values()),
			call: func(ctx context.Context, c *gluesdk.Client, v string) error {
				in := &gluesdk.CreateMLTransformInput{
					WorkerType: gluetypes.WorkerType(v),
				}
				_, err := c.CreateMLTransform(ctx, in)

				return err
			},
		},
		{
			name:   "CreateSession.WorkerType",
			field:  "WorkerType",
			values: enumStrings(gluetypes.WorkerType("").Values()),
			call: func(ctx context.Context, c *gluesdk.Client, v string) error {
				in := &gluesdk.CreateSessionInput{
					WorkerType: gluetypes.WorkerType(v),
				}
				_, err := c.CreateSession(ctx, in)

				return err
			},
		},
		{
			name:   "CreateTableOptimizer.Type",
			field:  "Type",
			values: enumStrings(gluetypes.TableOptimizerType("").Values()),
			call: func(ctx context.Context, c *gluesdk.Client, v string) error {
				in := &gluesdk.CreateTableOptimizerInput{
					Type: gluetypes.TableOptimizerType(v),
				}
				_, err := c.CreateTableOptimizer(ctx, in)

				return err
			},
		},
		{
			name:   "CreateTrigger.Type",
			field:  "Type",
			values: enumStrings(gluetypes.TriggerType("").Values()),
			call: func(ctx context.Context, c *gluesdk.Client, v string) error {
				in := &gluesdk.CreateTriggerInput{
					Type: gluetypes.TriggerType(v),
				}
				_, err := c.CreateTrigger(ctx, in)

				return err
			},
		},
		{
			name:   "DeleteTableOptimizer.Type",
			field:  "Type",
			values: enumStrings(gluetypes.TableOptimizerType("").Values()),
			call: func(ctx context.Context, c *gluesdk.Client, v string) error {
				in := &gluesdk.DeleteTableOptimizerInput{
					Type: gluetypes.TableOptimizerType(v),
				}
				_, err := c.DeleteTableOptimizer(ctx, in)

				return err
			},
		},
		{
			name:   "GetSchemaVersionsDiff.SchemaDiffType",
			field:  "SchemaDiffType",
			values: enumStrings(gluetypes.SchemaDiffType("").Values()),
			call: func(ctx context.Context, c *gluesdk.Client, v string) error {
				in := &gluesdk.GetSchemaVersionsDiffInput{
					SchemaDiffType: gluetypes.SchemaDiffType(v),
				}
				_, err := c.GetSchemaVersionsDiff(ctx, in)

				return err
			},
		},
		{
			name:   "GetTable.AttributesToGet",
			field:  "AttributesToGet",
			values: enumStrings(gluetypes.TableAttributes("").Values()),
			call: func(ctx context.Context, c *gluesdk.Client, v string) error {
				in := &gluesdk.GetTableInput{
					AttributesToGet: []gluetypes.TableAttributes{gluetypes.TableAttributes(v)},
				}
				_, err := c.GetTable(ctx, in)

				return err
			},
		},
		{
			name:   "GetTableOptimizer.Type",
			field:  "Type",
			values: enumStrings(gluetypes.TableOptimizerType("").Values()),
			call: func(ctx context.Context, c *gluesdk.Client, v string) error {
				in := &gluesdk.GetTableOptimizerInput{
					Type: gluetypes.TableOptimizerType(v),
				}
				_, err := c.GetTableOptimizer(ctx, in)

				return err
			},
		},
		{
			name:   "GetUnfilteredPartitionMetadata.SupportedPermissionTypes",
			field:  "SupportedPermissionTypes",
			values: enumStrings(gluetypes.PermissionType("").Values()),
			call: func(ctx context.Context, c *gluesdk.Client, v string) error {
				in := &gluesdk.GetUnfilteredPartitionMetadataInput{
					SupportedPermissionTypes: []gluetypes.PermissionType{gluetypes.PermissionType(v)},
				}
				_, err := c.GetUnfilteredPartitionMetadata(ctx, in)

				return err
			},
		},
		{
			name:   "GetUnfilteredPartitionsMetadata.SupportedPermissionTypes",
			field:  "SupportedPermissionTypes",
			values: enumStrings(gluetypes.PermissionType("").Values()),
			call: func(ctx context.Context, c *gluesdk.Client, v string) error {
				in := &gluesdk.GetUnfilteredPartitionsMetadataInput{
					SupportedPermissionTypes: []gluetypes.PermissionType{gluetypes.PermissionType(v)},
				}
				_, err := c.GetUnfilteredPartitionsMetadata(ctx, in)

				return err
			},
		},
		{
			name:   "GetUnfilteredTableMetadata.SupportedPermissionTypes",
			field:  "SupportedPermissionTypes",
			values: enumStrings(gluetypes.PermissionType("").Values()),
			call: func(ctx context.Context, c *gluesdk.Client, v string) error {
				in := &gluesdk.GetUnfilteredTableMetadataInput{
					SupportedPermissionTypes: []gluetypes.PermissionType{gluetypes.PermissionType(v)},
				}
				_, err := c.GetUnfilteredTableMetadata(ctx, in)

				return err
			},
		},
		{
			name:   "ListTableOptimizerRuns.Type",
			field:  "Type",
			values: enumStrings(gluetypes.TableOptimizerType("").Values()),
			call: func(ctx context.Context, c *gluesdk.Client, v string) error {
				in := &gluesdk.ListTableOptimizerRunsInput{
					Type: gluetypes.TableOptimizerType(v),
				}
				_, err := c.ListTableOptimizerRuns(ctx, in)

				return err
			},
		},
		{
			name:   "PutDataQualityProfileAnnotation.InclusionAnnotation",
			field:  "InclusionAnnotation",
			values: enumStrings(gluetypes.InclusionAnnotationValue("").Values()),
			call: func(ctx context.Context, c *gluesdk.Client, v string) error {
				in := &gluesdk.PutDataQualityProfileAnnotationInput{
					InclusionAnnotation: gluetypes.InclusionAnnotationValue(v),
				}
				_, err := c.PutDataQualityProfileAnnotation(ctx, in)

				return err
			},
		},
		{
			name:   "StartJobRun.ExecutionClass",
			field:  "ExecutionClass",
			values: enumStrings(gluetypes.ExecutionClass("").Values()),
			call: func(ctx context.Context, c *gluesdk.Client, v string) error {
				in := &gluesdk.StartJobRunInput{
					ExecutionClass: gluetypes.ExecutionClass(v),
				}
				_, err := c.StartJobRun(ctx, in)

				return err
			},
		},
		{
			name:   "StartJobRun.WorkerType",
			field:  "WorkerType",
			values: enumStrings(gluetypes.WorkerType("").Values()),
			call: func(ctx context.Context, c *gluesdk.Client, v string) error {
				in := &gluesdk.StartJobRunInput{
					WorkerType: gluetypes.WorkerType(v),
				}
				_, err := c.StartJobRun(ctx, in)

				return err
			},
		},
		{
			name:   "UpdateMLTransform.WorkerType",
			field:  "WorkerType",
			values: enumStrings(gluetypes.WorkerType("").Values()),
			call: func(ctx context.Context, c *gluesdk.Client, v string) error {
				in := &gluesdk.UpdateMLTransformInput{
					WorkerType: gluetypes.WorkerType(v),
				}
				_, err := c.UpdateMLTransform(ctx, in)

				return err
			},
		},
		{
			name:   "UpdateTableOptimizer.Type",
			field:  "Type",
			values: enumStrings(gluetypes.TableOptimizerType("").Values()),
			call: func(ctx context.Context, c *gluesdk.Client, v string) error {
				in := &gluesdk.UpdateTableOptimizerInput{
					Type: gluetypes.TableOptimizerType(v),
				}
				_, err := c.UpdateTableOptimizer(ctx, in)

				return err
			},
		},
	}

	base := newTestGlueClient(t, newTestHandler(t))
	client := gluesdk.New(base.Options(), func(o *gluesdk.Options) {
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

				var invalid *gluetypes.InvalidInputException

				require.ErrorAs(t, err, &invalid)
				assert.Contains(t, invalid.ErrorMessage(), "invalid "+tt.field)
			})

			t.Run("every sdk value accepted", func(t *testing.T) {
				t.Parallel()

				for _, v := range tt.values {
					err := tt.call(t.Context(), client, v)

					if invalid, ok := errors.AsType[*gluetypes.InvalidInputException](err); ok {
						assert.NotContains(t, invalid.ErrorMessage(), "invalid "+tt.field,
							"%s rejected sdk value %q: %v", tt.field, v, err)
					}
				}
			})
		})
	}
}
