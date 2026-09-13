package lambda_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lambdasdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_ConfigAndInvocation drives lambda's function config and
// invocation op families (gopherstack-n3zi) through the real aws-sdk-go-v2
// lambda client, one case per named priority family, asserting decoded
// values.
func TestRealClient_ConfigAndInvocation(t *testing.T) {
	t.Parallel()

	createFn := func(t *testing.T, client *lambdasdk.Client, name string) {
		t.Helper()

		_, err := client.CreateFunction(t.Context(), &lambdasdk.CreateFunctionInput{
			FunctionName: aws.String(name),
			PackageType:  types.PackageTypeImage,
			Code:         &types.FunctionCode{ImageUri: aws.String("ecr/myapp:latest")},
			Role:         aws.String("arn:aws:iam:::role/r"),
		})
		require.NoError(t, err)
	}

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "permissions",
			run: func(t *testing.T) {
				t.Helper()

				h, _ := newInMemoryHandler(t)
				client := newTestLambdaClient(t, h)
				createFn(t, client, "s9-perm-fn")

				addOut, err := client.AddPermission(t.Context(), &lambdasdk.AddPermissionInput{
					FunctionName: aws.String("s9-perm-fn"),
					StatementId:  aws.String("s9-stmt"),
					Action:       aws.String("lambda:InvokeFunction"),
					Principal:    aws.String("s3.amazonaws.com"),
					SourceArn:    aws.String("arn:aws:s3:::s9-bucket"),
				})
				require.NoError(t, err)
				assert.Contains(t, aws.ToString(addOut.Statement), "s9-stmt")

				getOut, err := client.GetPolicy(t.Context(), &lambdasdk.GetPolicyInput{
					FunctionName: aws.String("s9-perm-fn"),
				})
				require.NoError(t, err)
				assert.Contains(t, aws.ToString(getOut.Policy), "s9-stmt")

				_, err = client.RemovePermission(t.Context(), &lambdasdk.RemovePermissionInput{
					FunctionName: aws.String("s9-perm-fn"),
					StatementId:  aws.String("s9-stmt"),
				})
				require.NoError(t, err)

				_, err = client.GetPolicy(t.Context(), &lambdasdk.GetPolicyInput{
					FunctionName: aws.String("s9-perm-fn"),
				})
				require.Error(t, err)
			},
		},
		{
			name: "aliases",
			run: func(t *testing.T) {
				t.Helper()

				h, _ := newInMemoryHandler(t)
				client := newTestLambdaClient(t, h)
				createFn(t, client, "s9-alias-fn")

				_, err := client.PublishVersion(t.Context(), &lambdasdk.PublishVersionInput{
					FunctionName: aws.String("s9-alias-fn"),
				})
				require.NoError(t, err)

				_, err = client.CreateAlias(t.Context(), &lambdasdk.CreateAliasInput{
					FunctionName:    aws.String("s9-alias-fn"),
					Name:            aws.String("s9-alias"),
					FunctionVersion: aws.String("1"),
				})
				require.NoError(t, err)

				listOut, err := client.ListAliases(t.Context(), &lambdasdk.ListAliasesInput{
					FunctionName: aws.String("s9-alias-fn"),
				})
				require.NoError(t, err)
				require.Len(t, listOut.Aliases, 1)
				assert.Equal(t, "s9-alias", aws.ToString(listOut.Aliases[0].Name))

				_, err = client.DeleteAlias(t.Context(), &lambdasdk.DeleteAliasInput{
					FunctionName: aws.String("s9-alias-fn"),
					Name:         aws.String("s9-alias"),
				})
				require.NoError(t, err)

				listAfter, err := client.ListAliases(t.Context(), &lambdasdk.ListAliasesInput{
					FunctionName: aws.String("s9-alias-fn"),
				})
				require.NoError(t, err)
				assert.Empty(t, listAfter.Aliases)
			},
		},
		{
			name: "code signing configs",
			run: func(t *testing.T) {
				t.Helper()

				h, _ := newInMemoryHandler(t)
				client := newTestLambdaClient(t, h)
				createFn(t, client, "s9-csc-fn")

				createOut, err := client.CreateCodeSigningConfig(
					t.Context(),
					&lambdasdk.CreateCodeSigningConfigInput{
						AllowedPublishers: &types.AllowedPublishers{
							SigningProfileVersionArns: []string{
								"arn:aws:signer:us-east-1:000000000000:/signing-profiles/s9",
							},
						},
						Description: aws.String("s9 csc"),
					},
				)
				require.NoError(t, err)
				cscArn := aws.ToString(createOut.CodeSigningConfig.CodeSigningConfigArn)
				require.NotEmpty(t, cscArn)

				getOut, err := client.GetCodeSigningConfig(
					t.Context(),
					&lambdasdk.GetCodeSigningConfigInput{
						CodeSigningConfigArn: aws.String(cscArn),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, "s9 csc", aws.ToString(getOut.CodeSigningConfig.Description))

				updOut, err := client.UpdateCodeSigningConfig(
					t.Context(),
					&lambdasdk.UpdateCodeSigningConfigInput{
						CodeSigningConfigArn: aws.String(cscArn),
						Description:          aws.String("s9 csc updated"),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, "s9 csc updated", aws.ToString(updOut.CodeSigningConfig.Description))

				putOut, err := client.PutFunctionCodeSigningConfig(
					t.Context(),
					&lambdasdk.PutFunctionCodeSigningConfigInput{
						FunctionName:         aws.String("s9-csc-fn"),
						CodeSigningConfigArn: aws.String(cscArn),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, cscArn, aws.ToString(putOut.CodeSigningConfigArn))

				getFnOut, err := client.GetFunctionCodeSigningConfig(
					t.Context(),
					&lambdasdk.GetFunctionCodeSigningConfigInput{
						FunctionName: aws.String("s9-csc-fn"),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, cscArn, aws.ToString(getFnOut.CodeSigningConfigArn))

				_, err = client.DeleteFunctionCodeSigningConfig(
					t.Context(),
					&lambdasdk.DeleteFunctionCodeSigningConfigInput{FunctionName: aws.String("s9-csc-fn")},
				)
				require.NoError(t, err)

				_, err = client.DeleteCodeSigningConfig(
					t.Context(),
					&lambdasdk.DeleteCodeSigningConfigInput{
						CodeSigningConfigArn: aws.String(cscArn),
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name: "concurrency",
			run: func(t *testing.T) {
				t.Helper()

				h, _ := newInMemoryHandler(t)
				client := newTestLambdaClient(t, h)
				createFn(t, client, "s9-conc-fn")

				putOut, err := client.PutFunctionConcurrency(
					t.Context(),
					&lambdasdk.PutFunctionConcurrencyInput{
						FunctionName:                 aws.String("s9-conc-fn"),
						ReservedConcurrentExecutions: aws.Int32(3),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, int32(3), aws.ToInt32(putOut.ReservedConcurrentExecutions))

				getOut, err := client.GetFunctionConcurrency(
					t.Context(),
					&lambdasdk.GetFunctionConcurrencyInput{
						FunctionName: aws.String("s9-conc-fn"),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, int32(3), aws.ToInt32(getOut.ReservedConcurrentExecutions))

				_, err = client.DeleteFunctionConcurrency(
					t.Context(),
					&lambdasdk.DeleteFunctionConcurrencyInput{
						FunctionName: aws.String("s9-conc-fn"),
					},
				)
				require.NoError(t, err)

				getAfter, err := client.GetFunctionConcurrency(
					t.Context(),
					&lambdasdk.GetFunctionConcurrencyInput{
						FunctionName: aws.String("s9-conc-fn"),
					},
				)
				require.NoError(t, err)
				assert.Nil(t, getAfter.ReservedConcurrentExecutions)
			},
		},
		{
			name: "event invoke config",
			run: func(t *testing.T) {
				t.Helper()

				h, _ := newInMemoryHandler(t)
				client := newTestLambdaClient(t, h)
				createFn(t, client, "s9-eic-fn")

				putOut, err := client.PutFunctionEventInvokeConfig(
					t.Context(),
					&lambdasdk.PutFunctionEventInvokeConfigInput{
						FunctionName:             aws.String("s9-eic-fn"),
						MaximumRetryAttempts:     aws.Int32(1),
						MaximumEventAgeInSeconds: aws.Int32(60),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, int32(1), aws.ToInt32(putOut.MaximumRetryAttempts))

				getOut, err := client.GetFunctionEventInvokeConfig(
					t.Context(),
					&lambdasdk.GetFunctionEventInvokeConfigInput{
						FunctionName: aws.String("s9-eic-fn"),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, int32(60), aws.ToInt32(getOut.MaximumEventAgeInSeconds))

				updOut, err := client.UpdateFunctionEventInvokeConfig(
					t.Context(),
					&lambdasdk.UpdateFunctionEventInvokeConfigInput{
						FunctionName:         aws.String("s9-eic-fn"),
						MaximumRetryAttempts: aws.Int32(2),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, int32(2), aws.ToInt32(updOut.MaximumRetryAttempts))

				listOut, err := client.ListFunctionEventInvokeConfigs(
					t.Context(),
					&lambdasdk.ListFunctionEventInvokeConfigsInput{FunctionName: aws.String("s9-eic-fn")},
				)
				require.NoError(t, err)
				require.Len(t, listOut.FunctionEventInvokeConfigs, 1)

				_, err = client.DeleteFunctionEventInvokeConfig(
					t.Context(),
					&lambdasdk.DeleteFunctionEventInvokeConfigInput{FunctionName: aws.String("s9-eic-fn")},
				)
				require.NoError(t, err)
			},
		},
		{
			name: "function url configs",
			run: func(t *testing.T) {
				t.Helper()

				h, _ := newInMemoryHandler(t)
				client := newTestLambdaClient(t, h)
				createFn(t, client, "s9-url-fn")

				_, err := client.CreateFunctionUrlConfig(
					t.Context(),
					&lambdasdk.CreateFunctionUrlConfigInput{
						FunctionName: aws.String("s9-url-fn"),
						AuthType:     types.FunctionUrlAuthTypeNone,
					},
				)
				require.NoError(t, err)

				listOut, err := client.ListFunctionUrlConfigs(
					t.Context(),
					&lambdasdk.ListFunctionUrlConfigsInput{
						FunctionName: aws.String("s9-url-fn"),
					},
				)
				require.NoError(t, err)
				require.Len(t, listOut.FunctionUrlConfigs, 1)

				_, err = client.DeleteFunctionUrlConfig(
					t.Context(),
					&lambdasdk.DeleteFunctionUrlConfigInput{
						FunctionName: aws.String("s9-url-fn"),
					},
				)
				require.NoError(t, err)

				listAfter, err := client.ListFunctionUrlConfigs(
					t.Context(),
					&lambdasdk.ListFunctionUrlConfigsInput{
						FunctionName: aws.String("s9-url-fn"),
					},
				)
				require.NoError(t, err)
				assert.Empty(t, listAfter.FunctionUrlConfigs)
			},
		},
		{
			name: "account settings",
			run: func(t *testing.T) {
				t.Helper()

				h, _ := newInMemoryHandler(t)
				client := newTestLambdaClient(t, h)

				out, err := client.GetAccountSettings(t.Context(), &lambdasdk.GetAccountSettingsInput{})
				require.NoError(t, err)
				require.NotNil(t, out.AccountLimit)
				require.NotNil(t, out.AccountUsage)
			},
		},
		{
			name: "layers",
			run: func(t *testing.T) {
				t.Helper()

				h, _ := newInMemoryHandler(t)
				client := newTestLambdaClient(t, h)

				publishOut, err := client.PublishLayerVersion(
					t.Context(),
					&lambdasdk.PublishLayerVersionInput{
						LayerName: aws.String("s9-layer"),
						Content: &types.LayerVersionContentInput{
							ZipFile: []byte(
								"PK\x05\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
							),
						},
					},
				)
				require.NoError(t, err)
				layerVersionArn := aws.ToString(publishOut.LayerVersionArn)
				require.NotEmpty(t, layerVersionArn)

				getOut, err := client.GetLayerVersionByArn(
					t.Context(),
					&lambdasdk.GetLayerVersionByArnInput{
						Arn: aws.String(layerVersionArn),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, layerVersionArn, aws.ToString(getOut.LayerVersionArn))
			},
		},
		{
			name: "recursion config",
			run: func(t *testing.T) {
				t.Helper()

				h, _ := newInMemoryHandler(t)
				client := newTestLambdaClient(t, h)
				createFn(t, client, "s9-recursion-fn")

				putOut, err := client.PutFunctionRecursionConfig(
					t.Context(),
					&lambdasdk.PutFunctionRecursionConfigInput{
						FunctionName:  aws.String("s9-recursion-fn"),
						RecursiveLoop: types.RecursiveLoopAllow,
					},
				)
				require.NoError(t, err)
				assert.Equal(t, types.RecursiveLoopAllow, putOut.RecursiveLoop)

				getOut, err := client.GetFunctionRecursionConfig(
					t.Context(),
					&lambdasdk.GetFunctionRecursionConfigInput{
						FunctionName: aws.String("s9-recursion-fn"),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, types.RecursiveLoopAllow, getOut.RecursiveLoop)
			},
		},
		{
			name: "runtime management config",
			run: func(t *testing.T) {
				t.Helper()

				h, _ := newInMemoryHandler(t)
				client := newTestLambdaClient(t, h)
				createFn(t, client, "s9-runtime-fn")

				putOut, err := client.PutRuntimeManagementConfig(
					t.Context(),
					&lambdasdk.PutRuntimeManagementConfigInput{
						FunctionName:    aws.String("s9-runtime-fn"),
						UpdateRuntimeOn: types.UpdateRuntimeOnFunctionUpdate,
					},
				)
				require.NoError(t, err)
				assert.Equal(t, types.UpdateRuntimeOnFunctionUpdate, putOut.UpdateRuntimeOn)

				getOut, err := client.GetRuntimeManagementConfig(
					t.Context(),
					&lambdasdk.GetRuntimeManagementConfigInput{
						FunctionName: aws.String("s9-runtime-fn"),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, types.UpdateRuntimeOnFunctionUpdate, getOut.UpdateRuntimeOn)
			},
		},
		// Real RequestResponse invocation spins up an actual container via
		// pkgs/container/pkgs/portalloc; this backend is constructed with nil
		// allocators (matching every other unit test in this package -- real
		// container execution is test/integration's job, not a fast unit
		// test's), so only DryRun (skips execution) and the legacy
		// fire-and-forget InvokeAsync are exercised here for the real "sync/
		// async/dry-run" invoke family.

		{
			name: "invoke",
			run: func(t *testing.T) {
				t.Helper()

				h, _ := newInMemoryHandler(t)
				client := newTestLambdaClient(t, h)
				createFn(t, client, "s9-invoke-fn")

				dryRunOut, err := client.Invoke(t.Context(), &lambdasdk.InvokeInput{
					FunctionName:   aws.String("s9-invoke-fn"),
					InvocationType: types.InvocationTypeDryRun,
					Payload:        []byte(`{}`),
				})
				require.NoError(t, err)
				assert.Equal(t, int32(204), dryRunOut.StatusCode)

				// InvokeAsync is deprecated in favor of Invoke's InvocationType=Event,
				// but is still a real, dispatched op with its own uncovered-op entry --
				// deliberately exercised here, not an oversight.
				asyncOut, err := client.InvokeAsync( //nolint:staticcheck // deliberate legacy-op coverage
					t.Context(),
					&lambdasdk.InvokeAsyncInput{
						FunctionName: aws.String("s9-invoke-fn"),
						InvokeArgs:   strings.NewReader(`{"k":"v"}`),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, int32(202), asyncOut.Status)
			},
		},
		{
			name: "invoke with response stream",
			run: func(t *testing.T) {
				t.Helper()

				h, _ := newInMemoryHandler(t)
				client := newTestLambdaClient(t, h)
				createFn(t, client, "s9-stream-fn")

				out, err := client.InvokeWithResponseStream(
					t.Context(),
					&lambdasdk.InvokeWithResponseStreamInput{
						FunctionName:   aws.String("s9-stream-fn"),
						InvocationType: types.ResponseStreamingInvocationTypeDryRun,
						Payload:        []byte(`{"k":"v"}`),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, int32(204), out.StatusCode)
			},
		},
		{
			name: "event source mapping update",
			run: func(t *testing.T) {
				t.Helper()

				h, _ := newInMemoryHandler(t)
				client := newTestLambdaClient(t, h)
				createFn(t, client, "s9-esm-fn")

				createOut, err := client.CreateEventSourceMapping(
					t.Context(),
					&lambdasdk.CreateEventSourceMappingInput{
						FunctionName:   aws.String("s9-esm-fn"),
						EventSourceArn: aws.String("arn:aws:sqs:us-east-1:000000000000:s9-queue"),
						BatchSize:      aws.Int32(5),
					},
				)
				require.NoError(t, err)
				uuid := aws.ToString(createOut.UUID)
				require.NotEmpty(t, uuid)

				updOut, err := client.UpdateEventSourceMapping(
					t.Context(),
					&lambdasdk.UpdateEventSourceMappingInput{
						UUID:      aws.String(uuid),
						BatchSize: aws.Int32(9),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, int32(9), aws.ToInt32(updOut.BatchSize))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
