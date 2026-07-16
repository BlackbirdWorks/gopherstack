package elasticache_test

import (
	"testing"

	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	elasticachetypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCacheParameterGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(t *testing.T, client *elasticachesdk.Client)
		name        string
		pgName      string
		family      string
		description string
		wantErr     bool
		wantCount   int
	}{
		{
			name:        "create_success",
			pgName:      "my-pg",
			family:      "redis7",
			description: "test param group",
		},
		{
			name:   "create_already_exists",
			pgName: "dup-pg",
			family: "redis7",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheParameterGroup(t.Context(), &elasticachesdk.CreateCacheParameterGroupInput{
					CacheParameterGroupName:   aws.String("dup-pg"),
					CacheParameterGroupFamily: aws.String("redis7"),
					Description:               aws.String("first"),
				})
				require.NoError(t, err)
			},
			wantErr: true,
		},
		{
			name:      "describe_all_includes_defaults",
			wantCount: 8, // 8 default parameter groups seeded
		},
		{
			name:   "describe_specific",
			pgName: "my-specific-pg",
			family: "redis7",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheParameterGroup(t.Context(), &elasticachesdk.CreateCacheParameterGroupInput{
					CacheParameterGroupName:   aws.String("my-specific-pg"),
					CacheParameterGroupFamily: aws.String("redis7"),
					Description:               aws.String("specific"),
				})
				require.NoError(t, err)
			},
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			if tt.pgName != "" && tt.setup == nil {
				out, err := client.CreateCacheParameterGroup(
					t.Context(),
					&elasticachesdk.CreateCacheParameterGroupInput{
						CacheParameterGroupName:   aws.String(tt.pgName),
						CacheParameterGroupFamily: aws.String(tt.family),
						Description:               aws.String(tt.description),
					},
				)

				if tt.wantErr {
					require.Error(t, err)

					return
				}

				require.NoError(t, err)
				require.NotNil(t, out.CacheParameterGroup)
				assert.Equal(t, tt.pgName, aws.ToString(out.CacheParameterGroup.CacheParameterGroupName))
				assert.Equal(t, tt.family, aws.ToString(out.CacheParameterGroup.CacheParameterGroupFamily))

				return
			}

			if tt.wantErr {
				_, err := client.CreateCacheParameterGroup(t.Context(), &elasticachesdk.CreateCacheParameterGroupInput{
					CacheParameterGroupName:   aws.String(tt.pgName),
					CacheParameterGroupFamily: aws.String(tt.family),
					Description:               aws.String(tt.description),
				})
				require.Error(t, err)

				return
			}

			if tt.wantCount > 0 {
				var pgName *string
				if tt.pgName != "" {
					pgName = aws.String(tt.pgName)
				}
				out, err := client.DescribeCacheParameterGroups(
					t.Context(),
					&elasticachesdk.DescribeCacheParameterGroupsInput{
						CacheParameterGroupName: pgName,
					},
				)
				require.NoError(t, err)
				assert.GreaterOrEqual(t, len(out.CacheParameterGroups), tt.wantCount)
			}
		})
	}
}

func TestDeleteCacheParameterGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client)
		name    string
		pgName  string
		wantErr bool
	}{
		{
			name:   "success",
			pgName: "my-pg",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheParameterGroup(t.Context(), &elasticachesdk.CreateCacheParameterGroupInput{
					CacheParameterGroupName:   aws.String("my-pg"),
					CacheParameterGroupFamily: aws.String("redis7"),
					Description:               aws.String("test"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			pgName:  "does-not-exist",
			wantErr: true,
		},
		{
			name:    "default_not_deletable",
			pgName:  "default.redis7",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			_, err := client.DeleteCacheParameterGroup(t.Context(), &elasticachesdk.DeleteCacheParameterGroupInput{
				CacheParameterGroupName: aws.String(tt.pgName),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestModifyAndResetCacheParameterGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pgName  string
		wantErr bool
	}{
		{
			name:   "modify_and_reset_success",
			pgName: "my-pg",
		},
		{
			name:    "modify_default_fails",
			pgName:  "default.redis7",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if !tt.wantErr {
				_, err := client.CreateCacheParameterGroup(t.Context(), &elasticachesdk.CreateCacheParameterGroupInput{
					CacheParameterGroupName:   aws.String(tt.pgName),
					CacheParameterGroupFamily: aws.String("redis7"),
					Description:               aws.String("test"),
				})
				require.NoError(t, err)
			}

			_, modErr := client.ModifyCacheParameterGroup(t.Context(), &elasticachesdk.ModifyCacheParameterGroupInput{
				CacheParameterGroupName: aws.String(tt.pgName),
				ParameterNameValues: []elasticachetypes.ParameterNameValue{
					{ParameterName: aws.String("maxmemory-policy"), ParameterValue: aws.String("allkeys-lru")},
				},
			})

			if tt.wantErr {
				require.Error(t, modErr)

				return
			}

			require.NoError(t, modErr)

			// Verify via DescribeCacheParameters
			paramsOut, err := client.DescribeCacheParameters(t.Context(), &elasticachesdk.DescribeCacheParametersInput{
				CacheParameterGroupName: aws.String(tt.pgName),
			})
			require.NoError(t, err)
			require.Len(t, paramsOut.Parameters, 1)
			assert.Equal(t, "maxmemory-policy", aws.ToString(paramsOut.Parameters[0].ParameterName))
			assert.Equal(t, "allkeys-lru", aws.ToString(paramsOut.Parameters[0].ParameterValue))

			// Reset all parameters
			_, resetErr := client.ResetCacheParameterGroup(t.Context(), &elasticachesdk.ResetCacheParameterGroupInput{
				CacheParameterGroupName: aws.String(tt.pgName),
				ResetAllParameters:      aws.Bool(true),
			})
			require.NoError(t, resetErr)

			// Should be empty again
			paramsOut2, err := client.DescribeCacheParameters(t.Context(), &elasticachesdk.DescribeCacheParametersInput{
				CacheParameterGroupName: aws.String(tt.pgName),
			})
			require.NoError(t, err)
			assert.Empty(t, paramsOut2.Parameters)
		})
	}
}

func TestDefaultParameterGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		paramGroupName string
		wantFound      bool
	}{
		{
			name:           "default_redis7",
			paramGroupName: "default.redis7",
			wantFound:      true,
		},
		{
			name:           "default_redis6x",
			paramGroupName: "default.redis6.x",
			wantFound:      true,
		},
		{
			name:           "default_memcached16",
			paramGroupName: "default.memcached1.6",
			wantFound:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			out, err := client.DescribeCacheParameterGroups(
				t.Context(),
				&elasticachesdk.DescribeCacheParameterGroupsInput{
					CacheParameterGroupName: aws.String(tt.paramGroupName),
				},
			)
			require.NoError(t, err)
			require.Len(t, out.CacheParameterGroups, 1)
			assert.Equal(t, tt.paramGroupName, aws.ToString(out.CacheParameterGroups[0].CacheParameterGroupName))
		})
	}
}

func TestHandler_DescribeEngineDefaultParameters_ReturnsRealParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		family    string
		wantParam string
	}{
		{family: "redis7", wantParam: "maxmemory-policy"},
		{family: "memcached1.6", wantParam: "max_item_size"},
		{family: "valkey8", wantParam: "maxmemory-policy"},
	}

	for _, tt := range tests {
		t.Run(tt.family, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			out, err := client.DescribeEngineDefaultParameters(
				t.Context(),
				&elasticachesdk.DescribeEngineDefaultParametersInput{
					CacheParameterGroupFamily: aws.String(tt.family),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, out.EngineDefaults)
			assert.GreaterOrEqual(t, len(out.EngineDefaults.Parameters), 1)

			names := make(map[string]bool)
			for _, p := range out.EngineDefaults.Parameters {
				names[aws.ToString(p.ParameterName)] = true
			}

			assert.True(
				t,
				names[tt.wantParam],
				"expected parameter %q in defaults for family %q",
				tt.wantParam,
				tt.family,
			)
		})
	}
}

// ----------------------------------------
// Reserved cache node ARN in response
// ----------------------------------------

func TestDescribeCacheParameterGroups_Valkey(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.DescribeCacheParameterGroups(t.Context(), &elasticachesdk.DescribeCacheParameterGroupsInput{})

	require.NoError(t, err)

	families := make(map[string]bool)
	for _, pg := range out.CacheParameterGroups {
		families[aws.ToString(pg.CacheParameterGroupFamily)] = true
	}

	assert.True(t, families["valkey8"], "expected valkey8 family")
	assert.True(t, families["valkey7"], "expected valkey7 family")
}

// ----------------------------------------
// GlobalReplicationGroup — region tracking (gap #12)
// ----------------------------------------

func TestHandler_ResetCacheParameterGroup_All(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateCacheParameterGroup(t.Context(), &elasticachesdk.CreateCacheParameterGroupInput{
		CacheParameterGroupName:   aws.String("reset-pg"),
		CacheParameterGroupFamily: aws.String("redis7"),
		Description:               aws.String("for reset test"),
	})
	require.NoError(t, err)

	_, err = client.ModifyCacheParameterGroup(t.Context(), &elasticachesdk.ModifyCacheParameterGroupInput{
		CacheParameterGroupName: aws.String("reset-pg"),
		ParameterNameValues: []elasticachetypes.ParameterNameValue{
			{ParameterName: aws.String("maxmemory-policy"), ParameterValue: aws.String("allkeys-lru")},
		},
	})
	require.NoError(t, err)

	out, err := client.ResetCacheParameterGroup(t.Context(), &elasticachesdk.ResetCacheParameterGroupInput{
		CacheParameterGroupName: aws.String("reset-pg"),
		ResetAllParameters:      aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Equal(t, "reset-pg", aws.ToString(out.CacheParameterGroupName))
}

// ----------------------------------------
// Snapshot — CopySnapshot cross-region
// ----------------------------------------

func TestHandler_DescribeEngineDefaultParameters_Redis(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.DescribeEngineDefaultParameters(
		t.Context(),
		&elasticachesdk.DescribeEngineDefaultParametersInput{
			CacheParameterGroupFamily: aws.String("redis7"),
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, out)
	assert.Equal(t, "redis7", aws.ToString(out.EngineDefaults.CacheParameterGroupFamily))
}

// ----------------------------------------
// RebootCacheCluster
// ----------------------------------------

func TestHandler_Tags_OnCacheParameterGroup(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.CreateCacheParameterGroup(t.Context(), &elasticachesdk.CreateCacheParameterGroupInput{
		CacheParameterGroupName:   aws.String("tagged-pg"),
		CacheParameterGroupFamily: aws.String("redis7"),
		Description:               aws.String("tagged param group"),
		Tags: []elasticachetypes.Tag{
			{Key: aws.String("purpose"), Value: aws.String("testing")},
		},
	})
	require.NoError(t, err)
	arn := aws.ToString(out.CacheParameterGroup.ARN)
	require.NotEmpty(t, arn)

	tagsOut, err := client.ListTagsForResource(t.Context(), &elasticachesdk.ListTagsForResourceInput{
		ResourceName: aws.String(arn),
	})
	require.NoError(t, err)
	require.Len(t, tagsOut.TagList, 1)
	assert.Equal(t, "purpose", aws.ToString(tagsOut.TagList[0].Key))
}

func TestDescribeEngineDefaultParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		family string
	}{
		{
			name:   "redis7_family",
			family: "redis7",
		},
		{
			name:   "memcached_family",
			family: "memcached1.6",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			out, err := client.DescribeEngineDefaultParameters(
				t.Context(),
				&elasticachesdk.DescribeEngineDefaultParametersInput{
					CacheParameterGroupFamily: aws.String(tt.family),
				},
			)

			require.NoError(t, err)
			assert.NotNil(t, out.EngineDefaults)
		})
	}
}
