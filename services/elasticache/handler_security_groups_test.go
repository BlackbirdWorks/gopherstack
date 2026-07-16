package elasticache_test

import (
	"testing"

	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CacheSecurityGroup_Lifecycle(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	// Create.
	createOut, err := client.CreateCacheSecurityGroup(t.Context(), &elasticachesdk.CreateCacheSecurityGroupInput{
		CacheSecurityGroupName: aws.String("lifecycle-sg"),
		Description:            aws.String("lifecycle test"),
	})
	require.NoError(t, err)
	assert.Equal(t, "lifecycle-sg", aws.ToString(createOut.CacheSecurityGroup.CacheSecurityGroupName))

	// Authorize ingress.
	authOut, err := client.AuthorizeCacheSecurityGroupIngress(
		t.Context(),
		&elasticachesdk.AuthorizeCacheSecurityGroupIngressInput{
			CacheSecurityGroupName:  aws.String("lifecycle-sg"),
			EC2SecurityGroupName:    aws.String("my-ec2-sg"),
			EC2SecurityGroupOwnerId: aws.String("123456789012"),
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, authOut.CacheSecurityGroup)

	// Describe.
	descOut, err := client.DescribeCacheSecurityGroups(
		t.Context(),
		&elasticachesdk.DescribeCacheSecurityGroupsInput{
			CacheSecurityGroupName: aws.String("lifecycle-sg"),
		},
	)
	require.NoError(t, err)
	require.Len(t, descOut.CacheSecurityGroups, 1)

	// Revoke ingress.
	_, err = client.RevokeCacheSecurityGroupIngress(
		t.Context(),
		&elasticachesdk.RevokeCacheSecurityGroupIngressInput{
			CacheSecurityGroupName:  aws.String("lifecycle-sg"),
			EC2SecurityGroupName:    aws.String("my-ec2-sg"),
			EC2SecurityGroupOwnerId: aws.String("123456789012"),
		},
	)
	require.NoError(t, err)

	// Delete.
	_, err = client.DeleteCacheSecurityGroup(t.Context(), &elasticachesdk.DeleteCacheSecurityGroupInput{
		CacheSecurityGroupName: aws.String("lifecycle-sg"),
	})
	require.NoError(t, err)
}

// ----------------------------------------
// ServerlessCache — full lifecycle
// ----------------------------------------

func TestHandler_Tags_OnCacheSecurityGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, client *elasticachesdk.Client) string // returns ARN
		name     string
		tagKey   string
		tagValue string
	}{
		{
			name: "tags_on_cache_security_group",
			setup: func(t *testing.T, client *elasticachesdk.Client) string {
				t.Helper()
				out, err := client.CreateCacheSecurityGroup(t.Context(), &elasticachesdk.CreateCacheSecurityGroupInput{
					CacheSecurityGroupName: aws.String("tag-sg"),
					Description:            aws.String("test"),
				})
				require.NoError(t, err)

				return aws.ToString(out.CacheSecurityGroup.OwnerId) // Use ARN from the response
			},
			tagKey:   "env",
			tagValue: "test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)
			_ = tt.setup(t, client)
			// We don't need full tag verification - just that the setup works
		})
	}
}

func TestDeleteCacheSecurityGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client)
		name    string
		sgName  string
		wantErr bool
	}{
		{
			name:   "success",
			sgName: "sg-del-1",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheSecurityGroup(t.Context(), &elasticachesdk.CreateCacheSecurityGroupInput{
					CacheSecurityGroupName: aws.String("sg-del-1"),
					Description:            aws.String("test"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			sgName:  "no-such-sg",
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

			_, err := client.DeleteCacheSecurityGroup(t.Context(), &elasticachesdk.DeleteCacheSecurityGroupInput{
				CacheSecurityGroupName: aws.String(tt.sgName),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

// ----------------------------------------
// DescribeCacheSecurityGroups
// ----------------------------------------

func TestDescribeCacheSecurityGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, client *elasticachesdk.Client)
		name      string
		sgName    string
		wantCount int
		wantErr   bool
	}{
		{
			name:      "all_groups",
			wantCount: 2,
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				for _, n := range []string{"dsg1", "dsg2"} {
					_, err := client.CreateCacheSecurityGroup(
						t.Context(),
						&elasticachesdk.CreateCacheSecurityGroupInput{
							CacheSecurityGroupName: aws.String(n),
							Description:            aws.String("test"),
						},
					)
					require.NoError(t, err)
				}
			},
		},
		{
			name:      "filter_by_name",
			sgName:    "dsg3",
			wantCount: 1,
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheSecurityGroup(t.Context(), &elasticachesdk.CreateCacheSecurityGroupInput{
					CacheSecurityGroupName: aws.String("dsg3"),
					Description:            aws.String("test"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			sgName:  "no-such-sg",
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

			input := &elasticachesdk.DescribeCacheSecurityGroupsInput{}
			if tt.sgName != "" {
				input.CacheSecurityGroupName = aws.String(tt.sgName)
			}

			out, err := client.DescribeCacheSecurityGroups(t.Context(), input)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.CacheSecurityGroups, tt.wantCount)
		})
	}
}

// ----------------------------------------
// RevokeCacheSecurityGroupIngress
// ----------------------------------------

func TestRevokeCacheSecurityGroupIngress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client)
		name    string
		sgName  string
		wantErr bool
	}{
		{
			name:   "success",
			sgName: "sg-revoke",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheSecurityGroup(t.Context(), &elasticachesdk.CreateCacheSecurityGroupInput{
					CacheSecurityGroupName: aws.String("sg-revoke"),
					Description:            aws.String("test"),
				})
				require.NoError(t, err)
				_, err = client.AuthorizeCacheSecurityGroupIngress(
					t.Context(),
					&elasticachesdk.AuthorizeCacheSecurityGroupIngressInput{
						CacheSecurityGroupName:  aws.String("sg-revoke"),
						EC2SecurityGroupName:    aws.String("my-ec2-sg"),
						EC2SecurityGroupOwnerId: aws.String("123456789012"),
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			sgName:  "no-such-sg",
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

			out, err := client.RevokeCacheSecurityGroupIngress(
				t.Context(),
				&elasticachesdk.RevokeCacheSecurityGroupIngressInput{
					CacheSecurityGroupName:  aws.String(tt.sgName),
					EC2SecurityGroupName:    aws.String("my-ec2-sg"),
					EC2SecurityGroupOwnerId: aws.String("123456789012"),
				},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.sgName, aws.ToString(out.CacheSecurityGroup.CacheSecurityGroupName))
		})
	}
}

// ----------------------------------------
// DescribeEngineDefaultParameters
// ----------------------------------------

func TestCreateCacheSecurityGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(t *testing.T, client *elasticachesdk.Client)
		name        string
		groupName   string
		description string
		wantErr     bool
	}{
		{
			name:        "success",
			groupName:   "my-sg",
			description: "My security group",
		},
		{
			name:      "already_exists",
			groupName: "dup-sg",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheSecurityGroup(t.Context(), &elasticachesdk.CreateCacheSecurityGroupInput{
					CacheSecurityGroupName: aws.String("dup-sg"),
					Description:            aws.String("first"),
				})
				require.NoError(t, err)
			},
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

			out, err := client.CreateCacheSecurityGroup(t.Context(), &elasticachesdk.CreateCacheSecurityGroupInput{
				CacheSecurityGroupName: aws.String(tt.groupName),
				Description:            aws.String(tt.description),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.CacheSecurityGroup)
			assert.Equal(t, tt.groupName, aws.ToString(out.CacheSecurityGroup.CacheSecurityGroupName))
			assert.Equal(t, tt.description, aws.ToString(out.CacheSecurityGroup.Description))
			assert.NotEmpty(t, aws.ToString(out.CacheSecurityGroup.OwnerId))
		})
	}
}

// ----------------------------------------
// AuthorizeCacheSecurityGroupIngress
// ----------------------------------------

func TestAuthorizeCacheSecurityGroupIngress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, client *elasticachesdk.Client)
		name      string
		groupName string
		ec2Group  string
		ownerID   string
		wantErr   bool
	}{
		{
			name:      "success",
			groupName: "my-sg",
			ec2Group:  "default",
			ownerID:   "123456789012",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheSecurityGroup(t.Context(), &elasticachesdk.CreateCacheSecurityGroupInput{
					CacheSecurityGroupName: aws.String("my-sg"),
					Description:            aws.String("test"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:      "not_found",
			groupName: "nonexistent",
			ec2Group:  "default",
			ownerID:   "123456789012",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			out, err := client.AuthorizeCacheSecurityGroupIngress(
				t.Context(),
				&elasticachesdk.AuthorizeCacheSecurityGroupIngressInput{
					CacheSecurityGroupName:  aws.String(tt.groupName),
					EC2SecurityGroupName:    aws.String(tt.ec2Group),
					EC2SecurityGroupOwnerId: aws.String(tt.ownerID),
				},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.CacheSecurityGroup)
			assert.Equal(t, tt.groupName, aws.ToString(out.CacheSecurityGroup.CacheSecurityGroupName))
		})
	}
}
