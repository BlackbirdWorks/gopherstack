package autoscaling_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

func TestInMemoryBackend_CompleteLifecycleAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		input   autoscaling.CompleteLifecycleActionInput
		wantErr bool
	}{
		{
			name: "complete_success",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "lca-g",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			input: autoscaling.CompleteLifecycleActionInput{
				AutoScalingGroupName:  "lca-g",
				LifecycleHookName:     "my-hook",
				LifecycleActionToken:  "token-abc",
				LifecycleActionResult: "CONTINUE",
			},
		},
		{
			name: "group_not_found",
			input: autoscaling.CompleteLifecycleActionInput{
				AutoScalingGroupName:  "no-such",
				LifecycleHookName:     "my-hook",
				LifecycleActionResult: "CONTINUE",
			},
			wantErr: true,
		},
		{
			name: "missing_hook_name",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "lca-nohook",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			input: autoscaling.CompleteLifecycleActionInput{
				AutoScalingGroupName:  "lca-nohook",
				LifecycleActionResult: "CONTINUE",
			},
			wantErr: true,
		},
		{
			name: "missing_result",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "lca-noresult",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			input: autoscaling.CompleteLifecycleActionInput{
				AutoScalingGroupName: "lca-noresult",
				LifecycleHookName:    "my-hook",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.CompleteLifecycleAction(tt.input)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_DeleteLifecycleHook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *autoscaling.InMemoryBackend)
		name     string
		group    string
		hookName string
		wantErr  bool
	}{
		{
			name: "delete_existing_hook",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "hook-g",
					MinSize:              0,
					MaxSize:              5,
				})
				_ = b.AddLifecycleHook(autoscaling.LifecycleHook{
					LifecycleHookName:    "launch-hook",
					AutoScalingGroupName: "hook-g",
				})
			},
			group:    "hook-g",
			hookName: "launch-hook",
		},
		{
			name: "delete_nonexistent_hook",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "no-hook-g",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			group:    "no-hook-g",
			hookName: "ghost",
			wantErr:  true,
		},
		{
			name:     "group_not_found",
			group:    "no-such",
			hookName: "my-hook",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.DeleteLifecycleHook(tt.group, tt.hookName)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_PutAndDescribeLifecycleHooks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(b *autoscaling.InMemoryBackend)
		name      string
		hookNames []string
		hook      autoscaling.LifecycleHook
		wantCount int
		wantErr   bool
	}{
		{
			name: "put_and_describe_hook",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "hook-asg",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			hook: autoscaling.LifecycleHook{
				LifecycleHookName:    "my-hook",
				AutoScalingGroupName: "hook-asg",
				LifecycleTransition:  "autoscaling:EC2_INSTANCE_LAUNCHING",
			},
			wantCount: 1,
		},
		{
			name: "put_hook_group_not_found",
			hook: autoscaling.LifecycleHook{
				LifecycleHookName:    "h",
				AutoScalingGroupName: "no-such",
			},
			wantErr: true,
		},
		{
			name: "put_hook_name_required",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "hook-req-asg",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			hook: autoscaling.LifecycleHook{
				AutoScalingGroupName: "hook-req-asg",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.PutLifecycleHook(tt.hook)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)

			hooks, err := b.DescribeLifecycleHooks(tt.hook.AutoScalingGroupName, nil)
			require.NoError(t, err)
			assert.Len(t, hooks, tt.wantCount)
		})
	}
}

func TestInMemoryBackend_PutLifecycleHookValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		hook    autoscaling.LifecycleHook
		wantErr bool
	}{
		{
			name: "valid_hook",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "lh-asg",
					MinSize:              1,
					MaxSize:              3,
				})
			},
			hook: autoscaling.LifecycleHook{
				LifecycleHookName:    "my-hook",
				AutoScalingGroupName: "lh-asg",
				LifecycleTransition:  "autoscaling:EC2_INSTANCE_LAUNCHING",
				DefaultResult:        "CONTINUE",
				HeartbeatTimeout:     300,
			},
			wantErr: false,
		},
		{
			name: "default_heartbeat_timeout_applied",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "lh-default-asg",
					MinSize:              1,
					MaxSize:              3,
				})
			},
			hook: autoscaling.LifecycleHook{
				LifecycleHookName:    "my-hook",
				AutoScalingGroupName: "lh-default-asg",
				DefaultResult:        "ABANDON",
			},
			wantErr: false,
		},
		{
			name: "heartbeat_too_low",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "lh-low-asg",
					MinSize:              1,
					MaxSize:              3,
				})
			},
			hook: autoscaling.LifecycleHook{
				LifecycleHookName:    "my-hook",
				AutoScalingGroupName: "lh-low-asg",
				HeartbeatTimeout:     10, // below min of 30
			},
			wantErr: true,
		},
		{
			name: "heartbeat_too_high",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "lh-high-asg",
					MinSize:              1,
					MaxSize:              3,
				})
			},
			hook: autoscaling.LifecycleHook{
				LifecycleHookName:    "my-hook",
				AutoScalingGroupName: "lh-high-asg",
				HeartbeatTimeout:     200000, // above max of 172800
			},
			wantErr: true,
		},
		{
			name: "invalid_default_result",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "lh-dr-asg",
					MinSize:              1,
					MaxSize:              3,
				})
			},
			hook: autoscaling.LifecycleHook{
				LifecycleHookName:    "my-hook",
				AutoScalingGroupName: "lh-dr-asg",
				DefaultResult:        "FAIL",
				HeartbeatTimeout:     300,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.PutLifecycleHook(tt.hook)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestInMemoryBackend_RecordLifecycleActionHeartbeat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		input   autoscaling.RecordLifecycleActionHeartbeatInput
		name    string
		wantErr bool
	}{
		{
			name: "heartbeat_hook_exists",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "hb-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_ = b.PutLifecycleHook(autoscaling.LifecycleHook{
					LifecycleHookName:    "my-hook",
					AutoScalingGroupName: "hb-asg",
					DefaultResult:        "CONTINUE",
				})
			},
			input: autoscaling.RecordLifecycleActionHeartbeatInput{
				AutoScalingGroupName: "hb-asg",
				LifecycleHookName:    "my-hook",
			},
		},
		{
			name:    "group_not_found",
			wantErr: true,
			input: autoscaling.RecordLifecycleActionHeartbeatInput{
				AutoScalingGroupName: "no-such",
				LifecycleHookName:    "my-hook",
			},
		},
		{
			name: "hook_not_found",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "hb-nohook-asg",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			wantErr: true,
			input: autoscaling.RecordLifecycleActionHeartbeatInput{
				AutoScalingGroupName: "hb-nohook-asg",
				LifecycleHookName:    "ghost-hook",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.RecordLifecycleActionHeartbeat(tt.input)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}
