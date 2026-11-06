package codebuild

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Build copies are read by handlers after the backend lock is released; the run
// goroutine must not write through pointers/slices they share.
func TestBuildCopy_NotAliasedToRunProgress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *InMemoryBackend, id string)
		advance func(r *buildRunner, b *InMemoryBackend, id string)
		read    func(bd *Build) string
		name    string
	}{
		{
			name: "logs",
			setup: func(*InMemoryBackend, string) {},
			advance: func(r *buildRunner, _ *InMemoryBackend, _ string) {
				r.initLogs()
			},
			read: func(bd *Build) string { return bd.Logs.CloudWatchLogs.GroupName + bd.Logs.GroupName },
		},
		{
			name: "phases",
			setup: func(b *InMemoryBackend, id string) {
				b.mutateBuild(id, func(bd *Build) {
					bd.Phases = append(bd.Phases, BuildPhase{PhaseType: "BUILD", StartTime: 1})
				})
				b.mu.Lock("test")
				b.runs[id] = &buildRun{cancel: func() {}}
				b.mu.Unlock()
			},
			advance: func(_ *buildRunner, b *InMemoryBackend, id string) {
				_, _ = b.StopBuild(id)
			},
			read: func(bd *Build) string { return bd.Phases[len(bd.Phases)-1].PhaseStatus },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreateProject(ProjectConfig{
				Name:        "proj",
				Source:      &ProjectSource{Type: "NO_SOURCE"},
				Artifacts:   &ProjectArtifacts{Type: "NO_ARTIFACTS"},
				Environment: &ProjectEnvironment{Type: "LINUX_CONTAINER", Image: "img", ComputeType: "BUILD_GENERAL1_SMALL"},
				LogsConfig:  &LogsConfig{CloudWatchLogs: CloudWatchLogsConfig{Status: "ENABLED", GroupName: "g"}},
			})
			require.NoError(t, err)

			started, err := b.StartBuild("proj", StartBuildConfig{})
			require.NoError(t, err)

			tt.setup(b, started.ID)

			got, _ := b.BatchGetBuilds([]string{started.ID})
			require.Len(t, got, 1)
			held := got[0]
			before := tt.read(held)

			ctx := context.Background()
			r := &buildRunner{b: b, build: *started, runCtx: ctx, ctx: ctx, region: "us-east-1"}
			r.log = &buildLogger{}

			stop := make(chan struct{})

			var wg sync.WaitGroup

			wg.Go(func() {
				for {
					select {
					case <-stop:
						return
					default:
						_ = tt.read(held)
					}
				}
			})

			tt.advance(r, b, started.ID)
			close(stop)
			wg.Wait()

			assert.Equal(t, before, tt.read(held))
		})
	}
}
