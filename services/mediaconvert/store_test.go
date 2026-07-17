package mediaconvert_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediaconvert"
)

// TestDeepCloneMap_PreservesDeeplyNestedSettings verifies that deepCloneMap clones a
// settings document without silently truncating deeply-nested values to nil. Previously
// any value nested beyond depth 20 was dropped, corrupting legitimate job settings.
func TestDeepCloneMap_PreservesDeeplyNestedSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		depth int
	}{
		{name: "shallow", depth: 5},
		{name: "at_old_limit", depth: 20},
		{name: "beyond_old_limit", depth: 30},
		{name: "deep", depth: 50},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Build a chain of nested maps of the requested depth, with a leaf marker.
			leafKey := "leaf"
			leafVal := "preserved"

			current := map[string]any{leafKey: leafVal}
			for range tt.depth {
				current = map[string]any{"child": current}
			}

			cloned := mediaconvert.DeepCloneMapForTest(current)

			// Walk the clone down to the leaf and assert the marker survived.
			node := cloned
			for range tt.depth {
				next, ok := node["child"].(map[string]any)
				require.Truef(t, ok, "expected nested map at each level, missing one (depth %d)", tt.depth)
				node = next
			}

			assert.Equal(t, leafVal, node[leafKey])
		})
	}
}

func buildNestedSettingsMap(depth int, leafValue string) map[string]any {
	if depth == 0 {
		return map[string]any{"leaf": leafValue}
	}

	return map[string]any{"nested": buildNestedSettingsMap(depth-1, leafValue)}
}

// extractLeafSetting drills into nested maps following "nested" keys until it finds "leaf".
func extractLeafSetting(m map[string]any) (string, bool) {
	cur := m
	for {
		if leaf, ok := cur["leaf"]; ok {
			s, isStr := leaf.(string)

			return s, isStr
		}

		next, ok := cur["nested"]
		if !ok {
			return "", false
		}

		nextMap, ok := next.(map[string]any)
		if !ok {
			return "", false
		}

		cur = nextMap
	}
}

// TestDeepCloneValueAt_PreservesDeepNesting verifies -- through the full
// CreateJob → ListJobs HTTP round trip -- that deeply nested job settings
// documents survive deepCloneValueAt without truncation.
func TestDeepCloneValueAt_PreservesDeepNesting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		leafValue string
		depth     int
	}{
		{
			name:      "depth_20_preserved",
			depth:     20,
			leafValue: "value-at-20",
		},
		{
			name:      "depth_25_preserved",
			depth:     25,
			leafValue: "value-at-25",
		},
		{
			name:      "depth_50_preserved",
			depth:     50,
			leafValue: "value-at-50",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			nestedSettings := buildNestedSettingsMap(tt.depth, tt.leafValue)

			resp, code := parseJSONResponse(t, h, "POST", "/2017-08-29/jobs", map[string]any{
				"role":     "arn:aws:iam::" + testAccountID + ":role/MediaConvert_Role",
				"settings": nestedSettings,
			})
			require.Equal(t, 201, code)

			jobData, ok := resp["job"].(map[string]any)
			require.True(t, ok)
			jobID, _ := jobData["id"].(string)
			require.NotEmpty(t, jobID)

			listResp, listCode := parseJSONResponse(t, h, "GET", "/2017-08-29/jobs", nil)
			require.Equal(t, 200, listCode)

			jobs, ok := listResp["jobs"].([]any)
			require.True(t, ok)
			require.NotEmpty(t, jobs)

			var found map[string]any
			for _, j := range jobs {
				jm, isMap := j.(map[string]any)
				if !isMap {
					continue
				}

				if jm["id"] == jobID {
					found = jm

					break
				}
			}
			require.NotNil(t, found, "job %s not found in list", jobID)

			settings, ok := found["settings"].(map[string]any)
			require.True(t, ok, "settings field missing or wrong type")

			got, ok := extractLeafSetting(settings)
			assert.True(t, ok, "leaf value not found in nested settings")
			assert.Equal(t, tt.leafValue, got)
		})
	}
}

// TestBackendReset ensures Reset clears all state.
func TestBackendReset(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateQueue("q1", "", "", "", nil)
	require.NoError(t, err)
	require.Equal(t, 1, mediaconvert.QueueCount(b))

	b.Reset()

	require.Equal(t, 0, mediaconvert.QueueCount(b))
	require.Equal(t, 0, mediaconvert.JobCount(b))
	require.Equal(t, 0, mediaconvert.JobTemplateCount(b))
	require.Equal(t, 0, mediaconvert.PresetCount(b))
	require.Equal(t, 0, mediaconvert.CertificateCount(b))
}

// TestHandlerReset ensures Handler.Reset delegates to backend.
func TestHandlerReset(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	h := mediaconvert.NewHandler(b)
	_, err := b.CreatePreset("p1", "", "", nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, mediaconvert.PresetCount(b))

	h.Reset()

	require.Equal(t, 0, mediaconvert.PresetCount(b))
}

// TestAccountID ensures AccountID/Region return the configured values.
func TestAccountID(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend("111122223333", "eu-west-1")
	assert.Equal(t, "111122223333", b.AccountID())
	assert.Equal(t, "eu-west-1", b.Region())
}

// TestSeedHelpers verifies all Add*Internal seed helpers work correctly.
func TestSeedHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		seedFunc func(b *mediaconvert.InMemoryBackend)
		check    func(b *mediaconvert.InMemoryBackend)
		name     string
	}{
		{
			name: "add_queue_internal",
			seedFunc: func(b *mediaconvert.InMemoryBackend) {
				b.AddQueueInternal(&mediaconvert.Queue{Name: "seed-q", Status: "ACTIVE", Arn: "arn:test:q"})
			},
			check: func(b *mediaconvert.InMemoryBackend) {
				assert.Equal(t, 1, mediaconvert.QueueCount(b))
			},
		},
		{
			name: "add_job_template_internal",
			seedFunc: func(b *mediaconvert.InMemoryBackend) {
				b.AddJobTemplateInternal(&mediaconvert.JobTemplate{Name: "seed-jt", Arn: "arn:test:jt"})
			},
			check: func(b *mediaconvert.InMemoryBackend) {
				assert.Equal(t, 1, mediaconvert.JobTemplateCount(b))
			},
		},
		{
			name: "add_job_internal",
			seedFunc: func(b *mediaconvert.InMemoryBackend) {
				b.AddJobInternal(&mediaconvert.Job{ID: "seed-job-id", Status: "SUBMITTED", Arn: "arn:test:job"})
			},
			check: func(b *mediaconvert.InMemoryBackend) {
				assert.Equal(t, 1, mediaconvert.JobCount(b))
			},
		},
		{
			name: "add_preset_internal",
			seedFunc: func(b *mediaconvert.InMemoryBackend) {
				b.AddPresetInternal(&mediaconvert.Preset{Name: "seed-preset", Arn: "arn:test:preset"})
			},
			check: func(b *mediaconvert.InMemoryBackend) {
				assert.Equal(t, 1, mediaconvert.PresetCount(b))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
			tt.seedFunc(b)
			tt.check(b)
		})
	}
}

// TestARNsFormat_AllResourceTypes verifies ARNs are correctly formatted for
// every resource family.
func TestARNsFormat_AllResourceTypes(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)

	q, err := b.CreateQueue("arn-test-queue", "", "", "", nil)
	require.NoError(t, err)
	assert.Contains(t, q.Arn, "mediaconvert")
	assert.Contains(t, q.Arn, testRegion)
	assert.Contains(t, q.Arn, testAccountID)

	jt, err := b.CreateJobTemplate("arn-test-jt", "", "", "", 0, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, jt.Arn, "jobTemplates/arn-test-jt")

	p, err := b.CreatePreset("arn-test-preset", "", "", nil, nil)
	require.NoError(t, err)
	assert.Contains(t, p.Arn, "presets/arn-test-preset")

	j, err := b.CreateJob("arn:aws:iam::123:role/r", "", "", nil, nil, nil, "")
	require.NoError(t, err)
	assert.Contains(t, j.Arn, "jobs/")
}
