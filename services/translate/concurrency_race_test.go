package translate_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/translate"
)

const raceIterations = 200

// TestListFieldsRaceWithInPlaceAdvance proves that fields read off a List
// result can't race a concurrent op advancing the same value in place.
func TestListFieldsRaceWithInPlaceAdvance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, b *translate.InMemoryBackend) string
		reader  func(b *translate.InMemoryBackend)
		mutator func(b *translate.InMemoryBackend, id string)
		name    string
	}{
		{
			name: "jobs",
			setup: func(t *testing.T, b *translate.InMemoryBackend) string {
				t.Helper()

				return startJob(t, b, "race-job").JobID
			},
			reader: func(b *translate.InMemoryBackend) {
				list, _ := b.ListTextTranslationJobs(translate.TextTranslationJobFilter{}, 10, "")
				for _, j := range list {
					_ = j.JobStatus
					_ = j.EndAt
					_ = j.Message
				}
			},
			mutator: func(b *translate.InMemoryBackend, id string) {
				_, _ = b.DescribeTextTranslationJob(id)
				_, _ = b.StopTextTranslationJob(id)
			},
		},
		{
			name: "parallel_data",
			setup: func(t *testing.T, b *translate.InMemoryBackend) string {
				t.Helper()

				_, err := b.CreateParallelData("race-pd", "d", nil, nil, nil)
				require.NoError(t, err)

				return "race-pd"
			},
			reader: func(b *translate.InMemoryBackend) {
				list, _ := b.ListParallelData(10, "")
				for _, pd := range list {
					_ = pd.Status
					_ = pd.LastUpdatedAt
				}
			},
			mutator: func(b *translate.InMemoryBackend, name string) {
				_, _ = b.GetParallelData(name)
				_, _ = b.UpdateParallelData(name, "d2", nil)
			},
		},
		{
			name: "terminology",
			setup: func(t *testing.T, b *translate.InMemoryBackend) string {
				t.Helper()

				data := &translate.TerminologyData{File: []byte("en,es\nhello,hola\n"), Format: "CSV"}
				_, err := b.ImportTerminology("race-term", "d", data, nil, nil)
				require.NoError(t, err)

				return "race-term"
			},
			reader: func(b *translate.InMemoryBackend) {
				list, _ := b.ListTerminologies(10, "")
				for _, term := range list {
					_ = term.Description
					_ = term.LastUpdatedAt
				}
			},
			mutator: func(b *translate.InMemoryBackend, name string) {
				data := &translate.TerminologyData{File: []byte("en,es\nhello,hola\n"), Format: "CSV"}
				_, _ = b.ImportTerminology(name, "d2", data, nil, nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			id := tt.setup(t, b)

			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				defer wg.Done()

				for range raceIterations {
					tt.reader(b)
				}
			}()

			go func() {
				defer wg.Done()

				for range raceIterations {
					tt.mutator(b, id)
				}
			}()

			wg.Wait()
		})
	}
}
