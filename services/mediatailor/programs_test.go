package mediatailor_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediatailor"
)

// TestGetChannelSchedule_Paginates verifies GetChannelSchedule actually
// respects maxResults/nextToken instead of always returning every program
// with an empty NextToken.
func TestGetChannelSchedule_Paginates(t *testing.T) {
	t.Parallel()

	b := mediatailor.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateChannel("ch1", "LOOP", "", nil, nil, nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateSourceLocation("sl1", "https://example.com", nil, nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateVodSource("sl1", "vs1", nil, nil)
	require.NoError(t, err)

	startMillis := int64(1_700_000_000_000)
	for i, name := range []string{"prog-a", "prog-b", "prog-c"} {
		_, progErr := b.CreateProgram(
			"ch1", name, "sl1", "vs1", "",
			testScheduleConfig(startMillis+int64(i)*60_000), nil, nil, nil,
		)
		require.NoError(t, progErr)
	}

	page1, next1, err := b.GetChannelSchedule("ch1", 1, "")
	require.NoError(t, err)
	require.Len(t, page1, 1)
	require.NotEmpty(t, next1, "a NextToken must be returned when more pages remain")

	page2, _, err := b.GetChannelSchedule("ch1", 1, next1)
	require.NoError(t, err)
	require.Len(t, page2, 1)
	assert.NotEqual(t, page1[0].ProgramName, page2[0].ProgramName, "pages must not repeat items")
}

// TestCreateProgram_RequiresScheduleConfiguration verifies CreateProgram
// rejects a request whose ScheduleConfiguration.Transition is missing Type
// or RelativePosition -- both are required smithy members on
// CreateProgramInput.
func TestCreateProgram_RequiresScheduleConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		scheduleConfig *mediatailor.ScheduleConfiguration
		name           string
	}{
		{name: "nil ScheduleConfiguration"},
		{
			name: "missing Transition.Type",
			scheduleConfig: &mediatailor.ScheduleConfiguration{
				Transition: mediatailor.Transition{RelativePosition: "AFTER_PROGRAM"},
			},
		},
		{
			name: "missing Transition.RelativePosition",
			scheduleConfig: &mediatailor.ScheduleConfiguration{
				Transition: mediatailor.Transition{Type: "ABSOLUTE"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := mediatailor.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreateChannel("ch1", "LOOP", "", nil, nil, nil, nil, nil)
			require.NoError(t, err)

			_, err = b.CreateProgram("ch1", "prog1", "sl1", "vs1", "", tc.scheduleConfig, nil, nil, nil)
			require.ErrorIs(t, err, mediatailor.ErrInvalidParameter)
		})
	}
}

// TestCreateProgram_RelativeTransition verifies a RELATIVE transition
// positions a new program immediately before/after its RelativeProgram
// sibling on the same channel's schedule, mirroring how MediaTailor computes
// ScheduledStartTime for schedule entries that aren't wall-clock (ABSOLUTE)
// positioned.
func TestCreateProgram_RelativeTransition(t *testing.T) {
	t.Parallel()

	b := mediatailor.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateChannel("ch1", "LOOP", "", nil, nil, nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateSourceLocation("sl1", "https://example.com", nil, nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateVodSource("sl1", "vs1", nil, nil)
	require.NoError(t, err)

	first, err := b.CreateProgram(
		"ch1", "prog1", "sl1", "vs1", "",
		&mediatailor.ScheduleConfiguration{
			Transition: mediatailor.Transition{
				Type:                     "ABSOLUTE",
				RelativePosition:         "AFTER_PROGRAM",
				ScheduledStartTimeMillis: 1_700_000_000_000,
				DurationMillis:           30_000,
			},
		},
		nil, nil, nil,
	)
	require.NoError(t, err)

	after, err := b.CreateProgram(
		"ch1", "prog2", "sl1", "vs1", "",
		&mediatailor.ScheduleConfiguration{
			Transition: mediatailor.Transition{
				Type:             "RELATIVE",
				RelativePosition: "AFTER_PROGRAM",
				RelativeProgram:  "prog1",
				DurationMillis:   10_000,
			},
		},
		nil, nil, nil,
	)
	require.NoError(t, err)
	assert.Equal(t, first.ScheduledStartTime.Add(30_000*time.Millisecond), after.ScheduledStartTime)

	before, err := b.CreateProgram(
		"ch1", "prog0", "sl1", "vs1", "",
		&mediatailor.ScheduleConfiguration{
			Transition: mediatailor.Transition{
				Type:             "RELATIVE",
				RelativePosition: "BEFORE_PROGRAM",
				RelativeProgram:  "prog1",
				DurationMillis:   5_000,
			},
		},
		nil, nil, nil,
	)
	require.NoError(t, err)
	assert.Equal(t, first.ScheduledStartTime.Add(-5_000*time.Millisecond), before.ScheduledStartTime)

	_, err = b.CreateProgram(
		"ch1", "prog3", "sl1", "vs1", "",
		&mediatailor.ScheduleConfiguration{
			Transition: mediatailor.Transition{
				Type:             "RELATIVE",
				RelativePosition: "AFTER_PROGRAM",
				RelativeProgram:  "does-not-exist",
			},
		},
		nil, nil, nil,
	)
	require.ErrorIs(t, err, mediatailor.ErrInvalidParameter, "unknown RelativeProgram must be rejected")
}

// TestUpdateProgram_RequiresScheduleConfiguration verifies UpdateProgram
// rejects a nil ScheduleConfiguration -- it is a required top-level member
// of UpdateProgramInput, even though its own sub-fields are all optional.
func TestUpdateProgram_RequiresScheduleConfiguration(t *testing.T) {
	t.Parallel()

	b := mediatailor.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateChannel("ch1", "LOOP", "", nil, nil, nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateSourceLocation("sl1", "https://example.com", nil, nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateVodSource("sl1", "vs1", nil, nil)
	require.NoError(t, err)

	_, err = b.CreateProgram("ch1", "prog1", "sl1", "vs1", "", testScheduleConfig(1_700_000_000_000), nil, nil, nil)
	require.NoError(t, err)

	_, err = b.UpdateProgram("ch1", "prog1", nil, nil, nil)
	require.ErrorIs(t, err, mediatailor.ErrInvalidParameter)
}

// TestUpdateProgram_PartialUpdate verifies an UpdateProgramScheduleConfiguration
// with a nil Transition changes nothing about the schedule (an empty
// ScheduleConfiguration is a valid, no-op update per the real API's
// individually-optional Transition/ClipRange members).
func TestUpdateProgram_PartialUpdate(t *testing.T) {
	t.Parallel()

	b := mediatailor.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateChannel("ch1", "LOOP", "", nil, nil, nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateSourceLocation("sl1", "https://example.com", nil, nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateVodSource("sl1", "vs1", nil, nil)
	require.NoError(t, err)

	created, err := b.CreateProgram(
		"ch1", "prog1", "sl1", "vs1", "", testScheduleConfig(1_700_000_000_000), nil, nil, nil,
	)
	require.NoError(t, err)

	updated, err := b.UpdateProgram("ch1", "prog1", &mediatailor.UpdateProgramScheduleConfiguration{}, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, created.ScheduledStartTime, updated.ScheduledStartTime)
	assert.Equal(t, created.DurationMillis, updated.DurationMillis)
}
