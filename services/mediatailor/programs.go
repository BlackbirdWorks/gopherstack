package mediatailor

import (
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	transitionTypeAbsolute = "ABSOLUTE"
	transitionTypeRelative = "RELATIVE"

	relativePositionBefore = "BEFORE_PROGRAM"
	relativePositionAfter  = "AFTER_PROGRAM"

	millisPerSecond = 1000
)

// --- Program operations ---

func programKey(channelName, programName string) string {
	return channelName + "/" + programName
}

func cloneAdBreaks(in []AdBreak) []AdBreak {
	if len(in) == 0 {
		return nil
	}

	out := make([]AdBreak, len(in))
	for i, ab := range in {
		out[i] = ab
		out[i].AdBreakMetadata = slices.Clone(ab.AdBreakMetadata)

		if ab.Slate != nil {
			s := *ab.Slate
			out[i].Slate = &s
		}

		if ab.SpliceInsertMessage != nil {
			m := *ab.SpliceInsertMessage
			out[i].SpliceInsertMessage = &m
		}

		if ab.TimeSignalMessage != nil {
			m := *ab.TimeSignalMessage
			m.SegmentationDescriptors = slices.Clone(ab.TimeSignalMessage.SegmentationDescriptors)
			out[i].TimeSignalMessage = &m
		}
	}

	return out
}

func cloneClipRange(in *ClipRange) *ClipRange {
	if in == nil {
		return nil
	}

	cr := *in

	return &cr
}

func cloneAudienceMedia(in []AudienceMedia) []AudienceMedia {
	if len(in) == 0 {
		return nil
	}

	out := make([]AudienceMedia, len(in))
	for i, am := range in {
		out[i] = am
		out[i].AlternateMedia = make([]AlternateMedia, len(am.AlternateMedia))

		for j, alt := range am.AlternateMedia {
			out[i].AlternateMedia[j] = alt
			out[i].AlternateMedia[j].AdBreaks = cloneAdBreaks(alt.AdBreaks)
			out[i].AlternateMedia[j].ClipRange = cloneClipRange(alt.ClipRange)
		}
	}

	return out
}

// resolveProgramSchedule computes a new program's ScheduledStartTime and
// DurationMillis from its (required) Transition, mirroring how MediaTailor
// positions programs on a channel's schedule: ABSOLUTE transitions play at a
// specific wall-clock time; RELATIVE transitions are positioned immediately
// before or after an existing sibling program in the same channel.
func (b *InMemoryBackend) resolveProgramSchedule(
	channelName string, t Transition,
) (time.Time, error) {
	switch t.Type {
	case transitionTypeAbsolute:
		if t.ScheduledStartTimeMillis == 0 {
			return time.Time{}, fmt.Errorf(
				"%w: Transition.ScheduledStartTimeMillis required for ABSOLUTE transitions",
				ErrInvalidParameter,
			)
		}

		return time.UnixMilli(t.ScheduledStartTimeMillis).UTC(), nil
	case transitionTypeRelative:
		return b.resolveRelativeSchedule(channelName, t)
	default:
		return time.Time{}, fmt.Errorf(
			"%w: Transition.Type must be ABSOLUTE or RELATIVE", ErrInvalidParameter,
		)
	}
}

func (b *InMemoryBackend) resolveRelativeSchedule(channelName string, t Transition) (time.Time, error) {
	if t.RelativeProgram == "" {
		return time.Time{}, fmt.Errorf(
			"%w: Transition.RelativeProgram required for RELATIVE transitions", ErrInvalidParameter,
		)
	}

	rel, ok := b.programs.Get(programKey(channelName, t.RelativeProgram))
	if !ok {
		return time.Time{}, fmt.Errorf(
			"%w: relative program %s not found", ErrInvalidParameter, t.RelativeProgram,
		)
	}

	switch t.RelativePosition {
	case relativePositionAfter:
		return rel.ScheduledStartTime.Add(time.Duration(rel.DurationMillis) * time.Millisecond), nil
	case relativePositionBefore:
		return rel.ScheduledStartTime.Add(-time.Duration(t.DurationMillis) * time.Millisecond), nil
	default:
		return time.Time{}, fmt.Errorf(
			"%w: Transition.RelativePosition must be BEFORE_PROGRAM or AFTER_PROGRAM",
			ErrInvalidParameter,
		)
	}
}

// CreateProgram creates a program within a channel.
func (b *InMemoryBackend) CreateProgram(
	channelName, programName, sourceLocationName, vodSourceName, liveSourceName string,
	scheduleConfig *ScheduleConfiguration,
	adBreaks []AdBreak,
	audienceMedia []AudienceMedia,
	tags map[string]string,
) (*Program, error) {
	if scheduleConfig == nil ||
		scheduleConfig.Transition.Type == "" ||
		scheduleConfig.Transition.RelativePosition == "" {
		return nil, fmt.Errorf(
			"%w: ScheduleConfiguration.Transition (Type and RelativePosition) required",
			ErrInvalidParameter,
		)
	}

	if sourceLocationName == "" {
		return nil, fmt.Errorf("%w: SourceLocationName required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateProgram")
	defer b.mu.Unlock()

	if !b.channels.Has(channelName) {
		return nil, fmt.Errorf("%w: channel %s not found", ErrNotFound, channelName)
	}

	if !b.sourceLocations.Has(sourceLocationName) {
		return nil, fmt.Errorf("%w: source location %s not found", ErrNotFound, sourceLocationName)
	}

	if vodSourceName != "" && !b.vodSources.Has(vodSourceKey(sourceLocationName, vodSourceName)) {
		return nil, fmt.Errorf("%w: vod source %s not found", ErrNotFound, vodSourceName)
	}

	if liveSourceName != "" && !b.liveSources.Has(liveSourceKey(sourceLocationName, liveSourceName)) {
		return nil, fmt.Errorf("%w: live source %s not found", ErrNotFound, liveSourceName)
	}

	key := programKey(channelName, programName)
	if b.programs.Has(key) {
		return nil, fmt.Errorf("%w: program %s already exists", ErrConflict, programName)
	}

	scheduledStart, err := b.resolveProgramSchedule(channelName, scheduleConfig.Transition)
	if err != nil {
		return nil, err
	}

	progARN := fmt.Sprintf(
		"arn:aws:mediatailor:%s:%s:channel/%s/program/%s",
		b.region, b.accountID, channelName, programName,
	)
	prog := &Program{
		Tags:               copyTags(tags),
		ARN:                progARN,
		ChannelName:        channelName,
		ProgramName:        programName,
		SourceLocationName: sourceLocationName,
		VodSourceName:      vodSourceName,
		LiveSourceName:     liveSourceName,
		CreationTime:       time.Now().UTC(),
		ScheduledStartTime: scheduledStart,
		DurationMillis:     scheduleConfig.Transition.DurationMillis,
		ClipRange:          cloneClipRange(scheduleConfig.ClipRange),
		AdBreaks:           cloneAdBreaks(adBreaks),
		AudienceMedia:      cloneAudienceMedia(audienceMedia),
	}
	b.programs.Put(prog)
	b.tags[progARN] = copyTags(tags)

	return prog, nil
}

// DescribeProgram returns a program.
func (b *InMemoryBackend) DescribeProgram(channelName, programName string) (*Program, error) {
	b.mu.RLock("DescribeProgram")
	defer b.mu.RUnlock()

	prog, ok := b.programs.Get(programKey(channelName, programName))
	if !ok {
		return nil, fmt.Errorf("%w: program %s not found", ErrNotFound, programName)
	}

	out := *prog
	out.Tags = copyTags(b.tags[prog.ARN])

	return &out, nil
}

// UpdateProgram updates a program's schedule configuration, ad breaks, and
// audience media. Per the real UpdateProgramInput model, ScheduleConfiguration
// is a required top-level member, but its Transition/ClipRange sub-fields are
// individually optional -- an empty ScheduleConfiguration changes nothing.
func (b *InMemoryBackend) UpdateProgram(
	channelName, programName string,
	scheduleConfig *UpdateProgramScheduleConfiguration,
	adBreaks []AdBreak,
	audienceMedia []AudienceMedia,
) (*Program, error) {
	b.mu.Lock("UpdateProgram")
	defer b.mu.Unlock()

	prog, ok := b.programs.Get(programKey(channelName, programName))
	if !ok {
		return nil, fmt.Errorf("%w: program %s not found", ErrNotFound, programName)
	}

	if scheduleConfig == nil {
		return nil, fmt.Errorf("%w: ScheduleConfiguration required", ErrInvalidParameter)
	}

	if scheduleConfig.Transition != nil {
		if scheduleConfig.Transition.ScheduledStartTimeMillis != 0 {
			prog.ScheduledStartTime = time.UnixMilli(scheduleConfig.Transition.ScheduledStartTimeMillis).UTC()
		}

		if scheduleConfig.Transition.DurationMillis != 0 {
			prog.DurationMillis = scheduleConfig.Transition.DurationMillis
		}
	}

	if scheduleConfig.ClipRange != nil {
		prog.ClipRange = cloneClipRange(scheduleConfig.ClipRange)
	}

	if adBreaks != nil {
		prog.AdBreaks = cloneAdBreaks(adBreaks)
	}

	if audienceMedia != nil {
		prog.AudienceMedia = cloneAudienceMedia(audienceMedia)
	}

	out := *prog
	out.Tags = copyTags(b.tags[prog.ARN])

	return &out, nil
}

// DeleteProgram deletes a program.
func (b *InMemoryBackend) DeleteProgram(channelName, programName string) error {
	b.mu.Lock("DeleteProgram")
	defer b.mu.Unlock()

	key := programKey(channelName, programName)

	prog, ok := b.programs.Get(key)
	if !ok {
		return fmt.Errorf("%w: program %s not found", ErrNotFound, programName)
	}

	delete(b.tags, prog.ARN)
	b.programs.Delete(key)

	return nil
}

// GetChannelSchedule returns a paginated schedule for a channel.
func (b *InMemoryBackend) GetChannelSchedule(
	channelName string, maxResults int, nextToken string,
) ([]*ProgramScheduleEntry, string, error) {
	b.mu.RLock("GetChannelSchedule")
	defer b.mu.RUnlock()

	if !b.channels.Has(channelName) {
		return nil, "", fmt.Errorf("%w: channel %s not found", ErrNotFound, channelName)
	}

	all := slices.Clone(b.programsByChannel.Get(channelName))

	sort.Slice(all, func(i, j int) bool {
		return all[i].ScheduledStartTime.Before(all[j].ScheduledStartTime)
	})

	pg := page.New(all, nextToken, maxResults, defaultMaxResults)

	out := make([]*ProgramScheduleEntry, 0, len(pg.Data))
	for _, prog := range pg.Data {
		out = append(out, &ProgramScheduleEntry{
			ARN:                        prog.ARN,
			ChannelName:                prog.ChannelName,
			ProgramName:                prog.ProgramName,
			SourceLocationName:         prog.SourceLocationName,
			VodSourceName:              prog.VodSourceName,
			LiveSourceName:             prog.LiveSourceName,
			ScheduleEntryType:          "PROGRAM",
			ApproximateStartTime:       prog.ScheduledStartTime,
			ApproximateDurationSeconds: prog.DurationMillis / millisPerSecond,
		})
	}

	return out, pg.Next, nil
}
