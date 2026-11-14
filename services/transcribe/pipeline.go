package transcribe

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

type jobKind string

const (
	kindTranscription   jobKind = "transcription"
	kindMedical         jobKind = "medical"
	kindCallAnalytics   jobKind = "call-analytics"
	managedMainFile             = "asrOutput.json"
	managedRedactedFile         = "asrOutput.redacted.json"
	managedTokenLen             = 32
	defaultEndpointBase         = "http://localhost:8000"
)

// Failure reasons from the SDK's documented FailureReason list (types.CallAnalyticsJob.FailureReason).
const (
	failUnsupportedFormat = "Unsupported media format"
	failFormatMismatch    = "The media format provided does not match the detected media format"
	failSampleRate        = "The sample rate provided does not match the detected sample rate"
	failChannels          = "Invalid number of channels: number of channels too large"
	failFileSize          = "Invalid file size: file size too large"
	// failMediaAccess is the console wording for unreadable media; not in the SDK docs.
	failMediaAccess = "The S3 URI that you provided can't be accessed. " +
		"Make sure that you have read permission and try your request again."
)

type jobSpec struct {
	Kind             jobKind
	Name             string
	Language         string
	MediaURI         string
	MediaFormat      string
	OutBucket        string
	OutKey           string
	SubtitleFormats  []string
	Channels         []ChannelDefinition
	SampleRate       int32
	SubtitleStart    int32
	Deferred         bool
	ShowSpeakers     bool
	IdentifyChannels bool
	Redaction        bool
	PHI              bool
}

type outFile struct {
	role string
	name string
	data []byte
}

type outcome struct {
	failure string
	files   []outFile
}

type pipelineDeps struct {
	reader    MediaReader
	engine    Engine
	accountID string
}

func normalizeFormat(f string) string {
	if f == "m4a" {
		return mediaMP4
	}

	return f
}

func failf(format string, args ...any) outcome { return outcome{failure: fmt.Sprintf(format, args...)} }

// inspectMedia reads and validates the media object; a nil reader means media is not inspected.
func inspectMedia(ctx context.Context, deps pipelineDeps, spec jobSpec) ([]byte, string, outcome) {
	if deps.reader == nil {
		return nil, "", outcome{}
	}

	bucket, key, ok := parseMediaURI(spec.MediaURI)
	if !ok {
		return nil, "", failf("%s", failMediaAccess)
	}

	data, err := deps.reader.GetObject(ctx, bucket, key)
	if err != nil {
		return nil, "", failf("%s", failMediaAccess)
	}

	if len(data) > maxMediaBytes {
		return nil, "", failf("%s", failFileSize)
	}

	format := sniffMediaFormat(data)

	switch {
	case format == "":
		return nil, "", failf("%s", failUnsupportedFormat)
	case spec.MediaFormat != "" && normalizeFormat(spec.MediaFormat) != format:
		return nil, "", failf("%s", failFormatMismatch)
	}

	return data, format, outcome{}
}

// checkWAV applies the header-derived checks; ok is false when the outcome is a failure.
func checkWAV(data []byte, spec jobSpec) (wavInfo, outcome, bool) {
	info, err := parseWAV(data)
	if err != nil {
		return info, failf("%s", failUnsupportedFormat), false
	}

	if info.channels > dualChannel {
		return info, failf("%s", failChannels), false
	}

	if spec.SampleRate != 0 && info.sampleRate != 0 && int(spec.SampleRate) != info.sampleRate {
		return info, failf("%s", failSampleRate), false
	}

	return info, outcome{}, true
}

// recognize runs the engine over the WAV channels the job needs.
func recognize(ctx context.Context, deps pipelineDeps, spec jobSpec, info wavInfo, format string) ([][]Word, outcome) {
	eng := deps.engine
	if !eng.SupportsLanguage(spec.Language) {
		return nil, failf("Language %s is not supported by the emulator speech engine.", spec.Language)
	}

	switch {
	case spec.ShowSpeakers:
		return nil, failf("Speaker partitioning is not supported by the emulator speech engine.")
	case spec.Redaction:
		return nil, failf("Content redaction is not supported by the emulator speech engine.")
	case spec.PHI:
		return nil, failf("Content identification is not supported by the emulator speech engine.")
	case format != mediaWAV || !info.isPCM16():
		return nil, failf("The emulator speech engine can only transcribe 16-bit PCM WAV media; %s is not supported.",
			formatLabel(format))
	}

	streams := info.channelPCM()
	if !spec.IdentifyChannels && spec.Kind != kindCallAnalytics {
		streams = [][]byte{downmix(streams)}
	}

	out := make([][]Word, 0, len(streams))

	for _, pcm := range streams {
		words, err := eng.Recognize(ctx, spec.Language, info.sampleRate, pcm)
		if err != nil {
			return nil, failf("The emulator speech engine failed: %v", err)
		}

		out = append(out, words)
	}

	return out, outcome{}
}

func formatLabel(f string) string {
	if f == "" {
		return "unknown media"
	}

	return f + " media"
}

// downmix averages 16-bit channels into one stream.
func downmix(streams [][]byte) []byte {
	if len(streams) == 1 {
		return streams[0]
	}

	out := make([]byte, len(streams[0]))

	for i := 0; i+1 < len(out); i += 2 {
		sum := 0
		for _, s := range streams {
			sum += int(int16(binary.LittleEndian.Uint16(s[i:]))) //nolint:gosec // 16-bit sample reinterpretation
		}

		binary.LittleEndian.PutUint16(out[i:], uint16(int16(sum/len(streams)))) //nolint:gosec // average fits int16
	}

	return out
}

// runPipeline reads and validates the media, recognises speech when an engine is installed and renders the documents.
func runPipeline(ctx context.Context, deps pipelineDeps, spec jobSpec) outcome {
	data, format, bad := inspectMedia(ctx, deps, spec)
	if bad.failure != "" {
		return bad
	}

	var (
		info     wavInfo
		channels [][]Word
		duration int
	)

	if format == mediaWAV {
		var ok bool
		if info, bad, ok = checkWAV(data, spec); !ok {
			return bad
		}

		duration = info.durationMillis()
	}

	if deps.engine != nil && deps.reader != nil {
		if channels, bad = recognize(ctx, deps, spec, info, format); bad.failure != "" {
			return bad
		}
	}

	return outcome{files: renderFiles(deps.accountID, spec, channels, duration)}
}

func renderFiles(accountID string, spec jobSpec, channels [][]Word, duration int) []outFile {
	var main []byte

	words := mergeChannels(channels)

	if spec.Kind == kindCallAnalytics {
		main = buildCallAnalyticsDocument(spec.Name, accountID, spec.Language, spec.Channels, channels, duration)
	} else {
		main = buildTranscriptDocument(spec.Name, accountID, docOptions{
			channels: channels, showSpeakers: spec.ShowSpeakers, identifyChans: spec.IdentifyChannels,
		})
	}

	files := []outFile{{role: roleTranscript, data: main}}
	if spec.Redaction {
		files = append(files, outFile{role: roleRedacted, data: main})
	}

	for _, f := range spec.SubtitleFormats {
		var data []byte
		if f == "srt" {
			data = renderSRT(words, int(spec.SubtitleStart))
		} else {
			data = renderVTT(words)
		}

		files = append(files, outFile{role: roleSubtitle, name: f, data: data})
	}

	return files
}

// objectKeys resolves the customer-bucket key of each output file.
func (s jobSpec) objectKey(f outFile) string {
	main := outputObjectKey(s.OutKey, s.Name)
	base := strings.TrimSuffix(main, ".json")

	switch f.role {
	case roleRedacted:
		return base + ".redacted.json"
	case "subtitle":
		return base + "." + f.name
	default:
		return main
	}
}

func (s jobSpec) managedName(f outFile) string {
	switch f.role {
	case roleRedacted:
		return managedRedactedFile
	case "subtitle":
		return s.Name + "." + f.name
	default:
		return managedMainFile
	}
}

// delivered describes where finished output landed.
type delivered struct {
	extra      map[string][]byte
	token      string
	transcript string
	redacted   string
	main       []byte
	subtitles  []string
}

func (b *InMemoryBackend) endpointBase() string {
	if ep, ok := b.appConfig.(interface{ GetEndpoint() string }); ok {
		if v := strings.TrimRight(ep.GetEndpoint(), "/"); v != "" {
			return v
		}
	}

	return defaultEndpointBase
}

func (b *InMemoryBackend) managedURL(kind jobKind, name, token, file string) string {
	return b.endpointBase() + managedOutputPath + b.region + "/" + string(kind) + "/" + name + "/" + token + "/" + file
}

// deliverLocked stores the files in the customer bucket or the service-managed store.
func (b *InMemoryBackend) deliverLocked(spec jobSpec, files []outFile) (delivered, string) {
	var d delivered

	if spec.OutBucket == "" {
		d.token = awsid.NewN("", managedTokenLen)
		d.extra = map[string][]byte{}
	}

	for _, f := range files {
		var uri string

		if spec.OutBucket != "" {
			key := spec.objectKey(f)
			if !b.putOutputLocked(spec.OutBucket, key, f.data) {
				return d, outputWriteFailure
			}

			uri = "s3://" + spec.OutBucket + "/" + key
		} else {
			name := spec.managedName(f)
			uri = b.managedURL(spec.Kind, spec.Name, d.token, name)

			if f.role == roleTranscript {
				d.main = f.data
			} else {
				d.extra[name] = f.data
			}
		}

		switch f.role {
		case roleTranscript:
			d.transcript = uri
		case roleRedacted:
			d.redacted = uri
		default:
			d.subtitles = append(d.subtitles, uri)
		}
	}

	return d, ""
}

// jobView exposes the lifecycle fields of any job kind to the shared state machine.
type jobView struct {
	created   time.Time
	status    *string
	failure   *string
	started   *time.Time
	completed *time.Time
	complete  func(delivered)
	spec      jobSpec
}

func terminal(status string) bool { return status == jobStatusCompleted || status == jobStatusFailed }

func runKey(kind jobKind, name string) string { return string(kind) + "/" + name }

func (b *InMemoryBackend) deps() pipelineDeps {
	return pipelineDeps{reader: b.resolveMediaReader(), engine: b.engine, accountID: defaultAccountID}
}

// settleLocked advances a job as far as the clock and engine allow.
func (b *InMemoryBackend) settleLocked(v jobView) {
	if terminal(*v.status) {
		return
	}

	now := b.now()

	if *v.status == jobStatusQueued {
		*v.status = jobStatusInProgress
		*v.started = now

		if v.spec.Deferred {
			return
		}
	}

	if *v.failure != "" {
		*v.status = jobStatusFailed
		*v.completed = now

		return
	}

	if b.engine != nil && b.resolveMediaReader() != nil {
		b.launchRunLocked(v)

		return
	}

	if b.settling(v.created) {
		return
	}

	b.finishLocked(v, runPipeline(context.Background(), b.deps(), v.spec))
}

func (b *InMemoryBackend) launchRunLocked(v jobView) {
	key := runKey(v.spec.Kind, v.spec.Name)
	if _, running := b.inflight[key]; running {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	b.inflight[key] = cancel
	deps, spec, created := b.deps(), v.spec, v.created

	b.runs.Go(func() {
		out := runPipeline(ctx, deps, spec)

		b.mu.Lock("finishRun")
		defer b.mu.Unlock()

		delete(b.inflight, key)

		if ctx.Err() != nil {
			return
		}

		if cur, ok := b.viewOf(spec.Kind, spec.Name); ok && cur.created.Equal(created) && !terminal(*cur.status) {
			b.finishLocked(cur, out)
		}
	})
}

func (b *InMemoryBackend) finishLocked(v jobView, out outcome) {
	*v.completed = b.now()

	var d delivered

	if out.failure == "" {
		d, out.failure = b.deliverLocked(v.spec, out.files)
	}

	if out.failure != "" {
		*v.status = jobStatusFailed
		*v.failure = out.failure

		return
	}

	*v.status = jobStatusCompleted
	v.complete(d)
}

func (b *InMemoryBackend) viewOf(kind jobKind, name string) (jobView, bool) {
	switch kind {
	case kindTranscription:
		if j, ok := b.jobs.Get(name); ok {
			return viewTranscription(j), true
		}
	case kindMedical:
		if j, ok := b.medicalTranscriptionJobs.Get(name); ok {
			return viewMedical(j), true
		}
	case kindCallAnalytics:
		if j, ok := b.callAnalyticsJobs.Get(name); ok {
			return viewCallAnalytics(j), true
		}
	}

	return jobView{}, false
}

// Shutdown cancels in-flight recognition runs and waits for them.
func (b *InMemoryBackend) Shutdown(ctx context.Context) {
	b.mu.Lock("Shutdown")

	for _, cancel := range b.inflight {
		cancel()
	}

	eng := b.engine
	b.mu.Unlock()

	b.runs.Wait()

	if c, ok := eng.(interface{ Close(context.Context) }); ok {
		c.Close(ctx)
	}
}

// SetClock replaces the time source used for job lifecycles.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = now
}

// ManagedOutput returns a service-managed output file for the fetch URL.
func (b *InMemoryBackend) ManagedOutput(kind, name, token, file string) ([]byte, bool) {
	b.mu.RLock("ManagedOutput")
	defer b.mu.RUnlock()

	var (
		tok  string
		main []byte
		more map[string][]byte
	)

	switch jobKind(kind) {
	case kindTranscription:
		if j, ok := b.jobs.Get(name); ok {
			tok, main, more = j.ManagedToken, j.TranscriptJSON, j.ManagedOutputs
		}
	case kindMedical:
		if j, ok := b.medicalTranscriptionJobs.Get(name); ok {
			tok, main, more = j.ManagedToken, j.TranscriptJSON, j.ManagedOutputs
		}
	case kindCallAnalytics:
		if j, ok := b.callAnalyticsJobs.Get(name); ok {
			tok, main, more = j.ManagedToken, j.TranscriptJSON, j.ManagedOutputs
		}
	}

	if tok == "" || tok != token {
		return nil, false
	}

	if file == managedMainFile {
		return main, main != nil
	}

	data, ok := more[file]

	return data, ok
}

// settleListLocked settles a job for a List call, which never advances a deferred job out of QUEUED.
func (b *InMemoryBackend) settleListLocked(v jobView) {
	if *v.status != jobStatusQueued {
		b.settleLocked(v)
	}
}
