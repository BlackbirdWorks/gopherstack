package transcribe

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

const (
	languageEnUS      = "en-US"
	roleTranscript    = "transcript"
	roleRedacted      = "redacted"
	roleSubtitle      = "subtitle"
	itemPronunciation = "pronunciation"
	roleAgent         = "AGENT"
	roleCustomer      = "CUSTOMER"

	segmentGapSeconds = 1.0
	cueMaxWords       = 10
	cueMaxSeconds     = 5.0
	millisPerSecond   = 1000
	secondsDecimals   = 2
	confDecimals      = 4
	dualChannel       = 2
	roundHalf         = 0.5
	millisPerHour     = 3600000
	millisPerMinute   = 60000
	secondsPerMinute  = 60
)

type transcriptDocument struct {
	JobName   string            `json:"jobName"`
	AccountID string            `json:"accountId"`
	Status    string            `json:"status"`
	Results   transcriptResults `json:"results"`
}

type transcriptResults struct {
	SpeakerLabels *speakerLabels    `json:"speaker_labels,omitempty"`
	ChannelLabels *channelLabels    `json:"channel_labels,omitempty"`
	Transcripts   []transcriptEntry `json:"transcripts"`
	Items         []transcriptItem  `json:"items"`
	AudioSegments []audioSegment    `json:"audio_segments"`
}

type speakerLabels struct {
	Segments []json.RawMessage `json:"segments"`
	Speakers int               `json:"speakers"`
}

type channelLabels struct {
	Channels         []channelEntry `json:"channels"`
	NumberOfChannels int            `json:"number_of_channels"`
}

type channelEntry struct {
	ChannelLabel string           `json:"channel_label"`
	Items        []transcriptItem `json:"items"`
}

type transcriptEntry struct {
	Transcript string `json:"transcript"`
}

type transcriptItem struct {
	StartTime    string                  `json:"start_time,omitempty"`
	EndTime      string                  `json:"end_time,omitempty"`
	Type         string                  `json:"type"`
	Alternatives []transcriptAlternative `json:"alternatives"`
	ID           int                     `json:"id"`
}

type transcriptAlternative struct {
	Content    string `json:"content"`
	Confidence string `json:"confidence,omitempty"`
}

type audioSegment struct {
	Transcript string `json:"transcript"`
	StartTime  string `json:"start_time"`
	EndTime    string `json:"end_time"`
	Items      []int  `json:"items"`
	ID         int    `json:"id"`
}

func fmtSeconds(v float64) string { return strconv.FormatFloat(v, 'f', secondsDecimals, 64) }

func fmtConfidence(v float64) string {
	s := strconv.FormatFloat(v, 'f', confDecimals, 64)
	s = strings.TrimRight(s, "0")

	if strings.HasSuffix(s, ".") {
		s += "0"
	}

	return s
}

func wordsText(words []Word) string {
	parts := make([]string, 0, len(words))
	for _, w := range words {
		parts = append(parts, w.Text)
	}

	return strings.Join(parts, " ")
}

func wordItems(words []Word, firstID int) []transcriptItem {
	items := make([]transcriptItem, 0, len(words))

	for i, w := range words {
		items = append(items, transcriptItem{
			ID:           firstID + i,
			StartTime:    fmtSeconds(w.Start),
			EndTime:      fmtSeconds(w.End),
			Type:         itemPronunciation,
			Alternatives: []transcriptAlternative{{Content: w.Text, Confidence: fmtConfidence(w.Confidence)}},
		})
	}

	return items
}

// segmentWords splits words into runs separated by pauses longer than segmentGapSeconds.
func segmentWords(words []Word) [][]Word {
	var out [][]Word

	start := 0

	for i := 1; i <= len(words); i++ {
		if i == len(words) || words[i].Start-words[i-1].End > segmentGapSeconds {
			out = append(out, words[start:i])
			start = i
		}
	}

	return out
}

func audioSegments(words []Word) []audioSegment {
	runs := segmentWords(words)
	segs := make([]audioSegment, 0, len(runs))
	next := 0

	for id, run := range runs {
		ids := make([]int, len(run))
		for i := range run {
			ids[i] = next + i
		}

		next += len(run)
		segs = append(segs, audioSegment{
			ID: id, Transcript: wordsText(run), Items: ids,
			StartTime: fmtSeconds(run[0].Start), EndTime: fmtSeconds(run[len(run)-1].End),
		})
	}

	return segs
}

type docOptions struct {
	channels      [][]Word
	showSpeakers  bool
	identifyChans bool
}

// mergeChannels interleaves channel words by start time.
func mergeChannels(channels [][]Word) []Word {
	var all []Word
	for _, c := range channels {
		all = append(all, c...)
	}

	sort.SliceStable(all, func(i, j int) bool { return all[i].Start < all[j].Start })

	return all
}

// buildTranscriptDocument renders the batch transcript JSON (jobName, accountId, results, status).
func buildTranscriptDocument(jobName, accountID string, opt docOptions) []byte {
	words := mergeChannels(opt.channels)
	res := transcriptResults{
		Transcripts:   []transcriptEntry{{Transcript: wordsText(words)}},
		Items:         wordItems(words, 0),
		AudioSegments: audioSegments(words),
	}

	if opt.showSpeakers {
		res.SpeakerLabels = &speakerLabels{Segments: []json.RawMessage{}}
	}

	if opt.identifyChans {
		cl := &channelLabels{NumberOfChannels: dualChannel}

		for c := range dualChannel {
			var cw []Word
			if c < len(opt.channels) {
				cw = opt.channels[c]
			}

			cl.Channels = append(cl.Channels, channelEntry{
				ChannelLabel: "ch_" + strconv.Itoa(c), Items: wordItems(cw, 0),
			})
		}

		res.ChannelLabels = cl
	}

	data, _ := json.Marshal(transcriptDocument{
		JobName: jobName, AccountID: accountID, Status: jobStatusCompleted, Results: res,
	})

	return data
}

type callAnalyticsDocument struct {
	JobName            string                       `json:"JobName"`
	AccountID          string                       `json:"AccountId"`
	JobStatus          string                       `json:"JobStatus"`
	LanguageCode       string                       `json:"LanguageCode"`
	ChannelDefinitions []ChannelDefinition          `json:"ChannelDefinitions,omitempty"`
	Participants       []callAnalyticsPerson        `json:"Participants"`
	Transcript         []callAnalyticsSegment       `json:"Transcript"`
	Characteristics    callAnalyticsCharacteristics `json:"ConversationCharacteristics"`
}

type callAnalyticsPerson struct {
	ParticipantRole string `json:"ParticipantRole"`
}

type callAnalyticsSegment struct {
	ParticipantRole string              `json:"ParticipantRole"`
	Content         string              `json:"Content"`
	ID              string              `json:"Id"`
	Items           []callAnalyticsItem `json:"Items"`
	Begin           int                 `json:"BeginOffsetMillis"`
	End             int                 `json:"EndOffsetMillis"`
}

type callAnalyticsItem struct {
	Content    string  `json:"Content"`
	Type       string  `json:"Type"`
	Confidence float64 `json:"Confidence"`
	Begin      int     `json:"BeginOffsetMillis"`
	End        int     `json:"EndOffsetMillis"`
}

type callAnalyticsCharacteristics struct {
	TotalConversationDurationMillis int `json:"TotalConversationDurationMillis"`
}

func millis(sec float64) int { return int(sec*millisPerSecond + roundHalf) }

// buildCallAnalyticsDocument renders the post-call transcript; roles[i] names channel i.
func buildCallAnalyticsDocument(
	jobName, accountID, language string, defs []ChannelDefinition, channels [][]Word, durationMillis int,
) []byte {
	roles := channelRoles(defs)
	doc := callAnalyticsDocument{
		JobName:            jobName,
		AccountID:          accountID,
		JobStatus:          jobStatusCompleted,
		LanguageCode:       language,
		ChannelDefinitions: defs,
		Participants:       []callAnalyticsPerson{{ParticipantRole: roleAgent}, {ParticipantRole: roleCustomer}},
		Transcript:         []callAnalyticsSegment{},
	}

	type tagged struct {
		role string
		run  []Word
	}

	var runs []tagged

	for c, words := range channels {
		for _, run := range segmentWords(words) {
			if len(run) > 0 {
				runs = append(runs, tagged{role: roles[min(c, len(roles)-1)], run: run})
			}
		}
	}

	sort.SliceStable(runs, func(i, j int) bool { return runs[i].run[0].Start < runs[j].run[0].Start })

	last := durationMillis

	for i, r := range runs {
		seg := callAnalyticsSegment{
			ParticipantRole: r.role, Content: wordsText(r.run), ID: strconv.Itoa(i),
			Begin: millis(r.run[0].Start), End: millis(r.run[len(r.run)-1].End), Items: []callAnalyticsItem{},
		}

		for _, w := range r.run {
			seg.Items = append(seg.Items, callAnalyticsItem{
				Content: w.Text, Type: itemPronunciation, Confidence: w.Confidence,
				Begin: millis(w.Start), End: millis(w.End),
			})
		}

		last = max(last, seg.End)
		doc.Transcript = append(doc.Transcript, seg)
	}

	doc.Characteristics.TotalConversationDurationMillis = last
	data, _ := json.Marshal(doc)

	return data
}

// channelRoles maps channel index to participant role, defaulting to channel 0 agent and channel 1 customer.
func channelRoles(defs []ChannelDefinition) []string {
	roles := []string{roleAgent, roleCustomer}

	for _, d := range defs {
		if d.ChannelID >= 0 && int(d.ChannelID) < len(roles) {
			roles[d.ChannelID] = d.ParticipantRole
		}
	}

	return roles
}

type cue struct {
	text       string
	start, end float64
}

func buildCues(words []Word) []cue {
	var cues []cue

	for _, run := range segmentWords(words) {
		var cur []Word

		flush := func() {
			if len(cur) > 0 {
				cues = append(cues, cue{text: wordsText(cur), start: cur[0].Start, end: cur[len(cur)-1].End})
				cur = nil
			}
		}

		for _, w := range run {
			if len(cur) >= cueMaxWords || (len(cur) > 0 && w.End-cur[0].Start > cueMaxSeconds) {
				flush()
			}

			cur = append(cur, w)
		}

		flush()
	}

	return cues
}

func clockStamp(sec float64, sep string) string {
	ms := millis(sec)

	return fmt.Sprintf(
		"%02d:%02d:%02d%s%03d",
		ms/millisPerHour,
		ms/millisPerMinute%secondsPerMinute,
		ms/millisPerSecond%secondsPerMinute,
		sep,
		ms%millisPerSecond,
	)
}

func renderVTT(words []Word) []byte {
	var sb strings.Builder

	sb.WriteString("WEBVTT\n\n")

	for _, c := range buildCues(words) {
		fmt.Fprintf(&sb, "%s --> %s\n%s\n\n", clockStamp(c.start, "."), clockStamp(c.end, "."), c.text)
	}

	return []byte(sb.String())
}

func renderSRT(words []Word, startIndex int) []byte {
	var sb strings.Builder

	for i, c := range buildCues(words) {
		fmt.Fprintf(
			&sb,
			"%d\n%s --> %s\n%s\n\n",
			startIndex+i,
			clockStamp(c.start, ","),
			clockStamp(c.end, ","),
			c.text,
		)
	}

	return []byte(sb.String())
}
