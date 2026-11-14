package connect

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const analyticsModePostContact = "PostContact"

// analyticsJobRec is a Contact Lens post-contact job requested for a contact.
type analyticsJobRec struct {
	Started       awstime.EpochTime `json:"started"`
	Configuration jsontext.Value    `json:"configuration"`
	ID            string            `json:"id"`
	RecordingFile string            `json:"recordingFile"`
	Modes         []string          `json:"modes"`
}

// AnalyticsDataSet is types.AnalyticsDataSetsResult.
type AnalyticsDataSet struct {
	DataSetID   string `json:"DataSetId"`
	DataSetName string `json:"DataSetName"`
}

// ListAnalyticsDataLakeDataSetsInput is the ListAnalyticsDataLakeDataSets request.
type ListAnalyticsDataLakeDataSetsInput struct {
	NextToken string `json:"-" query:"nextToken"`
	instPath
	MaxResults int32 `json:"-" query:"maxResults"`
}

// StartConversationalAnalyticsJobInput is the StartContactConversationalAnalyticsJob request.
type StartConversationalAnalyticsJobInput struct {
	AnalyticsConfiguration jsontext.Value `json:"AnalyticsConfiguration"`
	ClientToken            string         `json:"ClientToken"`
	ContactID              string         `json:"-"                      path:"ContactId"`
	instPath
	AnalyticsModes []string `json:"AnalyticsModes"`
}

// ConversationalAnalyticsJobOut is the StartContactConversationalAnalyticsJob response.
type ConversationalAnalyticsJobOut struct {
	ContactID  string `json:"ContactId"`
	InstanceID string `json:"InstanceId"`
}

type behaviorConfig struct {
	Behavior string `json:"Behavior"`
}

type analyticsConfig struct {
	Redaction *struct {
		Behavior string   `json:"Behavior"`
		Policy   string   `json:"Policy"`
		MaskMode string   `json:"MaskMode"`
		Entities []string `json:"Entities"`
	} `json:"RedactionConfiguration"`
	Rules     *behaviorConfig `json:"RulesConfiguration"`
	Sentiment *behaviorConfig `json:"SentimentConfiguration"`
	Summary   *struct {
		SummaryModes []string `json:"SummaryModes"`
	} `json:"SummaryConfiguration"`
}

// dataLakeTables are the table names the Connect admin guide ("Data retention in the analytics data lake") lists.
func dataLakeTables() []string {
	return []string{
		"agent_queue_statistic_record", "agent_statistic_record", "bot_conversations", "bot_intents", "bot_slots",
		"coaching_session_record", "contact_evaluation_record", "contact_flow_events",
		"contact_lens_conversational_analytics", "contact_record", "contact_statistic_record",
		"evaluation_review_record", "intraday_forecasts", "long_term_forecasts", "outbound_campaign_events",
		"schedule_goals", "schedule_metrics", "short_term_forecasts", "staff_shift_activities", "staff_shifts",
		"staff_timeoff_intervals", "staff_timeoffs",
	}
}

// ListAnalyticsDataLakeDataSets pages through the shareable data lake tables; the table name is the dataset ID.
func (b *Backend) ListAnalyticsDataLakeDataSets(in *ListAnalyticsDataLakeDataSetsInput) (*map[string]any, error) {
	if in.MaxResults > maxAnalyticsPage {
		return nil, invalidParamErr("maxResults must be 1-%d", maxAnalyticsPage)
	}

	all, err := readInstance(b, "ListAnalyticsDataLakeDataSets", in.InstanceID,
		func(string) ([]AnalyticsDataSet, error) {
			tables := dataLakeTables()
			out := make([]AnalyticsDataSet, 0, len(tables))

			for _, t := range tables {
				out = append(out, AnalyticsDataSet{DataSetID: t, DataSetName: t})
			}

			return out, nil
		})
	if err != nil {
		return nil, err
	}

	page, next, err := paged(all, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return listBody("Results", page, next), nil
}

func (c *analyticsConfig) validate() error {
	for name, bc := range map[string]*behaviorConfig{
		"RulesConfiguration.Behavior": c.Rules, "SentimentConfiguration.Behavior": c.Sentiment,
	} {
		if bc == nil || bc.Behavior == "" {
			continue
		}

		if err := oneOf(name, bc.Behavior, "Enable", "Disable"); err != nil {
			return invalidRequestErr("%v", err)
		}
	}

	if c.Summary != nil {
		for _, m := range c.Summary.SummaryModes {
			if err := oneOf(
				"SummaryConfiguration.SummaryModes",
				m,
				"PostContact",
				"AutomatedInteraction",
				"ContactChain",
			); err != nil {
				return invalidRequestErr("%v", err)
			}
		}
	}

	return c.validateRedaction()
}

func (c *analyticsConfig) validateRedaction() error {
	r := c.Redaction
	if r == nil {
		return nil
	}

	checks := []struct {
		name, v string
		allowed []string
	}{
		{"RedactionConfiguration.Behavior", r.Behavior, []string{"Enable", "Disable"}},
		{"RedactionConfiguration.Policy", r.Policy, []string{"None", "RedactedOnly", "RedactedAndOriginal"}},
		{"RedactionConfiguration.MaskMode", r.MaskMode, []string{"PII", "EntityType"}},
	}

	for _, ck := range checks {
		if ck.v == "" {
			continue
		}

		if err := oneOf(ck.name, ck.v, ck.allowed...); err != nil {
			return invalidRequestErr("%v", err)
		}
	}

	return nil
}

func (b *Backend) recordingFile(inst string, c *contactRec) (string, bool) {
	for _, f := range b.attachedFiles.in(inst) {
		if f.UseCase == useCaseVoiceRecording && f.Resource == c.Contact.Arn && f.Status == fileStatusApproved {
			return f.FileID, true
		}
	}

	return "", false
}

// StartContactConversationalAnalyticsJob records a post-contact job for a voice contact with a recording.
func (b *Backend) StartContactConversationalAnalyticsJob(
	ctx context.Context,
	in *StartConversationalAnalyticsJobInput,
) (*ConversationalAnalyticsJobOut, error) {
	var cfg analyticsConfig

	if len(in.AnalyticsConfiguration) == 0 || string(in.AnalyticsConfiguration) == jsonNull {
		return nil, invalidRequestErr("AnalyticsConfiguration is required")
	}

	if err := json.Unmarshal(in.AnalyticsConfiguration, &cfg, jsonx.V1()); err != nil {
		return nil, invalidRequestErr("AnalyticsConfiguration is not valid")
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	if len(in.AnalyticsModes) == 0 ||
		slices.ContainsFunc(in.AnalyticsModes, func(m string) bool { return m != analyticsModePostContact }) {
		return nil, invalidRequestErr("AnalyticsModes must contain only %s", analyticsModePostContact)
	}

	return writeInstance(b, "StartContactConversationalAnalyticsJob", in.InstanceID,
		func(inst string) (*ConversationalAnalyticsJobOut, error) {
			c, err := b.contacts.get(inst, in.ContactID)
			if err != nil {
				return nil, err
			}

			if c.Contact.Channel != channelVoice {
				return nil, invalidRequestErr("conversational analytics need a voice contact")
			}

			file, ok := b.recordingFile(inst, c)
			if !ok {
				return nil, invalidRequestErr(
					"contact %q has no voice recording attached; use CreateAttachedFile",
					in.ContactID,
				)
			}

			c.AnalyticsJobs = append(c.AnalyticsJobs, analyticsJobRec{
				ID: newID(), Started: awstime.NewEpochTime(b.now()), Configuration: in.AnalyticsConfiguration,
				RecordingFile: file, Modes: slices.Clone(in.AnalyticsModes),
			})
			logger.Load(ctx).
				InfoContext(ctx, "connect: would run Contact Lens post-contact analytics (no Contact Lens)",
					"contact", in.ContactID, "recordingFile", file)

			return &ConversationalAnalyticsJobOut{ContactID: in.ContactID, InstanceID: inst}, nil
		})
}
