package athena

import "encoding/json"

const (
	keySessionID  = "SessionId"
	keyState      = "State"
	keyStatus     = "Status"
	keyStatistics = "Statistics"
)

type startSessionInput struct {
	WorkGroup                   string                  `json:"WorkGroup"`
	ClientRequestToken          string                  `json:"ClientRequestToken"`
	Description                 string                  `json:"Description"`
	NotebookVersion             string                  `json:"NotebookVersion"`
	ExecutionRole               string                  `json:"ExecutionRole"`
	MonitoringConfiguration     MonitoringConfiguration `json:"MonitoringConfiguration"`
	Tags                        []Tag                   `json:"Tags"`
	EngineConfiguration         EngineConfiguration     `json:"EngineConfiguration"`
	SessionIdleTimeoutInMinutes int32                   `json:"SessionIdleTimeoutInMinutes"`
	CopyWorkGroupTags           bool                    `json:"CopyWorkGroupTags"`
}

// notebookID extracts the session's linked notebook ID. StartSessionInput has
// no top-level NotebookId member on the real wire (confirmed against
// athena@v1.60.4 serializers.go's awsAwsjson11_serializeOpDocumentStartSessionInput,
// which emits only NotebookVersion) -- the real client instead threads it
// through EngineConfiguration.AdditionalConfigs["NotebookId"], per
// EngineConfiguration.AdditionalConfigs's own doc comment ("add a key named
// NotebookId to AdditionalConfigs"). Reading a nonexistent top-level field, as
// this handler previously did, meant NotebookID was always empty from any
// real client that specified NotebookVersion -- breaking ListNotebookSessions
// and the session's own NotebookId association.
func (in startSessionInput) notebookID() string {
	return in.EngineConfiguration.AdditionalConfigs["NotebookId"]
}

type sessionIDInput struct {
	SessionID string `json:"SessionId"`
}

// listPageInput carries only the paging members.
type listPageInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listSessionsInput struct {
	NextToken   string `json:"NextToken"`
	WorkGroup   string `json:"WorkGroup"`
	StateFilter string `json:"StateFilter"`
	MaxResults  int    `json:"MaxResults"`
}

type listNotebookSessionsInput struct {
	NextToken  string `json:"NextToken"`
	NotebookID string `json:"NotebookId"`
	MaxResults int    `json:"MaxResults"`
}

type listExecutorsInput struct {
	NextToken     string `json:"NextToken"`
	SessionID     string `json:"SessionId"`
	ExecutorState string `json:"ExecutorStateFilter"`
	MaxResults    int    `json:"MaxResults"`
}

type getResourceDashboardInput struct {
	ResourceARN string `json:"ResourceARN"`
}

func (h *Handler) handleStartSession(b []byte) (any, error) {
	var input startSessionInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	const secondsPerMinute = 60

	sessionCfg := SessionConfiguration{
		ExecutionRole: input.ExecutionRole,
		// StartSessionInput only carries minutes; the model stores IdleTimeoutSeconds (athena@v1.60.4).
		IdleTimeoutSeconds: int64(input.SessionIdleTimeoutInMinutes) * secondsPerMinute,
	}

	id, err := h.replayCreate(
		"StartSession", input.ClientRequestToken, input,
		found(h.Backend.GetSession),
		func() (string, error) {
			id, _, err := h.Backend.StartSession(
				input.WorkGroup, input.Description, input.NotebookVersion,
				input.EngineConfiguration, sessionCfg,
				input.MonitoringConfiguration, input.notebookID(),
			)

			return id, err
		},
	)
	if err != nil {
		return nil, err
	}

	tags := make(map[string]string, len(input.Tags))
	for _, t := range input.Tags {
		tags[t.Key] = t.Value
	}

	if err = h.Backend.TagSession(id, input.CopyWorkGroupTags, tags); err != nil {
		return nil, err
	}

	s, err := h.Backend.GetSession(id)
	if err != nil {
		return nil, err
	}

	return map[string]any{keySessionID: id, keyState: s.Status.State}, nil
}

func (h *Handler) sessionCoreOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"StartSession": h.handleStartSession,
		"GetSession": func(b []byte) (any, error) {
			var input sessionIDInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			s, err := h.Backend.GetSession(input.SessionID)
			if err != nil {
				return nil, err
			}

			return map[string]any{
				keySessionID:              s.SessionID,
				"Description":             s.Description,
				"WorkGroup":               s.WorkGroup,
				"EngineVersion":           pysparkEngineV3,
				"NotebookVersion":         s.NotebookVersion,
				"EngineConfiguration":     s.EngineConfiguration,
				"SessionConfiguration":    s.SessionConfiguration,
				"MonitoringConfiguration": s.MonitoringConfiguration,
				keyStatus:                 s.Status,
				keyStatistics:             s.Statistics,
			}, nil
		},
		"GetSessionStatus": func(b []byte) (any, error) {
			var input sessionIDInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			st, err := h.Backend.GetSessionStatus(input.SessionID)
			if err != nil {
				return nil, err
			}

			return map[string]any{keySessionID: input.SessionID, keyStatus: st}, nil
		},
		"GetSessionEndpoint": func(b []byte) (any, error) {
			var input sessionIDInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			url, authToken, authTokenExpiration, err := h.Backend.GetSessionEndpoint(input.SessionID)
			if err != nil {
				return nil, err
			}

			return map[string]any{
				"EndpointUrl":             url,
				"AuthToken":               authToken,
				"AuthTokenExpirationTime": authTokenExpiration,
			}, nil
		},
		"TerminateSession": func(b []byte) (any, error) {
			var input sessionIDInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			state, err := h.Backend.TerminateSession(input.SessionID)
			if err != nil {
				return nil, err
			}

			return map[string]any{keyState: state}, nil
		},
	}
}

func (h *Handler) sessionListOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"ListSessions": func(b []byte) (any, error) {
			var input listSessionsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			sums, err := h.Backend.ListSessions(input.WorkGroup, input.StateFilter)
			if err != nil {
				return nil, err
			}

			page, next, pageErr := pageByKey(
				h.tokens,
				sums,
				func(s SessionSummary) string { return s.SessionID },
				input.MaxResults,
				input.NextToken,
			)
			if pageErr != nil {
				return nil, pageErr
			}

			return withNextToken(map[string]any{"Sessions": page}, next), nil
		},
		"ListNotebookSessions": func(b []byte) (any, error) {
			var input listNotebookSessionsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			sums, err := h.Backend.ListNotebookSessions(input.NotebookID)
			if err != nil {
				return nil, err
			}

			page, next, pageErr := pageByKey(
				h.tokens,
				sums,
				func(s NotebookSessionSummary) string { return s.SessionID },
				input.MaxResults,
				input.NextToken,
			)
			if pageErr != nil {
				return nil, pageErr
			}

			return withNextToken(map[string]any{"NotebookSessionsList": page}, next), nil
		},
	}
}

// sessionInfoOps covers session-adjacent read-only info operations: executor
// listing, available engine versions/DPU sizes, and the session dashboard URL.
func (h *Handler) sessionInfoOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"ListExecutors": func(b []byte) (any, error) {
			var input listExecutorsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			execs, err := h.Backend.ListExecutors(input.SessionID, input.ExecutorState)
			if err != nil {
				return nil, err
			}

			page, next, pageErr := pageByKey(
				h.tokens,
				execs,
				func(e Executor) string { return e.ExecutorID },
				input.MaxResults,
				input.NextToken,
			)
			if pageErr != nil {
				return nil, pageErr
			}

			return withNextToken(map[string]any{"ExecutorsSummary": page, keySessionID: input.SessionID}, next), nil
		},
		"ListEngineVersions": func(b []byte) (any, error) {
			var input listPageInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			page, next, err := pageByKey(
				h.tokens,
				h.Backend.ListEngineVersions(),
				func(e EngineVersionDescriptor) string { return e.SelectedEngineVersion },
				input.MaxResults,
				input.NextToken,
			)
			if err != nil {
				return nil, err
			}

			return withNextToken(map[string]any{"EngineVersions": page}, next), nil
		},
		"ListApplicationDPUSizes": func(b []byte) (any, error) {
			var input listPageInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			page, next, err := pageByKey(
				h.tokens, h.Backend.ListApplicationDPUSizes(),
				func(a ApplicationDPUSizes) string { return a.ApplicationRuntimeID }, input.MaxResults, input.NextToken,
			)
			if err != nil {
				return nil, err
			}

			return withNextToken(map[string]any{"ApplicationDPUSizes": page}, next), nil
		},
		"GetResourceDashboard": func(b []byte) (any, error) {
			var input getResourceDashboardInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			url, err := h.Backend.GetResourceDashboard(input.ResourceARN)
			if err != nil {
				return nil, err
			}

			return map[string]any{"Url": url}, nil
		},
	}
}
