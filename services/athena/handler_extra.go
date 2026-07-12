package athena

import (
	"encoding/json"
	"maps"
)

// --- Response key constants ---

const (
	keySessionID  = "SessionId"
	keyState      = "State"
	keyStatus     = "Status"
	keyStatistics = "Statistics"
)

// --- Input types for new operations ---

type startSessionInput struct {
	WorkGroup            string               `json:"WorkGroup"`
	Description          string               `json:"Description"`
	NotebookVersion      string               `json:"NotebookVersion"`
	NotebookID           string               `json:"NotebookId"`
	SessionConfiguration SessionConfiguration `json:"SessionConfiguration"`
	EngineConfiguration  EngineConfiguration  `json:"EngineConfiguration"`
}

type sessionIDInput struct {
	SessionID string `json:"SessionId"`
}

type listSessionsInput struct {
	WorkGroup   string `json:"WorkGroup"`
	StateFilter string `json:"StateFilter"`
}

type listNotebookSessionsInput struct {
	NotebookID string `json:"NotebookId"`
}

type startCalculationInput struct {
	SessionID          string `json:"SessionId"`
	Description        string `json:"Description"`
	CodeBlock          string `json:"CodeBlock"`
	ClientRequestToken string `json:"ClientRequestToken"`
}

type calculationIDInput struct {
	CalculationExecutionID string `json:"CalculationExecutionId"`
}

type listCalculationsInput struct {
	SessionID   string `json:"SessionId"`
	StateFilter string `json:"StateFilter"`
}

type capacityReservationNameInput struct {
	Name string `json:"Name"`
}

type updateCapacityReservationInput struct {
	Name       string `json:"Name"`
	TargetDpus int32  `json:"TargetDpus"`
}

type putCapacityAssignmentInput struct {
	CapacityReservationName string               `json:"CapacityReservationName"`
	CapacityAssignments     []CapacityAssignment `json:"CapacityAssignments"`
}

type getCapacityAssignmentInput struct {
	CapacityReservationName string `json:"CapacityReservationName"`
}

type getDatabaseInput struct {
	CatalogName  string `json:"CatalogName"`
	DatabaseName string `json:"DatabaseName"`
}

type listDatabasesInput struct {
	CatalogName string `json:"CatalogName"`
}

type getTableMetadataInput struct {
	CatalogName  string `json:"CatalogName"`
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
}

type listTableMetadataInput struct {
	CatalogName  string `json:"CatalogName"`
	DatabaseName string `json:"DatabaseName"`
	Expression   string `json:"Expression"`
}

type notebookIDInput struct {
	NotebookID string `json:"NotebookId"`
}

type listNotebookMetadataInput struct {
	WorkGroup string                     `json:"WorkGroup"`
	Filters   listNotebookMetadataFilter `json:"Filters"`
}

type listNotebookMetadataFilter struct {
	Name string `json:"Name"`
}

type importNotebookInput struct {
	WorkGroup string `json:"WorkGroup"`
	Name      string `json:"Name"`
	Payload   string `json:"Payload"`
	Type      string `json:"Type"`
}

type updateNotebookInput struct {
	NotebookID string `json:"NotebookId"`
	Payload    string `json:"Payload"`
	Type       string `json:"Type"`
	SessionID  string `json:"SessionId"`
}

type updateNotebookMetadataInput struct {
	NotebookID string `json:"NotebookId"`
	Name       string `json:"Name"`
}

type updateNamedQueryInput struct {
	NamedQueryID string `json:"NamedQueryId"`
	Name         string `json:"Name"`
	Description  string `json:"Description"`
	QueryString  string `json:"QueryString"`
}

type updatePreparedStatementInput struct {
	StatementName  string `json:"StatementName"`
	WorkGroup      string `json:"WorkGroup"`
	QueryStatement string `json:"QueryStatement"`
	Description    string `json:"Description"`
}

type listExecutorsInput struct {
	SessionID     string `json:"SessionId"`
	ExecutorState string `json:"ExecutorStateFilter"`
}

type getQueryRuntimeStatsInput struct {
	QueryExecutionID string `json:"QueryExecutionId"`
}

type getResourceDashboardInput struct {
	ResourceARN string `json:"ResourceARN"`
}

// extendedSupportedOperations are the operations added in this iteration.
func (h *Handler) extendedSupportedOperations() []string {
	return []string{
		"StartSession",
		"GetSession",
		"GetSessionStatus",
		"GetSessionEndpoint",
		"TerminateSession",
		"ListSessions",
		"ListNotebookSessions",
		"StartCalculationExecution",
		"GetCalculationExecution",
		"GetCalculationExecutionStatus",
		"GetCalculationExecutionCode",
		"StopCalculationExecution",
		"ListCalculationExecutions",
		"GetCapacityReservation",
		"ListCapacityReservations",
		"UpdateCapacityReservation",
		"PutCapacityAssignmentConfiguration",
		"GetCapacityAssignmentConfiguration",
		"GetDatabase",
		"ListDatabases",
		"GetTableMetadata",
		"ListTableMetadata",
		"GetNotebookMetadata",
		"ListNotebookMetadata",
		"ImportNotebook",
		"UpdateNotebook",
		"UpdateNotebookMetadata",
		"UpdateNamedQuery",
		"UpdatePreparedStatement",
		"ListExecutors",
		"ListEngineVersions",
		"ListApplicationDPUSizes",
		"GetQueryRuntimeStatistics",
		"GetResourceDashboard",
	}
}

func (h *Handler) sessionCoreOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"StartSession": func(b []byte) (any, error) {
			var input startSessionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			id, state, err := h.Backend.StartSession(
				input.WorkGroup, input.Description, input.NotebookVersion,
				input.EngineConfiguration, input.SessionConfiguration, input.NotebookID,
			)
			if err != nil {
				return nil, err
			}

			return map[string]any{keySessionID: id, keyState: state}, nil
		},
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
				keySessionID:           s.SessionID,
				"Description":          s.Description,
				"WorkGroup":            s.WorkGroup,
				"EngineVersion":        s.NotebookVersion,
				"NotebookVersion":      s.NotebookVersion,
				"EngineConfiguration":  s.EngineConfiguration,
				"SessionConfiguration": s.SessionConfiguration,
				keyStatus:              s.Status,
				keyStatistics:          s.Statistics,
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

			return map[string]any{"Sessions": sums}, nil
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

			return map[string]any{"NotebookSessionsList": sums}, nil
		},
	}
}

func (h *Handler) calcCoreOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"StartCalculationExecution": func(b []byte) (any, error) {
			var input startCalculationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			id, state, err := h.Backend.StartCalculationExecution(input.SessionID, input.Description, input.CodeBlock)
			if err != nil {
				return nil, err
			}

			return map[string]any{"CalculationExecutionId": id, keyState: state}, nil
		},
		"GetCalculationExecution": func(b []byte) (any, error) {
			var input calculationIDInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			c, err := h.Backend.GetCalculationExecution(input.CalculationExecutionID)
			if err != nil {
				return nil, err
			}

			return map[string]any{
				"CalculationExecutionId": c.CalculationID,
				keySessionID:             c.SessionID,
				"Description":            c.Description,
				"WorkingDirectory":       c.WorkingDir,
				keyStatus:                c.Status,
				keyStatistics:            c.Statistics,
				"Result":                 c.Result,
			}, nil
		},
		"GetCalculationExecutionStatus": func(b []byte) (any, error) {
			var input calculationIDInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			st, stats, err := h.Backend.GetCalculationExecutionStatus(input.CalculationExecutionID)
			if err != nil {
				return nil, err
			}

			return map[string]any{keyStatus: st, keyStatistics: stats}, nil
		},
		"GetCalculationExecutionCode": func(b []byte) (any, error) {
			var input calculationIDInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			code, err := h.Backend.GetCalculationExecutionCode(input.CalculationExecutionID)
			if err != nil {
				return nil, err
			}

			return map[string]any{"CodeBlock": code}, nil
		},
	}
}

func (h *Handler) calcControlOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"StopCalculationExecution": func(b []byte) (any, error) {
			var input calculationIDInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			state, err := h.Backend.StopCalculationExecution(input.CalculationExecutionID)
			if err != nil {
				return nil, err
			}

			return map[string]any{keyState: state}, nil
		},
		"ListCalculationExecutions": func(b []byte) (any, error) {
			var input listCalculationsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			sums, err := h.Backend.ListCalculationExecutions(input.SessionID, input.StateFilter)
			if err != nil {
				return nil, err
			}

			return map[string]any{"Calculations": sums}, nil
		},
	}
}

func (h *Handler) capacityExtraOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"GetCapacityReservation": func(b []byte) (any, error) {
			var input capacityReservationNameInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			cr, err := h.Backend.GetCapacityReservation(input.Name)
			if err != nil {
				return nil, err
			}

			return map[string]any{"CapacityReservation": cr}, nil
		},
		"ListCapacityReservations": func(_ []byte) (any, error) {
			list, err := h.Backend.ListCapacityReservations()
			if err != nil {
				return nil, err
			}

			return map[string]any{"CapacityReservations": list}, nil
		},
		"UpdateCapacityReservation": func(b []byte) (any, error) {
			var input updateCapacityReservationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.UpdateCapacityReservation(input.Name, input.TargetDpus)
		},
		"PutCapacityAssignmentConfiguration": func(b []byte) (any, error) {
			var input putCapacityAssignmentInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.PutCapacityAssignmentConfiguration(
				input.CapacityReservationName, input.CapacityAssignments,
			)
		},
		"GetCapacityAssignmentConfiguration": func(b []byte) (any, error) {
			var input getCapacityAssignmentInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			cfg, err := h.Backend.GetCapacityAssignmentConfiguration(input.CapacityReservationName)
			if err != nil {
				return nil, err
			}

			return map[string]any{"CapacityAssignmentConfiguration": cfg}, nil
		},
	}
}

func (h *Handler) metadataOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"GetDatabase": func(b []byte) (any, error) {
			var input getDatabaseInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			db, err := h.Backend.GetDatabase(input.CatalogName, input.DatabaseName)
			if err != nil {
				return nil, err
			}

			return map[string]any{"Database": db}, nil
		},
		"ListDatabases": func(b []byte) (any, error) {
			var input listDatabasesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			dbs, err := h.Backend.ListDatabases(input.CatalogName)
			if err != nil {
				return nil, err
			}

			return map[string]any{"DatabaseList": dbs}, nil
		},
		"GetTableMetadata": func(b []byte) (any, error) {
			var input getTableMetadataInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			t, err := h.Backend.GetTableMetadata(input.CatalogName, input.DatabaseName, input.TableName)
			if err != nil {
				return nil, err
			}

			return map[string]any{"TableMetadata": t}, nil
		},
		"ListTableMetadata": func(b []byte) (any, error) {
			var input listTableMetadataInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			tables, err := h.Backend.ListTableMetadata(input.CatalogName, input.DatabaseName, input.Expression)
			if err != nil {
				return nil, err
			}

			return map[string]any{"TableMetadataList": tables}, nil
		},
	}
}

func (h *Handler) notebookExtraOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"GetNotebookMetadata": func(b []byte) (any, error) {
			var input notebookIDInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			meta, err := h.Backend.GetNotebookMetadata(input.NotebookID)
			if err != nil {
				return nil, err
			}

			return map[string]any{"NotebookMetadata": meta}, nil
		},
		"ListNotebookMetadata": func(b []byte) (any, error) {
			var input listNotebookMetadataInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			list, err := h.Backend.ListNotebookMetadata(input.WorkGroup, input.Filters.Name)
			if err != nil {
				return nil, err
			}

			return map[string]any{"NotebookMetadataList": list}, nil
		},
		"ImportNotebook": func(b []byte) (any, error) {
			var input importNotebookInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			id, err := h.Backend.ImportNotebook(input.WorkGroup, input.Name, input.Payload, input.Type)
			if err != nil {
				return nil, err
			}

			return map[string]any{"NotebookId": id}, nil
		},
		"UpdateNotebook": func(b []byte) (any, error) {
			var input updateNotebookInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.UpdateNotebook(input.NotebookID, input.Payload, input.Type, input.SessionID)
		},
		"UpdateNotebookMetadata": func(b []byte) (any, error) {
			var input updateNotebookMetadataInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.UpdateNotebookMetadata(input.NotebookID, input.Name)
		},
	}
}

func (h *Handler) miscOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"UpdateNamedQuery": func(b []byte) (any, error) {
			var input updateNamedQueryInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.UpdateNamedQuery(
				input.NamedQueryID,
				input.Name,
				input.Description,
				input.QueryString,
			)
		},
		"UpdatePreparedStatement": func(b []byte) (any, error) {
			var input updatePreparedStatementInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.UpdatePreparedStatement(
				input.StatementName, input.WorkGroup, input.QueryStatement, input.Description,
			)
		},
		"ListExecutors": func(b []byte) (any, error) {
			var input listExecutorsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			execs, err := h.Backend.ListExecutors(input.SessionID, input.ExecutorState)
			if err != nil {
				return nil, err
			}

			return map[string]any{"ExecutorsSummary": execs, keySessionID: input.SessionID}, nil
		},
		"ListEngineVersions": func(_ []byte) (any, error) {
			return map[string]any{"EngineVersions": h.Backend.ListEngineVersions()}, nil
		},
		"ListApplicationDPUSizes": func(_ []byte) (any, error) {
			return map[string]any{"ApplicationDPUSizes": h.Backend.ListApplicationDPUSizes()}, nil
		},
		"GetQueryRuntimeStatistics": func(b []byte) (any, error) {
			var input getQueryRuntimeStatsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			stats, err := h.Backend.GetQueryRuntimeStatistics(input.QueryExecutionID)
			if err != nil {
				return nil, err
			}

			return map[string]any{"QueryRuntimeStatistics": stats}, nil
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

// extendedDispatchTable returns the dispatch entries for the new operations.
func (h *Handler) extendedDispatchTable() map[string]athenaActionFn {
	ops := h.sessionCoreOps()
	maps.Copy(ops, h.sessionListOps())
	maps.Copy(ops, h.calcCoreOps())
	maps.Copy(ops, h.calcControlOps())
	maps.Copy(ops, h.capacityExtraOps())
	maps.Copy(ops, h.metadataOps())
	maps.Copy(ops, h.notebookExtraOps())
	maps.Copy(ops, h.miscOps())

	return ops
}
