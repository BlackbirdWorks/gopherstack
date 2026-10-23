package bedrockagentcore

import (
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func routes() []restroute.Route {
	const (
		mem   = "/memories/{memoryId}"
		ci    = "/code-interpreters/{codeInterpreterIdentifier}"
		br    = "/browsers/{browserIdentifier}"
		rt    = "/runtimes/{agentRuntimeArn}"
		event = mem + "/actor/{actorId}/sessions/{sessionId}"
	)

	return []restroute.Route{
		{Method: http.MethodPost, Path: rt + "/invocations", Op: "InvokeAgentRuntime"},
		{Method: http.MethodGet, Path: rt + "/invocations/.well-known/agent-card.json", Op: "GetAgentCard"},
		{Method: http.MethodPost, Path: rt + "/stopruntimesession", Op: "StopRuntimeSession"},

		{Method: http.MethodPost, Path: mem + "/events", Op: "CreateEvent"},
		{Method: http.MethodGet, Path: event + "/events/{eventId}", Op: "GetEvent"},
		{Method: http.MethodDelete, Path: event + "/events/{eventId}", Op: "DeleteEvent"},
		{Method: http.MethodPost, Path: event, Op: "ListEvents"},
		{Method: http.MethodPost, Path: mem + "/actor/{actorId}/sessions", Op: "ListSessions"},
		{Method: http.MethodPost, Path: mem + "/actors", Op: "ListActors"},
		{Method: http.MethodPost, Path: mem + "/retrieve", Op: "RetrieveMemoryRecords"},
		{Method: http.MethodPost, Path: mem + "/memoryRecords", Op: "ListMemoryRecords"},
		{Method: http.MethodPost, Path: mem + "/memoryRecords/batchCreate", Op: "BatchCreateMemoryRecords"},
		{Method: http.MethodPost, Path: mem + "/memoryRecords/batchUpdate", Op: "BatchUpdateMemoryRecords"},
		{Method: http.MethodPost, Path: mem + "/memoryRecords/batchDelete", Op: "BatchDeleteMemoryRecords"},
		{Method: http.MethodGet, Path: mem + "/memoryRecord/{memoryRecordId}", Op: "GetMemoryRecord"},
		{Method: http.MethodDelete, Path: mem + "/memoryRecords/{memoryRecordId}", Op: "DeleteMemoryRecord"},

		{Method: http.MethodPut, Path: ci + "/sessions/start", Op: "StartCodeInterpreterSession"},
		{Method: http.MethodPut, Path: ci + "/sessions/stop", Op: "StopCodeInterpreterSession"},
		{Method: http.MethodGet, Path: ci + "/sessions/get", Op: "GetCodeInterpreterSession"},
		{Method: http.MethodPost, Path: ci + "/sessions/list", Op: "ListCodeInterpreterSessions"},
		{Method: http.MethodPost, Path: ci + "/tools/invoke", Op: "InvokeCodeInterpreter"},

		{Method: http.MethodPut, Path: br + "/sessions/start", Op: "StartBrowserSession"},
		{Method: http.MethodPut, Path: br + "/sessions/stop", Op: "StopBrowserSession"},
		{Method: http.MethodGet, Path: br + "/sessions/get", Op: "GetBrowserSession"},
		{Method: http.MethodPost, Path: br + "/sessions/list", Op: "ListBrowserSessions"},

		{Method: http.MethodPost, Path: "/identities/GetWorkloadAccessToken", Op: "GetWorkloadAccessToken"},
		{Method: http.MethodPost, Path: "/identities/GetWorkloadAccessTokenForJWT", Op: "GetWorkloadAccessTokenForJWT"},
		{
			Method: http.MethodPost, Path: "/identities/GetWorkloadAccessTokenForUserId",
			Op: "GetWorkloadAccessTokenForUserId",
		},
		{Method: http.MethodPost, Path: "/identities/api-key", Op: "GetResourceApiKey"},
		{Method: http.MethodPost, Path: "/identities/oauth2/token", Op: "GetResourceOauth2Token"},
	}
}

func (b *InMemoryBackend) ops() map[string]restroute.OpFunc {
	return map[string]restroute.OpFunc{
		"CreateEvent":              b.createEvent,
		"GetEvent":                 b.getEvent,
		"DeleteEvent":              b.deleteEvent,
		"ListEvents":               b.listEvents,
		"ListSessions":             b.listSessions,
		"ListActors":               b.listActors,
		"RetrieveMemoryRecords":    b.retrieveRecords,
		"ListMemoryRecords":        b.listRecords,
		"BatchCreateMemoryRecords": b.batchCreateRecords,
		"BatchUpdateMemoryRecords": b.batchUpdateRecords,
		"BatchDeleteMemoryRecords": b.batchDeleteRecords,
		"GetMemoryRecord":          b.getRecord,
		"DeleteMemoryRecord":       b.deleteRecord,

		"StartCodeInterpreterSession": b.startSession(ToolCodeInterpreter),
		"StopCodeInterpreterSession":  b.stopSession(ToolCodeInterpreter),
		"GetCodeInterpreterSession":   b.getSession(ToolCodeInterpreter),
		"ListCodeInterpreterSessions": b.listSessionsOf(ToolCodeInterpreter),

		"StartBrowserSession": b.startSession(ToolBrowser),
		"StopBrowserSession":  b.stopSession(ToolBrowser),
		"GetBrowserSession":   b.getSession(ToolBrowser),
		"ListBrowserSessions": b.listSessionsOf(ToolBrowser),

		"GetWorkloadAccessToken":          b.workloadToken(noUser),
		"GetWorkloadAccessTokenForJWT":    b.workloadToken(userFromJWT),
		"GetWorkloadAccessTokenForUserId": b.workloadToken(userFromID),
		"GetResourceApiKey":               b.getResourceAPIKey,
		"GetResourceOauth2Token":          b.getResourceOAuth2Token,
	}
}
