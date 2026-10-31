package ocilogging

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	cfgTypeLogging    = "LOGGING"
	cfgTypeMonitoring = "MONITORING"
	cfgStateValid     = "VALID"
)

type groupAssociationWire struct {
	GroupList []string `json:"groupList"`
}

// CreateAgentConfigIn is the CreateUnifiedAgentConfiguration request body.
type CreateAgentConfigIn struct {
	Tags
	Enabled       *bool                 `json:"isEnabled"`
	Description   *string               `json:"description"`
	GroupAssoc    *groupAssociationWire `json:"groupAssociation"`
	Name          string                `json:"displayName"`
	Compartment   string                `json:"compartmentId"`
	ServiceConfig json.RawMessage       `json:"serviceConfiguration"`
}

// UpdateAgentConfigIn is the UpdateUnifiedAgentConfiguration request body.
type UpdateAgentConfigIn struct {
	Tags
	Enabled       *bool                 `json:"isEnabled"`
	Description   *string               `json:"description"`
	Name          *string               `json:"displayName"`
	GroupAssoc    *groupAssociationWire `json:"groupAssociation"`
	ServiceConfig json.RawMessage       `json:"serviceConfiguration"`
}

// AgentConfigWire is the GetUnifiedAgentConfiguration response.
type AgentConfigWire struct {
	TimeCreated        time.Time                 `json:"timeCreated"`
	TimeModified       time.Time                 `json:"timeLastModified"`
	FreeformTags       map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags        map[string]map[string]any `json:"definedTags,omitempty"`
	ID                 string                    `json:"id"`
	CompartmentID      string                    `json:"compartmentId"`
	Name               string                    `json:"displayName"`
	Description        string                    `json:"description,omitempty"`
	LifecycleState     string                    `json:"lifecycleState"`
	ConfigurationState string                    `json:"configurationState"`
	GroupAssociation   groupAssociationWire      `json:"groupAssociation"`
	ServiceConfig      json.RawMessage           `json:"serviceConfiguration"`
	Enabled            bool                      `json:"isEnabled"`
}

type agentSummaryWire struct {
	TimeCreated        time.Time                 `json:"timeCreated"`
	TimeModified       time.Time                 `json:"timeLastModified"`
	FreeformTags       map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags        map[string]map[string]any `json:"definedTags,omitempty"`
	ID                 string                    `json:"id"`
	CompartmentID      string                    `json:"compartmentId"`
	Name               string                    `json:"displayName"`
	Description        string                    `json:"description,omitempty"`
	LifecycleState     string                    `json:"lifecycleState"`
	ConfigurationType  string                    `json:"configurationType"`
	ConfigurationState string                    `json:"configurationState"`
	Enabled            bool                      `json:"isEnabled"`
}

func agentView(a *AgentConfig) AgentConfigWire {
	groups := a.Groups
	if groups == nil {
		groups = []string{}
	}

	return AgentConfigWire{
		ID: a.ID, CompartmentID: a.CompartmentID, Name: a.Name, Description: a.Description, LifecycleState: a.State,
		ConfigurationState: cfgStateValid, Enabled: a.Enabled, ServiceConfig: a.ServiceConfig,
		GroupAssociation: groupAssociationWire{GroupList: groups}, TimeCreated: a.TimeCreated,
		TimeModified: a.TimeModified, FreeformTags: a.Tags.FreeformTags, DefinedTags: a.Tags.DefinedTags,
	}
}

// validateServiceConfig checks the polymorphic serviceConfiguration and returns its configurationType.
func (b *InMemoryBackend) validateServiceConfig(raw json.RawMessage) (string, *APIError) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return "", errMissing("serviceConfiguration is required")
	}

	var c struct {
		Destination *struct {
			LogObjectID string `json:"logObjectId"`
		} `json:"destination"`
		ConfigurationType string `json:"configurationType"`
		Sources           []struct {
			Name       string `json:"name"`
			SourceType string `json:"sourceType"`
		} `json:"sources"`
	}

	if err := json.Unmarshal(raw, &c); err != nil {
		return "", errParam("serviceConfiguration is not a valid configuration object")
	}

	switch {
	case strs.Equal(c.ConfigurationType, cfgTypeMonitoring):
		return cfgTypeMonitoring, nil
	case !strs.Equal(c.ConfigurationType, cfgTypeLogging):
		return "", errParam("serviceConfiguration.configurationType must be LOGGING or MONITORING")
	}

	if len(c.Sources) == 0 {
		return "", errParam("serviceConfiguration.sources must contain at least one source")
	}

	for _, s := range c.Sources {
		if s.Name == "" || s.SourceType == "" {
			return "", errParam("every source needs a name and a sourceType")
		}
	}

	if c.Destination == nil || c.Destination.LogObjectID == "" {
		return "", errMissing("serviceConfiguration.destination.logObjectId is required")
	}

	if _, ok := b.logs[c.Destination.LogObjectID]; !ok {
		return "", errParam("destination log " + c.Destination.LogObjectID + " does not exist")
	}

	return cfgTypeLogging, nil
}

func groupsOf(g *groupAssociationWire) []string {
	if g == nil {
		return nil
	}

	return g.GroupList
}

// CreateAgentConfig creates a unified agent configuration.
func (b *InMemoryBackend) CreateAgentConfig(in CreateAgentConfigIn) (string, error) {
	if in.Compartment == "" {
		return "", errMissing("compartmentId is required")
	}

	if in.Enabled == nil {
		return "", errMissing("isEnabled is required")
	}

	if aerr := firstErr(validateName(in.Name), validDescription(in.Description)); aerr != nil {
		return "", aerr
	}

	b.mu.Lock("CreateAgentConfig")
	defer b.mu.Unlock()

	b.settleAll()

	cfgType, aerr := b.validateServiceConfig(in.ServiceConfig)
	if aerr != nil {
		return "", aerr
	}

	now := b.nowUTC()
	a := &AgentConfig{
		ID: newOCID("unifiedagentconfiguration"), CompartmentID: in.Compartment, Name: in.Name, State: stateCreating,
		Enabled: *in.Enabled, ServiceConfig: in.ServiceConfig, ConfigType: cfgType, Groups: groupsOf(in.GroupAssoc),
		TimeCreated: now, TimeModified: now, TransitionAt: b.transitionAt(), Tags: in.Tags, Etag: newEtag(),
		Seq: b.nextSeq(),
	}
	if in.Description != nil {
		a.Description = *in.Description
	}

	b.agents[a.ID] = a

	return b.newWR("CREATE_CONFIGURATION", "unifiedagentconfiguration", a.CompartmentID, a.ID, "CREATED",
		*a.TransitionAt), nil
}

func (b *InMemoryBackend) mutableAgent(id string) (*AgentConfig, *APIError) {
	a, ok := b.agents[id]
	if !ok {
		return nil, errNotFound(id)
	}

	if a.State != stateActive {
		return nil, errState(
			"Agent configuration " + id + " is in state " + a.State + "; the operation requires ACTIVE",
		)
	}

	return a, nil
}

// GetAgentConfig returns one agent configuration and its etag.
func (b *InMemoryBackend) GetAgentConfig(id string) (AgentConfigWire, string, error) {
	b.mu.Lock("GetAgentConfig")
	defer b.mu.Unlock()

	b.settleAll()

	a, ok := b.agents[id]
	if !ok {
		return AgentConfigWire{}, "", errNotFound(id)
	}

	return agentView(a), a.Etag, nil
}

// AgentFilter narrows ListUnifiedAgentConfigurations.
type AgentFilter struct {
	Compartment, Name, State, LogID, GroupID string
}

// ListAgentConfigs lists the agent configurations of a compartment.
func (b *InMemoryBackend) ListAgentConfigs(f AgentFilter, o ListOpts) (page.Page[agentSummaryWire], error) {
	if f.Compartment == "" {
		return page.Page[agentSummaryWire]{}, errMissing("compartmentId is required")
	}

	b.mu.Lock("ListAgentConfigs")
	defer b.mu.Unlock()

	b.settleAll()

	var all []*AgentConfig

	for _, a := range b.agents {
		if a.CompartmentID != f.Compartment || (f.Name != "" && a.Name != f.Name) ||
			(f.State != "" && !strs.Equal(a.State, f.State)) ||
			(f.LogID != "" && !bytes.Contains(a.ServiceConfig, []byte(`"`+f.LogID+`"`))) ||
			(f.GroupID != "" && !strs.ContainsFold(a.Groups, f.GroupID)) {
			continue
		}

		all = append(all, a)
	}

	pg := paginate(all, o, func(a *AgentConfig) string { return a.Name },
		func(a *AgentConfig) (time.Time, int64) { return a.TimeCreated, a.Seq })

	out := make([]agentSummaryWire, 0, len(pg.Data))
	for _, a := range pg.Data {
		out = append(out, agentSummaryWire{
			ID: a.ID, CompartmentID: a.CompartmentID, Name: a.Name, Description: a.Description,
			LifecycleState: a.State, ConfigurationType: a.ConfigType, ConfigurationState: cfgStateValid,
			Enabled: a.Enabled, TimeCreated: a.TimeCreated, TimeModified: a.TimeModified,
			FreeformTags: a.Tags.FreeformTags, DefinedTags: a.Tags.DefinedTags,
		})
	}

	return page.Page[agentSummaryWire]{Data: out, Next: pg.Next}, nil
}

// UpdateAgentConfig replaces the mutable fields of an agent configuration.
func (b *InMemoryBackend) UpdateAgentConfig(id, ifMatch string, in UpdateAgentConfigIn) (string, error) {
	if in.Enabled == nil {
		return "", errMissing("isEnabled is required")
	}

	if in.Name == nil {
		return "", errMissing("displayName is required")
	}

	if aerr := firstErr(validateName(*in.Name), validDescription(in.Description)); aerr != nil {
		return "", aerr
	}

	b.mu.Lock("UpdateAgentConfig")
	defer b.mu.Unlock()

	b.settleAll()

	a, aerr := b.mutableAgent(id)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(a.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	cfgType, aerr := b.validateServiceConfig(in.ServiceConfig)
	if aerr != nil {
		return "", aerr
	}

	a.Name, a.Enabled, a.ServiceConfig, a.ConfigType = *in.Name, *in.Enabled, in.ServiceConfig, cfgType
	if in.Description != nil {
		a.Description = *in.Description
	}

	if in.GroupAssoc != nil {
		a.Groups = in.GroupAssoc.GroupList
	}

	a.Tags.merge(in.Tags)
	a.State, a.TransitionAt, a.TimeModified, a.Etag = stateUpdating, b.transitionAt(), b.nowUTC(), newEtag()

	return b.newWR("UPDATE_CONFIGURATION", "unifiedagentconfiguration", a.CompartmentID, a.ID, "UPDATED",
		*a.TransitionAt), nil
}

// DeleteAgentConfig moves an agent configuration to DELETING.
func (b *InMemoryBackend) DeleteAgentConfig(id, ifMatch string) (string, error) {
	b.mu.Lock("DeleteAgentConfig")
	defer b.mu.Unlock()

	b.settleAll()

	a, aerr := b.mutableAgent(id)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(a.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	a.State, a.TransitionAt, a.TimeModified, a.Etag = stateDeleting, b.transitionAt(), b.nowUTC(), newEtag()

	return b.newWR("DELETE_CONFIGURATION", "unifiedagentconfiguration", a.CompartmentID, a.ID, "DELETED",
		*a.TransitionAt), nil
}

// ChangeAgentConfigCompartment moves an agent configuration to another compartment.
func (b *InMemoryBackend) ChangeAgentConfigCompartment(id, ifMatch, compartment string) (string, error) {
	if compartment == "" {
		return "", errMissing("compartmentId is required")
	}

	b.mu.Lock("ChangeAgentConfigCompartment")
	defer b.mu.Unlock()

	b.settleAll()

	a, aerr := b.mutableAgent(id)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(a.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	a.CompartmentID, a.Etag, a.TimeModified = compartment, newEtag(), b.nowUTC()

	return b.newWR("MOVE_CONFIGURATION", "unifiedagentconfiguration", compartment, a.ID, "UPDATED", b.nowUTC()), nil
}

func (h *Handler) createAgentConfig(q *req) error {
	var in CreateAgentConfigIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.CreateAgentConfig(in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) getAgentConfig(q *req) error {
	v, etag, err := h.Backend.GetAgentConfig(q.param("unifiedAgentConfigurationId"))

	return reply(q, v, etag, err)
}

func (h *Handler) listAgentConfigs(q *req) error {
	pg, err := h.Backend.ListAgentConfigs(AgentFilter{
		Compartment: q.query("compartmentId"), Name: q.query("displayName"), State: q.query("lifecycleState"),
		LogID: q.query("logId"), GroupID: q.query("groupId"),
	}, q.listOpts())
	if err != nil {
		return err
	}

	q.nextPage(pg.Next)

	data := pg.Data
	if data == nil {
		data = []agentSummaryWire{}
	}

	return q.json(http.StatusOK, map[string]any{"items": data}, "")
}

func (h *Handler) updateAgentConfig(q *req) error {
	var in UpdateAgentConfigIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.UpdateAgentConfig(q.param("unifiedAgentConfigurationId"), q.ifMatch(), in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) deleteAgentConfig(q *req) error {
	wr, err := h.Backend.DeleteAgentConfig(q.param("unifiedAgentConfigurationId"), q.ifMatch())

	return acceptedWR(q, wr, err)
}

func (h *Handler) changeAgentConfigCompartment(q *req) error {
	var in changeCompartmentIn
	if err := q.decode(&in); err != nil {
		return err
	}

	id := q.param("unifiedAgentConfigurationId")
	wr, err := h.Backend.ChangeAgentConfigCompartment(id, q.ifMatch(), in.CompartmentID)

	return acceptedWR(q, wr, err)
}
