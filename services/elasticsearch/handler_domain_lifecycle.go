package elasticsearch

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// cancelSoftwareUpdateRequest is the JSON body for CancelElasticsearchServiceSoftwareUpdate.
type cancelSoftwareUpdateRequest struct {
	DomainName string `json:"DomainName"`
}

// serviceSoftwareOptionsJSON is the JSON representation of software update options.
// AutomatedUpdateDate is restjson1's unixTimestamp wire format (a JSON Number,
// deserializers.go:14366 awsRestjson1_deserializeDocumentServiceSoftwareOptions)
// -- a plain string field here always failed a real client's decode even when
// empty. Omitted entirely: this backend tracks no scheduled-update date.
type serviceSoftwareOptionsJSON struct {
	AutomatedUpdateDate *float64 `json:"AutomatedUpdateDate,omitempty"`
	CurrentVersion      string   `json:"CurrentVersion"`
	NewVersion          string   `json:"NewVersion"`
	UpdateStatus        string   `json:"UpdateStatus"`
	Description         string   `json:"Description"`
	UpdateAvailable     bool     `json:"UpdateAvailable"`
	Cancellable         bool     `json:"Cancellable"`
	OptionalDeployment  bool     `json:"OptionalDeployment"`
}

// cancelSoftwareUpdateOutput is the response for CancelElasticsearchServiceSoftwareUpdate.
type cancelSoftwareUpdateOutput struct {
	ServiceSoftwareOptions serviceSoftwareOptionsJSON `json:"ServiceSoftwareOptions"`
}

func (h *Handler) handleCancelElasticsearchServiceSoftwareUpdate(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req cancelSoftwareUpdateRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	_, cancelErr := h.Backend.CancelElasticsearchServiceSoftwareUpdate(h.reqContext(r), req.DomainName)
	if cancelErr != nil {
		if errors.Is(cancelErr, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", cancelErr.Error())
		} else {
			h.writeError(r, w, http.StatusInternalServerError, "InternalException", cancelErr.Error())
		}

		return
	}

	h.writeJSON(r, w, cancelSoftwareUpdateOutput{
		ServiceSoftwareOptions: serviceSoftwareOptionsJSON{
			UpdateAvailable: false,
			Cancellable:     false,
			UpdateStatus:    "NOT_ELIGIBLE",
			Description:     "No software update scheduled",
		},
	})
}

func (h *Handler) handleDeleteElasticsearchServiceRole(w http.ResponseWriter, r *http.Request) {
	if err := h.Backend.DeleteElasticsearchServiceRole(); err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleStartElasticsearchServiceSoftwareUpdate(w http.ResponseWriter, r *http.Request) {
	var req cancelSoftwareUpdateRequest
	if !h.decodeRequest(w, r, &req) {
		return
	}

	if _, err := h.Backend.StartElasticsearchServiceSoftwareUpdate(h.reqContext(r), req.DomainName); err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	h.writeJSON(r, w, map[string]any{"ServiceSoftwareOptions": map[string]any{
		"UpdateStatus": "PENDING_UPDATE",
		"Cancellable":  true,
	}})
}

func (h *Handler) handleGetUpgradeHistory(w http.ResponseWriter, r *http.Request) {
	domainName := pathID(r.URL.Path, elasticsearchUpgradeDomain+"/", "/history")

	records, err := h.Backend.GetUpgradeHistory(h.reqContext(r), domainName)
	if err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	maxResults, _ := strconv.Atoi(r.URL.Query().Get("maxResults"))
	pg := page.New(records, r.URL.Query().Get("nextToken"), maxResults, defaultUpgradeHistoryPage)

	histories := make([]map[string]any, 0, len(pg.Data))
	for _, rec := range pg.Data {
		steps := make([]map[string]any, 0, len(rec.Steps))
		for _, step := range rec.Steps {
			steps = append(steps, map[string]any{
				"UpgradeStep":       step,
				"UpgradeStepStatus": upgradeStatusSucceeded,
				"ProgressPercent":   upgradeProgressComplete,
			})
		}

		histories = append(histories, map[string]any{
			"UpgradeName":    rec.Name,
			"StartTimestamp": awstime.Epoch(rec.StartTimestamp),
			"UpgradeStatus":  upgradeStatusSucceeded,
			"StepsList":      steps,
		})
	}

	out := map[string]any{"UpgradeHistories": histories}
	if pg.Next != "" {
		out["NextToken"] = pg.Next
	}

	h.writeJSON(r, w, out)
}

func (h *Handler) handleGetUpgradeStatus(w http.ResponseWriter, r *http.Request) {
	domainName := pathID(r.URL.Path, elasticsearchUpgradeDomain+"/", "/status")

	rec, ok, err := h.Backend.GetUpgradeStatus(h.reqContext(r), domainName)
	if err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	out := map[string]any{"UpgradeStep": upgradeStepUpgrade, "StepStatus": upgradeStatusSucceeded}
	if ok {
		out["UpgradeName"] = rec.Name
		out["UpgradeStep"] = rec.Steps[len(rec.Steps)-1]
	}

	h.writeJSON(r, w, out)
}

func (h *Handler) handleUpgradeElasticsearchDomain(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DomainName       string `json:"DomainName"`
		TargetVersion    string `json:"TargetVersion"`
		PerformCheckOnly bool   `json:"PerformCheckOnly"`
	}
	if !h.decodeRequest(w, r, &req) {
		return
	}

	ctx := h.reqContext(r)

	var err error
	if req.PerformCheckOnly {
		err = h.Backend.CheckElasticsearchDomainUpgrade(ctx, req.DomainName, req.TargetVersion)
	} else {
		_, err = h.Backend.UpgradeElasticsearchDomain(ctx, req.DomainName, req.TargetVersion)
	}

	if err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	h.writeJSON(r, w, req)
}

func (h *Handler) handleDescribeElasticsearchInstanceTypeLimits(w http.ResponseWriter, r *http.Request) {
	h.writeJSON(r, w, map[string]any{"LimitsByRole": map[string]any{
		"data": map[string]any{"InstanceLimits": map[string]any{"InstanceCountLimits": map[string]any{
			"MinimumInstanceCount": minimumInstanceCount,
			"MaximumInstanceCount": maximumInstanceCount,
		}}},
	}})
}

func (h *Handler) handleListElasticsearchInstanceTypes(w http.ResponseWriter, r *http.Request) {
	h.writeJSON(r, w, map[string]any{"ElasticsearchInstanceTypes": []string{
		defaultInstanceType,
		largeInstanceType,
	}})
}
