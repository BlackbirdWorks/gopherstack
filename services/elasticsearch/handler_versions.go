package elasticsearch

import "net/http"

// compatibleVersionEntry is the JSON representation of a compatible version pair.
type compatibleVersionEntry struct {
	SourceVersion  string   `json:"SourceVersion"`
	TargetVersions []string `json:"TargetVersions"`
}

// compatibleVersionsFor returns the valid upgrade targets for a given Elasticsearch version.
func compatibleVersionsFor(version string) []string {
	switch version {
	case elasticsearchVersion51, elasticsearchVersion53, elasticsearchVersion55:
		return []string{elasticsearchVersion56}
	case elasticsearchVersion56:
		return []string{elasticsearchVersion68}
	case elasticsearchVersion60, elasticsearchVersion62, elasticsearchVersion63,
		elasticsearchVersion64, elasticsearchVersion65, elasticsearchVersion67:
		return []string{elasticsearchVersion68}
	case elasticsearchVersion68:
		return []string{elasticsearchVersion71, defaultElasticsearchVersion}
	case elasticsearchVersion71, elasticsearchVersion74,
		elasticsearchVersion77, elasticsearchVersion78, elasticsearchVersion79:
		return []string{defaultElasticsearchVersion}
	case elasticsearchVersion713:
		return []string{elasticsearchVersion716, elasticsearchVersion717}
	case elasticsearchVersion716:
		return []string{elasticsearchVersion717}
	default:
		return []string{}
	}
}

func (h *Handler) handleGetCompatibleElasticsearchVersions(w http.ResponseWriter, r *http.Request) {
	domainName := r.URL.Query().Get("domainName")

	if domainName != "" {
		d, err := h.Backend.DescribeDomain(h.reqContext(r), domainName)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return
		}

		targets := compatibleVersionsFor(d.ElasticsearchVersion)
		h.writeJSON(r, w, map[string]any{
			"CompatibleElasticsearchVersions": []compatibleVersionEntry{
				{SourceVersion: d.ElasticsearchVersion, TargetVersions: targets},
			},
		})

		return
	}

	h.writeJSON(r, w, map[string]any{"CompatibleElasticsearchVersions": []compatibleVersionEntry{
		{
			SourceVersion:  elasticsearchVersion68,
			TargetVersions: []string{elasticsearchVersion71, defaultElasticsearchVersion},
		},
		{SourceVersion: elasticsearchVersion71, TargetVersions: []string{defaultElasticsearchVersion}},
	}})
}

func (h *Handler) handleListElasticsearchVersions(w http.ResponseWriter, r *http.Request) {
	versions := []string{
		elasticsearchVersion717, elasticsearchVersion716, elasticsearchVersion713,
		defaultElasticsearchVersion, elasticsearchVersion79, elasticsearchVersion78,
		elasticsearchVersion77, elasticsearchVersion74, elasticsearchVersion71,
		elasticsearchVersion68, elasticsearchVersion67, elasticsearchVersion65,
		elasticsearchVersion64, elasticsearchVersion63, elasticsearchVersion62,
		elasticsearchVersion60, elasticsearchVersion56, elasticsearchVersion55,
		elasticsearchVersion53, elasticsearchVersion51, "2.3", "1.5",
	}
	h.writeJSON(r, w, map[string]any{"ElasticsearchVersions": versions})
}
