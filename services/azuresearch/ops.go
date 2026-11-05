package azuresearch

import "net/http"

const (
	opCreateIndex       = "CreateIndex"
	opGetIndex          = "GetIndex"
	opListIndexes       = "ListIndexes"
	opDeleteIndex       = "DeleteIndex"
	opIndexStatistics   = "GetIndexStatistics"
	opAnalyzeText       = "AnalyzeText"
	opSearch            = "SearchDocuments"
	opIndexDocuments    = "IndexDocuments"
	opGetDocument       = "GetDocument"
	opCountDocuments    = "CountDocuments"
	opSuggest           = "SuggestDocuments"
	opAutocomplete      = "AutocompleteDocuments"
	opSynonymMap        = "SynonymMap"
	opDataSource        = "DataSource"
	opIndexer           = "Indexer"
	opRunIndexer        = "RunIndexer"
	opResetIndexer      = "ResetIndexer"
	opIndexerStatus     = "GetIndexerStatus"
	opSkillset          = "Skillset"
	opServiceStatistics = "GetServiceStatistics"
	opUnknown           = "Unknown"
)

func operationOf(method string, segs []seg) string {
	if len(segs) == 0 {
		return opUnknown
	}

	switch segs[0].name {
	case "servicestats":
		return opServiceStatistics
	case kindSynonymMaps:
		return opSynonymMap
	case kindDataSources:
		return opDataSource
	case kindSkillsets:
		return opSkillset
	case kindIndexers:
		return indexerOp(segs)
	case "indexes":
		return indexesOp(method, segs)
	}

	return opUnknown
}

func indexerOp(segs []seg) string {
	if len(segs) > 1 {
		switch segs[1].name {
		case pathRun:
			return opRunIndexer
		case "search.reset":
			return opResetIndexer
		case "search.status":
			return opIndexerStatus
		}
	}

	return opIndexer
}

func indexesOp(method string, segs []seg) string {
	if len(segs) == 1 {
		if !segs[0].has {
			if method == http.MethodGet {
				return opListIndexes
			}

			return opCreateIndex
		}

		return indexItemOp(method)
	}

	if op, ok := indexSubOp(segs[len(segs)-1].name); ok {
		return op
	}

	if segs[len(segs)-1].has {
		return opGetDocument
	}

	return indexItemOp(method)
}

func indexSubOp(name string) (string, bool) {
	switch name {
	case "search.stats":
		return opIndexStatistics, true
	case "search.analyze":
		return opAnalyzeText, true
	case "search.index":
		return opIndexDocuments, true
	case "$count":
		return opCountDocuments, true
	case "search.suggest", pathPostSuggest:
		return opSuggest, true
	case "search.autocomplete", pathPostAutocomplet:
		return opAutocomplete, true
	case "docs", "search.post.search":
		return opSearch, true
	}

	return "", false
}

func indexItemOp(method string) string {
	switch method {
	case http.MethodDelete:
		return opDeleteIndex
	case http.MethodGet:
		return opGetIndex
	}

	return opCreateIndex
}
