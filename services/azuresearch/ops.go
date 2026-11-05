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
		case "search.run":
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
	if len(segs) == 1 && !segs[0].has {
		if method == http.MethodGet {
			return opListIndexes
		}

		return opCreateIndex
	}

	if len(segs) == 1 || (len(segs) == 2 && !segs[0].has && !segs[1].has && segs[1].name == "") {
		return indexItemOp(method)
	}

	last := segs[len(segs)-1].name

	switch last {
	case "search.stats":
		return opIndexStatistics
	case "search.analyze":
		return opAnalyzeText
	case "search.index":
		return opIndexDocuments
	case "$count":
		return opCountDocuments
	case "search.suggest", "search.post.suggest":
		return opSuggest
	case "search.autocomplete", "search.post.autocomplete":
		return opAutocomplete
	case "docs", "search.post.search":
		return opSearch
	}

	if segs[len(segs)-1].has {
		return opGetDocument
	}

	return indexItemOp(method)
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
