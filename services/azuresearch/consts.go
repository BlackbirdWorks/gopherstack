package azuresearch

const (
	fieldValue          = "value"
	fieldCount          = "count"
	fieldKey            = "key"
	fieldErrorMessage   = "errorMessage"
	fieldStatus         = "status"
	fieldQuota          = "quota"
	fieldContext        = "@odata.context"
	fieldMappingsKey    = "fieldMappings"
	fieldOutputMappings = "outputFieldMappings"
	fieldUsage          = "usage"

	actionUpload = "upload"
	opAnd        = "and"
	opOr         = "or"

	pathRun             = "search.run"
	pathPostSuggest     = "search.post.suggest"
	pathPostAutocomplet = "search.post.autocomplete"

	litNaN = "NaN"
	litINF = "INF"

	coordPair        = 2
	maxFuzz          = 2
	half             = 0.5
	cosineOffset     = 2.0
	degToRad         = 3.141592653589793 / 180
	monthsPerQuarter = 3
	daysPerWeek      = 7
	base64Group      = 4
	searchInArgsMax  = 3

	limitFree         = 3
	limitBasic        = 15
	limitStandard     = 50
	limitLarge        = 200
	limitStorageOpt   = 10
	etagBase          = 0x8DC00000000000
	maxComplexFields  = 40
	maxComplexObjects = 3000
	maxExtractionSize = 134217728
	maxExtractChars   = 4000000
)
