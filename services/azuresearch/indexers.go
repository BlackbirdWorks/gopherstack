package azuresearch

import (
	"encoding/base64"
	"encoding/csv"
	"encoding/json/v2"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/iso8601"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	maxHistory     = 50
	runSuccess     = "success"
	runTransient   = "transientFailure"
	statusRunning  = "running"
	statusUnknown  = "unknown"
	statusError    = "error"
	defaultAccount = "devstoreaccount1"
)

// RunResult is one indexer execution.
type RunResult struct {
	StartTime      time.Time        `json:"startTime"`
	EndTime        time.Time        `json:"endTime"`
	Status         string           `json:"status"`
	ErrorMessage   string           `json:"errorMessage,omitempty"`
	Errors         []map[string]any `json:"errors"`
	Warnings       []map[string]any `json:"warnings"`
	ItemsProcessed int              `json:"itemsProcessed"`
	ItemsFailed    int              `json:"itemsFailed"`
}

// IndexerRuntime is the mutable state of an indexer.
type IndexerRuntime struct {
	Seen    map[string]string `json:"seen"`
	NextDue time.Time         `json:"nextDue"`
	Last    *RunResult        `json:"last,omitempty"`
	History []*RunResult      `json:"history"`
}

func (s *Service) runtimeOf(name string) *IndexerRuntime {
	rt := s.Runtime[name]
	if rt == nil {
		rt = &IndexerRuntime{Seen: map[string]string{}}
		s.Runtime[name] = rt
	}

	return rt
}

var supportedMappingFuncs = map[string]bool{ //nolint:gochecknoglobals // read-only
	"base64Encode": true, "base64Decode": true, "urlEncode": true, "urlDecode": true,
	"extractTokenAtPosition": true, "jsonArrayToStringCollection": true,
}

func (s *Service) validateIndexer(o Object) error {
	if _, ok := s.Objects[kindDataSources][strOf(o, "dataSourceName")]; !ok {
		return badf(
			"The request is invalid. Details: dataSourceName: The data source '%s' was not found.",
			strOf(o, "dataSourceName"),
		)
	}

	if _, ok := s.Indexes[strOf(o, "targetIndexName")]; !ok {
		return badf(
			"The request is invalid. Details: targetIndexName: The index '%s' was not found.",
			strOf(o, "targetIndexName"),
		)
	}

	if sk := strOf(o, "skillsetName"); sk != "" {
		if _, ok := s.Objects[kindSkillsets][sk]; !ok {
			return badf("The request is invalid. Details: skillsetName: The skillset '%s' was not found.", sk)
		}
	}

	if sch := objOf(o["schedule"]); sch != nil {
		d, err := iso8601.Parse(strOf(sch, "interval"))
		if err != nil || d < 5*time.Minute || d > 24*time.Hour {
			return badf(
				"The request is invalid. Details: schedule: interval must be an ISO 8601 duration between PT5M and P1D.",
			)
		}
	}

	for _, key := range []string{fieldMappingsKey, fieldOutputMappings} {
		for _, raw := range listOf(o, key) {
			fn := objOf(objOf(raw)["mappingFunction"])
			if fn != nil && !supportedMappingFuncs[strOf(fn, "name")] {
				return badf(
					"The request is invalid. Details: mapping function '%s' is not supported.",
					strOf(fn, "name"),
				)
			}
		}
	}

	for _, k := range []string{
		"disabled", "parameters", fieldMappingsKey, fieldOutputMappings, "skillsetName", "description",
	} {
		if _, ok := o[k]; !ok {
			o[k] = map[string]any{"disabled": false, fieldMappingsKey: []any{}, "outputFieldMappings": []any{}}[k]
		}
	}

	return nil
}

// sourceDoc is one document read from a data source.
type sourceDoc struct {
	fields  map[string]any
	id      string
	version string
	deleted bool
}

func (b *InMemoryBackend) loadSource(ds Object, params map[string]any) ([]sourceDoc, error) {
	conn := strOf(objOf(ds["credentials"]), "connectionString")
	container := strOf(objOf(ds["container"]), "name")

	switch strOf(ds, "type") {
	case "azureblob":
		return b.loadBlobs(conn, container, strOf(objOf(ds["container"]), "query"), params, ds)
	case "cosmosdb":
		return b.loadCosmos(conn, container, ds)
	case "azuretable":
		return b.loadTable(container, strOf(objOf(ds["container"]), "query"), ds)
	}

	return nil, wrapf(ErrBadRequest, "unsupported data source type")
}

func deletionMarker(ds Object) (string, string) {
	pol := objOf(ds["dataDeletionDetectionPolicy"])
	if strOf(pol, "@odata.type") == "#Microsoft.Azure.Search.SoftDeleteColumnDeletionDetectionPolicy" {
		return strOf(pol, "softDeleteColumnName"), fmt.Sprint(pol["softDeleteMarkerValue"])
	}

	return "", ""
}

func (b *InMemoryBackend) loadCosmos(conn, container string, ds Object) ([]sourceDoc, error) {
	db := connValue(conn, "Database")

	docs, err := b.cosmos.ListDocuments(db, container)
	if err != nil {
		return nil, err
	}

	col, marker := deletionMarker(ds)
	out := make([]sourceDoc, 0, len(docs))

	for _, d := range docs {
		f := cloneMap(d.Body)
		f["_ts"] = float64(d.Timestamp.Unix())

		sd := sourceDoc{fields: f, id: d.ID, version: d.ETag}
		sd.deleted = col != "" && fmt.Sprint(f[col]) == marker

		out = append(out, sd)
	}

	return out, nil
}

// loadTable keys each document PartitionKey+RowKey and exposes it as Key (Learn: search-how-to-index-azure-tables).
func (b *InMemoryBackend) loadTable(table, query string, ds Object) ([]sourceDoc, error) {
	ents, err := b.tables.ListEntities(table, query)
	if err != nil {
		return nil, err
	}

	col, marker := deletionMarker(ds)
	out := make([]sourceDoc, 0, len(ents))

	for _, e := range ents {
		f := cloneMap(e.Properties)
		key := e.PartitionKey + e.RowKey
		f["PartitionKey"], f["RowKey"], f["Key"] = e.PartitionKey, e.RowKey, key
		f["Timestamp"] = e.Timestamp.UTC().Format(time.RFC3339)

		sd := sourceDoc{fields: f, id: key, version: e.ETag}
		sd.deleted = col != "" && f[col] != nil && fmt.Sprint(f[col]) == marker

		out = append(out, sd)
	}

	return out, nil
}

func (b *InMemoryBackend) loadBlobs(
	conn, container, prefix string,
	params map[string]any,
	ds Object,
) ([]sourceDoc, error) {
	blobs, err := b.blobs.ListBlobs(container)
	if err != nil {
		return nil, err
	}

	account := connValue(conn, "AccountName")
	if account == "" {
		account = defaultAccount
	}

	cfg := objOf(params["configuration"])
	col, marker := deletionMarker(ds)

	var out []sourceDoc

	for _, be := range blobs {
		if prefix != "" && !strings.HasPrefix(be.Name, strings.TrimPrefix(prefix, "/")) {
			continue
		}

		if !extensionAllowed(be.Name, cfg) {
			continue
		}

		data, rerr := b.blobs.ReadBlob(container, be.Name)
		if rerr != nil {
			continue
		}

		for i, f := range crackBlob(be, data, account, container, cfg) {
			id := be.Name
			if i > 0 {
				id = fmt.Sprintf("%s#%d", be.Name, i)
			}

			sd := sourceDoc{fields: f, id: id, version: be.ETag}
			sd.deleted = col != "" && f[col] != nil && fmt.Sprint(f[col]) == marker
			out = append(out, sd)
		}
	}

	return out, nil
}

func extensionAllowed(name string, cfg map[string]any) bool {
	ext := strings.ToLower(path.Ext(name))

	for _, e := range splitCSV(strOf(cfg, "excludedFileNameExtensions")) {
		if strings.EqualFold(e, ext) {
			return false
		}
	}

	if inc := splitCSV(strOf(cfg, "indexedFileNameExtensions")); len(inc) > 0 {
		for _, e := range inc {
			if strings.EqualFold(e, ext) {
				return true
			}
		}

		return false
	}

	return true
}

func blobMetadata(be BlobEntry, account, container string) map[string]any {
	m := map[string]any{
		"metadata_storage_name": be.Name,
		"metadata_storage_path": "https://" + account + ".blob.core.windows.net/" + container + "/" + url.PathEscape(
			be.Name,
		),
		"metadata_storage_size":          float64(be.Size),
		"metadata_storage_content_type":  be.ContentType,
		"metadata_storage_last_modified": be.LastModified.UTC().Format(time.RFC3339),
	}

	if be.MD5 != "" {
		m["metadata_storage_content_md5"] = be.MD5
	}

	for k, v := range be.Metadata {
		m[k] = v
	}

	return m
}

// crackBlob turns blob bytes into source documents according to the parsing mode.
func crackBlob(be BlobEntry, data []byte, account, container string, cfg map[string]any) []map[string]any {
	meta := blobMetadata(be, account, container)

	withMeta := func(f map[string]any) map[string]any {
		for k, v := range meta {
			if _, exists := f[k]; !exists {
				f[k] = v
			}
		}

		return f
	}

	switch strOf(cfg, "parsingMode") {
	case "json":
		return jsonDocs(data, withMeta, false)
	case "jsonArray":
		return jsonDocs(data, withMeta, true)
	case "jsonLines":
		return jsonLineDocs(data, withMeta)
	case "delimitedText":
		return csvDocs(data, cfg, withMeta)
	}

	if strOf(cfg, "dataToExtract") == "storageMetadata" || strOf(cfg, "dataToExtract") == "allMetadata" {
		return []map[string]any{withMeta(map[string]any{})}
	}

	return []map[string]any{withMeta(map[string]any{"content": string(data)})}
}

func jsonDocs(data []byte, withMeta func(map[string]any) map[string]any, array bool) []map[string]any {
	if array {
		var list []map[string]any
		if json.Unmarshal(data, &list, jsonx.V1()) != nil {
			return nil
		}

		out := make([]map[string]any, 0, len(list))
		for _, m := range list {
			out = append(out, withMeta(m))
		}

		return out
	}

	var m map[string]any
	if json.Unmarshal(data, &m, jsonx.V1()) != nil {
		return nil
	}

	return []map[string]any{withMeta(m)}
}

func jsonLineDocs(data []byte, withMeta func(map[string]any) map[string]any) []map[string]any {
	var out []map[string]any

	for line := range strings.SplitSeq(string(data), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}

		var m map[string]any
		if json.Unmarshal([]byte(line), &m, jsonx.V1()) == nil {
			out = append(out, withMeta(m))
		}
	}

	return out
}

func csvDocs(data []byte, cfg map[string]any, withMeta func(map[string]any) map[string]any) []map[string]any {
	delim := ','
	if d := strOf(cfg, "delimitedTextDelimiter"); d != "" {
		delim = []rune(d)[0]
	}

	r := csv.NewReader(strings.NewReader(string(data)))
	r.Comma = delim
	r.FieldsPerRecord = -1

	rows, err := r.ReadAll()
	if err != nil || len(rows) == 0 {
		return nil
	}

	headers := splitCSV(strOf(cfg, "delimitedTextHeaders"))
	if first, set := cfg["firstLineContainsHeaders"].(bool); (!set || first) && len(headers) == 0 {
		headers, rows = rows[0], rows[1:]
	}

	out := make([]map[string]any, 0, len(rows))

	for _, row := range rows {
		m := map[string]any{}

		for i, v := range row {
			if i < len(headers) {
				m[headers[i]] = v
			}
		}

		out = append(out, withMeta(m))
	}

	return out
}

// RunIndexer executes the named indexer once, synchronously, and records the result.
func (b *InMemoryBackend) RunIndexer(service, name string) error {
	b.mu.Lock("RunIndexer")
	defer b.mu.Unlock()

	s, err := b.svc(service)
	if err != nil {
		return err
	}

	if _, err = s.object(kindIndexers, name); err != nil {
		return err
	}

	b.runLocked(s, name)

	return nil
}

func (b *InMemoryBackend) runLocked(s *Service, name string) {
	idxr := s.Objects[kindIndexers][name]
	rt := s.runtimeOf(name)
	res := &RunResult{StartTime: b.now(), Errors: []map[string]any{}, Warnings: []map[string]any{}, Status: runSuccess}

	b.execute(s, idxr, rt, res)

	res.EndTime = b.now()
	rt.Last = res
	rt.History = append([]*RunResult{res}, rt.History...)

	if len(rt.History) > maxHistory {
		rt.History = rt.History[:maxHistory]
	}

	rt.NextDue = b.nextDue(idxr, res.StartTime)
}

func (b *InMemoryBackend) nextDue(idxr Object, from time.Time) time.Time {
	sch := objOf(idxr["schedule"])
	if sch == nil {
		return time.Time{}
	}

	d, err := iso8601.Parse(strOf(sch, "interval"))
	if err != nil {
		return time.Time{}
	}

	return from.Add(d)
}

func (b *InMemoryBackend) execute(s *Service, idxr Object, rt *IndexerRuntime, res *RunResult) {
	ds := s.Objects[kindDataSources][strOf(idxr, "dataSourceName")]
	params := objOf(idxr["parameters"])

	docs, err := b.loadSource(ds, params)
	if err != nil {
		res.Status, res.ErrorMessage = runTransient, "Could not read the data source: "+err.Error()

		return
	}

	ix := s.Indexes[strOf(idxr, "targetIndexName")]
	if ix == nil {
		res.Status, res.ErrorMessage = runTransient, "The target index does not exist."

		return
	}

	tolerated := 0
	if f, ok := params["maxFailedItems"].(float64); ok {
		tolerated = int(f)
	}

	for _, sd := range docs {
		if rt.Seen[sd.id] == sd.version && !sd.deleted && sd.version != "" {
			continue
		}

		if err = b.indexOne(s, idxr, ix, sd, rt, res); err != nil {
			res.ItemsFailed++
			res.Errors = append(
				res.Errors,
				map[string]any{
					fieldKey:            sd.id,
					fieldErrorMessage:   err.Error(),
					"statusCode":        http.StatusBadRequest,
					"name":              "Enrichment.Index",
					"details":           nil,
					"documentationLink": nil,
				},
			)

			if tolerated >= 0 && res.ItemsFailed > tolerated {
				res.Status = runTransient
				res.ErrorMessage = fmt.Sprintf(
					"The indexer failed because more than the maximum number of failed items (%d) were found.",
					tolerated,
				)

				return
			}

			continue
		}

		res.ItemsProcessed++
	}
}

func (b *InMemoryBackend) indexOne(
	s *Service,
	idxr Object,
	ix *Index,
	sd sourceDoc,
	rt *IndexerRuntime,
	res *RunResult,
) error {
	tree := enrichTree{"document": sd.fields}

	if sk := strOf(idxr, "skillsetName"); sk != "" {
		warn := func(skill, msg string) {
			res.Warnings = append(
				res.Warnings,
				map[string]any{
					fieldKey:            sd.id,
					"name":              skill,
					"message":           msg,
					"details":           nil,
					"documentationLink": nil,
				},
			)
		}

		if err := runSkillset(s.Objects[kindSkillsets][sk], tree, warn); err != nil {
			return err
		}
	}

	target, err := mapFields(idxr, ix, sd.fields, tree)
	if err != nil {
		return err
	}

	keyName := ix.comp.key.Name
	if k, _ := target[keyName].(string); k == "" {
		return badf("Document key cannot be missing or empty: map a source field to the key field '%s'.", keyName)
	}

	if sd.deleted {
		r := ix.applyAction(indexAction{action: "delete", fields: map[string]any{keyName: target[keyName]}}, "")
		delete(rt.Seen, sd.id)

		return resultErr(r)
	}

	r := ix.applyAction(indexAction{action: "mergeOrUpload", fields: target}, "")
	if !r.OK {
		return resultErr(r)
	}

	rt.Seen[sd.id] = sd.version

	return nil
}

func resultErr(r actionResult) error {
	if r.OK {
		return nil
	}

	return wrapf(ErrBadRequest, "%s", r.Err)
}

// mapFields builds the target document from field mappings, implicit same-name mappings and output mappings.
func mapFields(idxr Object, ix *Index, src map[string]any, tree enrichTree) (map[string]any, error) {
	target := map[string]any{}
	explicit := map[string]bool{}

	for _, raw := range listOf(idxr, fieldMappingsKey) {
		m := objOf(raw)
		tn := strOf(m, "targetFieldName")

		if tn == "" {
			tn = strOf(m, "sourceFieldName")
		}

		explicit[tn] = true

		v, err := applyMappingFunc(objOf(m["mappingFunction"]), src[strOf(m, "sourceFieldName")])
		if err != nil {
			return nil, err
		}

		if v != nil {
			target[tn] = v
		}
	}

	for _, f := range ix.comp.fields {
		if _, done := explicit[f.Name]; !done {
			if v, ok := src[f.Name]; ok {
				target[f.Name] = v
			}
		}
	}

	for _, raw := range listOf(idxr, fieldOutputMappings) {
		m := objOf(raw)
		v, ok := tree.get(strOf(m, "sourceFieldName"))

		if !ok {
			continue
		}

		v, err := applyMappingFunc(objOf(m["mappingFunction"]), v)
		if err != nil {
			return nil, err
		}

		target[strOf(m, "targetFieldName")] = v
	}

	return coerceTarget(ix, target), nil
}

// coerceTarget converts source JSON numbers to the types the index schema expects where unambiguous.
func coerceTarget(ix *Index, t map[string]any) map[string]any {
	for _, f := range ix.comp.fields {
		v, ok := t[f.Name]
		if !ok || f.Collection {
			continue
		}

		switch f.Base {
		case typeString:
			if n, isNum := v.(float64); isNum {
				t[f.Name] = fmtNum(n)
			}
		case typeInt32, typeInt64:
			if n, isNum := v.(float64); isNum {
				t[f.Name] = jsonx.Number(fmtNum(n))
			}
		}
	}

	return t
}

func applyMappingFunc(fn map[string]any, v any) (any, error) {
	if fn == nil || v == nil {
		return v, nil
	}

	params := objOf(fn["parameters"])
	s, _ := v.(string)

	switch strOf(fn, "name") {
	case "base64Encode":
		return base64Encode(s, boolOf(params, "useHttpServerUtilityUrlTokenEncode", true)), nil
	case "base64Decode":
		return base64Decode(s, boolOf(params, "useHttpServerUtilityUrlTokenDecode", true))
	case "urlEncode":
		return url.QueryEscape(s), nil
	case "urlDecode":
		return url.QueryUnescape(s)
	case "extractTokenAtPosition":
		parts := strings.Split(s, strOf(params, "delimiter"))
		pos, _ := params["position"].(float64)

		if int(pos) < 0 || int(pos) >= len(parts) {
			return nil, nil //nolint:nilnil // no token at that position maps to null
		}

		return parts[int(pos)], nil
	case "jsonArrayToStringCollection":
		var out []any
		if err := json.Unmarshal([]byte(s), &out, jsonx.V1()); err != nil {
			return nil, badf("jsonArrayToStringCollection: the value is not a JSON array of strings.")
		}

		return out, nil
	}

	return v, nil
}

func base64Encode(s string, urlToken bool) string {
	if !urlToken {
		return base64.URLEncoding.EncodeToString([]byte(s))
	}

	enc := base64.RawURLEncoding.EncodeToString([]byte(s))
	pad := (base64Group - len(enc)%base64Group) % base64Group

	return fmt.Sprintf("%s%d", enc, pad)
}

func base64Decode(s string, urlToken bool) (any, error) {
	if urlToken && s != "" {
		pad := int(s[len(s)-1] - '0')
		if pad < 0 || pad > 2 {
			return nil, badf("base64Decode: invalid URL token encoding.")
		}

		s = s[:len(s)-1] + strings.Repeat("=", pad)
	}

	b, err := base64.URLEncoding.DecodeString(s)
	if err != nil {
		return nil, badf("base64Decode: the value is not valid base64.")
	}

	return string(b), nil
}

// ResetIndexer clears the indexer's change-tracking state so the next run reprocesses everything.
func (b *InMemoryBackend) ResetIndexer(service, name string) error {
	b.mu.Lock("ResetIndexer")
	defer b.mu.Unlock()

	s, err := b.svc(service)
	if err != nil {
		return err
	}

	if _, err = s.object(kindIndexers, name); err != nil {
		return err
	}

	rt := s.runtimeOf(name)
	rt.Seen = map[string]string{}
	rt.Last = &RunResult{
		StartTime: b.now(),
		EndTime:   b.now(),
		Status:    "reset",
		Errors:    []map[string]any{},
		Warnings:  []map[string]any{},
	}

	return nil
}

// RunDue runs every enabled, scheduled indexer whose next run time has passed; it returns the runs made.
func (b *InMemoryBackend) RunDue() int {
	b.mu.Lock("RunDue")
	defer b.mu.Unlock()

	now := b.now()
	runs := 0

	for _, s := range b.services {
		names := make([]string, 0, len(s.Objects[kindIndexers]))
		for n := range s.Objects[kindIndexers] {
			names = append(names, n)
		}

		sort.Strings(names)

		for _, n := range names {
			idxr := s.Objects[kindIndexers][n]
			if boolOf(idxr, "disabled", false) || objOf(idxr["schedule"]) == nil {
				continue
			}

			rt := s.runtimeOf(n)
			if rt.NextDue.IsZero() {
				rt.NextDue = scheduleStart(idxr, now)
			}

			if !now.Before(rt.NextDue) {
				b.runLocked(s, n)

				runs++
			}
		}
	}

	return runs
}

func scheduleStart(idxr Object, now time.Time) time.Time {
	if t, ok := parseDate(strOf(objOf(idxr["schedule"]), "startTime")); ok && t.After(now) {
		return t
	}

	return now
}

func (s *Service) indexerStatus(name string) map[string]any {
	rt := s.runtimeOf(name)
	status := statusUnknown

	if rt.Last != nil {
		status = statusRunning
		if rt.Last.Status == "persistentFailure" {
			status = statusError
		}
	}

	hist := make([]any, 0, len(rt.History))
	for _, h := range rt.History {
		hist = append(hist, runJSON(h))
	}

	out := map[string]any{
		fieldStatus: status, "executionHistory": hist,
		"limits": map[string]any{
			"maxRunTime":                            "P1D",
			"maxDocumentExtractionSize":             maxExtractionSize,
			"maxDocumentContentCharactersToExtract": maxExtractChars,
		},
	}

	if rt.Last != nil {
		out["lastResult"] = runJSON(rt.Last)
	} else {
		out["lastResult"] = nil
	}

	return out
}

func runJSON(r *RunResult) map[string]any {
	var errMsg any
	if r.ErrorMessage != "" {
		errMsg = r.ErrorMessage
	}

	return map[string]any{
		fieldStatus: r.Status, fieldErrorMessage: errMsg, "startTime": r.StartTime.Format(time.RFC3339),
		"endTime": r.EndTime.Format(time.RFC3339), "itemsProcessed": r.ItemsProcessed, "itemsFailed": r.ItemsFailed,
		"initialTrackingState": nil, "finalTrackingState": nil, "errors": r.Errors, "warnings": r.Warnings,
		"metrics": nil,
	}
}
