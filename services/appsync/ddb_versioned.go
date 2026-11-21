package appsync

import (
	"context"
	"fmt"
	"maps"
	"math"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Metadata and delta-table attribute names of versioned data sources (AppSync developer guide,
// "Versioning DynamoDB data sources").
const (
	attrVersion     = "_version"
	attrLastChanged = "_lastChangedAt"
	attrDeleted     = "_deleted"
	attrTTL         = "_ttl"
	attrDeltaPK     = "ds_pk"
	attrDeltaSK     = "ds_sk"
	attrIndexPK     = "gsi_ds_pk"
	attrIndexSK     = "gsi_ds_sk"

	errCodeBadRequest        = "BadRequest"
	errCodeConflictUnhandled = "ConflictUnhandled"
	errCodeMaxConflicts      = "MaxConflicts"
	errCodeDeltaSyncWrite    = "DeltaSyncWriteError"

	versionedMaxAttempts = 5
	secondsPerMinute     = 60
	millisPerMinute      = 60_000
	deltaDayLayout       = "2006-01-02"
	deltaTimeLayout      = "15:04:05"
	deltaPartitionSpan   = 24 * time.Hour
	maxDeltaTTLMinutes   = 366 * 24 * 60

	nameVersion = "#__sv"
	nameChanged = "#__sl"
	nameDeleted = "#__sd"
	nameTTL     = "#__st"
	valVersion  = ":__sv"
	valNew      = ":__sn"
	valChanged  = ":__sl"
	valTrue     = ":__sd"
	valTTL      = ":__st"
)

var setClause = regexp.MustCompile(`(?i)\bSET\b`)

func int32Of(n int) int32 {
	if n > math.MaxInt32 {
		return math.MaxInt32
	}

	if n < math.MinInt32 {
		return math.MinInt32
	}

	return int32(n)
}

type versionedSource struct {
	ddb      DynamoDBBackend
	cfg      *DynamoDBDataSourceConfig
	dsName   string
	conflict conflictEnv
}

func (vs versionedSource) delta() (string, bool) {
	if vs.cfg.DeltaSyncConfig == nil || vs.cfg.DeltaSyncConfig.DeltaSyncTableName == "" {
		return "", false
	}

	return vs.cfg.DeltaSyncConfig.DeltaSyncTableName, true
}

// deltaTTLMinutes is the delta table retention, bounded so a huge configured value cannot overflow the time
// arithmetic or make a sync walk an unbounded number of day partitions.
func (vs versionedSource) deltaTTLMinutes() int64 {
	return min(max(vs.cfg.DeltaSyncConfig.DeltaSyncTableTTL, 0), maxDeltaTTLMinutes)
}

func numAttr(n int64) map[string]any { return map[string]any{"N": strconv.FormatInt(n, 10)} }

func strAttr(s string) map[string]any { return map[string]any{"S": s} }

func versionOf(item map[string]any) (int64, bool) {
	raw, ok := item[attrVersion].(map[string]any)
	if !ok {
		return 0, false
	}

	s, _ := raw["N"].(string)
	n, err := strconv.ParseInt(s, 10, 64)

	return n, err == nil
}

func expectedVersion(req map[string]any) (int64, bool) {
	if req[attrVersion] == nil {
		return 0, false
	}

	return int64(intOf(req[attrVersion])), true
}

func conflictError(existing map[string]any) error {
	return &DynamoDBError{
		Code:    errCodeConflictUnhandled,
		Message: "The conflict handler rejected the mutation: the item has a different version",
		Result:  ddbPlainItem(existing),
	}
}

// deltaLayout is the delta table record layout a write asks for with customPartitionKey and populateIndexFields.
type deltaLayout struct {
	custom   string
	populate bool
}

func deltaLayoutOf(req map[string]any) (deltaLayout, error) {
	var l deltaLayout

	if v, ok := req["customPartitionKey"]; ok && v != nil {
		if l.custom, ok = v.(string); !ok || l.custom == "" {
			return l, fmt.Errorf("%w: customPartitionKey must be a non-empty string", ErrValidation)
		}
	}

	if v, ok := req["populateIndexFields"]; ok && v != nil {
		if l.populate, ok = v.(bool); !ok {
			return l, fmt.Errorf("%w: populateIndexFields must be a boolean", ErrValidation)
		}
	}

	l.populate = l.populate && l.custom != ""

	return l, nil
}

// versionGuard ANDs the optimistic-concurrency check on the version just read into the request's condition.
func versionGuard(user *DDBExpression, hasVersion bool, curVer int64) *DDBExpression {
	guard := DDBExpression{Expression: "attribute_not_exists(" + nameVersion + ")", Names: map[string]string{
		nameVersion: attrVersion,
	}}

	if hasVersion {
		guard.Expression = nameVersion + " = " + valVersion
		guard.Values = map[string]any{valVersion: numAttr(curVer)}
	}

	if user == nil {
		return &guard
	}

	out := DDBExpression{Expression: "(" + user.Expression + ") AND (" + guard.Expression + ")"}
	out.Names, out.Values = maps.Clone(user.Names), maps.Clone(user.Values)

	if out.Names == nil {
		out.Names = map[string]string{}
	}

	if out.Values == nil {
		out.Values = map[string]any{}
	}

	maps.Copy(out.Names, guard.Names)
	maps.Copy(out.Values, guard.Values)

	return &out
}

// KeySchemaReader is implemented by a DynamoDBBackend that can report a table's key attribute names, hash key first.
type KeySchemaReader interface {
	KeyAttributeNames(ctx context.Context, tableName string) ([]string, error)
}

func escapeKeyPart(v string) string {
	return strings.NewReplacer(`\`, `\\`, "#", `\#`).Replace(v)
}

// keyString joins the key attribute values with "#" (a "#" or "\" inside a value is backslash-escaped so distinct
// keys never collide), hash key before range key when the backend reports the table's key schema, otherwise in
// attribute-name order.
func (vs versionedSource) keyString(ctx context.Context, key map[string]any) string {
	var names []string

	if kr, ok := vs.ddb.(KeySchemaReader); ok {
		if schema, err := kr.KeyAttributeNames(ctx, vs.cfg.TableName); err == nil && len(schema) == len(key) {
			names = slices.DeleteFunc(slices.Clone(schema), func(n string) bool {
				_, has := key[n]

				return !has
			})
		}
	}

	if len(names) != len(key) {
		names = slices.Sorted(maps.Keys(key))
	}

	parts := make([]string, len(names))
	for i, n := range names {
		parts[i] = escapeKeyPart(fmt.Sprint(ddbPlain(key[n])))
	}

	return strings.Join(parts, "#")
}

// logDelta records one change in the delta table (ds_pk "<data source>:<date>", ds_sk "<time>:<key>:<version>").
func (vs versionedSource) logDelta(
	ctx context.Context, key, item map[string]any, at time.Time, layout deltaLayout,
) error {
	table, ok := vs.delta()
	if !ok {
		return nil
	}

	ver, _ := versionOf(item)
	rec := maps.Clone(item)
	day := at.Format(deltaDayLayout)
	sortKey := at.Format(deltaTimeLayout) + ":" + vs.keyString(ctx, key) + ":" + strconv.FormatInt(ver, 10)

	rec[attrDeltaPK] = strAttr(vs.dsName + ":" + day)
	rec[attrDeltaSK] = strAttr(sortKey)

	if layout.custom != "" {
		rec[attrDeltaPK] = strAttr(layout.custom + ":" + day)
		rec[attrDeltaSK] = strAttr(at.Format(deltaTimeLayout) + ":" + layout.custom + ":" +
			vs.keyString(ctx, key) + ":" + strconv.FormatInt(ver, 10))
	}

	if layout.populate {
		rec[attrIndexPK] = strAttr(vs.dsName + ":" + day)
		rec[attrIndexSK] = strAttr(sortKey)
	}
	rec[attrTTL] = numAttr(at.Add(time.Duration(vs.deltaTTLMinutes()) * time.Minute).Unix())

	if err := vs.ddb.PutItemRaw(ctx, table, rec, nil); err != nil {
		return &DynamoDBError{
			Code:    errCodeDeltaSyncWrite,
			Message: "failed to write the delta sync record: " + err.Error(),
		}
	}

	return nil
}

func stamp(item map[string]any, ver int64, at time.Time) {
	item[attrVersion] = numAttr(ver)
	item[attrLastChanged] = numAttr(at.UnixMilli())
}

func metadataWriteError(name string) error {
	return &DynamoDBError{Code: errCodeBadRequest, Message: "the sync metadata field " + name + " cannot be written"}
}

func versionedWrite(ctx context.Context, vs versionedSource, req map[string]any, op string) (any, error) {
	if _, err := deltaLayoutOf(req); err != nil {
		return nil, err
	}

	if op == ddbOpPutItem {
		for name := range ddbPutItem(req) {
			if isMetadataAttr(name) {
				return nil, metadataWriteError(name)
			}
		}
	}

	var (
		res any
		err error
	)

	for range versionedMaxAttempts {
		var retry bool

		switch op {
		case ddbOpPutItem:
			res, retry, err = versionedPutOnce(ctx, vs, req)
		case ddbOpUpdateItem:
			res, retry, err = versionedUpdateOnce(ctx, vs, req)
		default:
			res, retry, err = versionedDeleteOnce(ctx, vs, req)
		}

		if !retry {
			return res, err
		}
	}

	return nil, &DynamoDBError{Code: errCodeMaxConflicts, Message: "too many concurrent writes to the item"}
}

// changedSince reports whether the stored item still has the version/existence the write was based on.
func (vs versionedSource) changedSince(ctx context.Context, table string, key, base map[string]any) bool {
	now, err := vs.ddb.GetItemRaw(ctx, table, key)
	if err != nil {
		return false
	}

	nv, nok := versionOf(now)
	bv, bok := versionOf(base)

	return (len(now) == 0) != (len(base) == 0) || nv != bv || nok != bok
}

// read returns the stored item and its version; conflict is set when the request's _version does not match it.
func (vs versionedSource) read(ctx context.Context, req map[string]any) (map[string]any, int64, bool, bool, error) {
	cur, err := vs.ddb.GetItemRaw(ctx, vs.cfg.TableName, mapOf(req["key"]))
	if err != nil {
		return nil, 0, false, false, err
	}

	ver, has := versionOf(cur)
	want, ok := expectedVersion(req)
	conflict := ok && len(cur) > 0 && (!has || want != ver) && vs.conflict.detects()

	return cur, ver, has, conflict, nil
}

func versionedPutOnce(ctx context.Context, vs versionedSource, req map[string]any) (any, bool, error) {
	cur, ver, has, conflict, err := vs.read(ctx, req)
	if err != nil {
		return nil, false, err
	}

	item := maps.Clone(ddbPutItem(req))

	if conflict {
		if item, err = vs.resolveConflict(ctx, ddbOpPutItem, req, cur); err != nil {
			return nil, false, err
		}
	}

	return vs.putStamped(ctx, req, cur, ver, has, item)
}

// putStamped stores item as the next version, guarded on the version just read.
func (vs versionedSource) putStamped(
	ctx context.Context, req, cur map[string]any, ver int64, has bool, item map[string]any,
) (any, bool, error) {
	now := vs.conflict.clock()
	stamp(item, ver+1, now)

	err := vs.ddb.PutItemRaw(ctx, vs.cfg.TableName, item, versionGuard(condOf(req), has, ver))
	if err != nil {
		if isCondFailed(err) && vs.changedSince(ctx, vs.cfg.TableName, mapOf(req["key"]), cur) {
			return nil, true, nil
		}

		return nil, false, checkFailedErr(req, err)
	}

	if err = vs.logDelta(ctx, mapOf(req["key"]), item, now, layoutOrZero(req)); err != nil {
		return nil, false, err
	}

	return ddbPlainItem(item), false, nil
}

func addSet(expr, assignments string) string {
	if loc := setClause.FindStringIndex(expr); loc != nil {
		return expr[:loc[1]] + " " + assignments + "," + expr[loc[1]:]
	}

	return "SET " + assignments + " " + expr
}

func versionedUpdateOnce(ctx context.Context, vs versionedSource, req map[string]any) (any, bool, error) {
	update := exprOf(req["update"])
	if update.Expression == "" {
		return nil, false, fmt.Errorf("%w: UpdateItem requires update.expression", ErrValidation)
	}

	cur, ver, has, conflict, err := vs.read(ctx, req)
	if err != nil {
		return nil, false, err
	}

	if name, bad := vs.updatesMetadata(cur, update); bad {
		return nil, false, metadataWriteError(name)
	}

	if conflict {
		merged, rerr := vs.resolveConflict(ctx, ddbOpUpdateItem, req, cur)
		if rerr != nil {
			return nil, false, rerr
		}

		return vs.putStamped(ctx, req, cur, ver, has, merged)
	}

	now := vs.conflict.clock()
	update.Names, update.Values = maps.Clone(update.Names), maps.Clone(update.Values)

	if update.Names == nil {
		update.Names = map[string]string{}
	}

	if update.Values == nil {
		update.Values = map[string]any{}
	}

	update.Names[nameVersion], update.Names[nameChanged] = attrVersion, attrLastChanged
	update.Values[valNew], update.Values[valChanged] = numAttr(ver+1), numAttr(now.UnixMilli())
	update.Expression = addSet(update.Expression, nameVersion+" = "+valNew+", "+nameChanged+" = "+valChanged)

	item, err := vs.ddb.UpdateItemRaw(
		ctx, vs.cfg.TableName, mapOf(req["key"]), update, versionGuard(condOf(req), has, ver),
	)
	if err != nil {
		if isCondFailed(err) && vs.changedSince(ctx, vs.cfg.TableName, mapOf(req["key"]), cur) {
			return nil, true, nil
		}

		return nil, false, checkFailedErr(req, err)
	}

	if err = vs.logDelta(ctx, mapOf(req["key"]), item, now, layoutOrZero(req)); err != nil {
		return nil, false, err
	}

	return ddbPlainItem(item), false, nil
}

// updatesMetadata reports a sync metadata attribute the update expression would change on the stored item.
func (vs versionedSource) updatesMetadata(cur map[string]any, update DDBExpression) (string, bool) {
	after, err := vs.ddb.ApplyUpdateRaw(cur, update)
	if err != nil {
		return "", false
	}

	for _, name := range []string{attrVersion, attrLastChanged, attrDeleted, attrTTL} {
		if !reflect.DeepEqual(cur[name], after[name]) {
			return name, true
		}
	}

	return "", false
}

func versionedDeleteOnce(ctx context.Context, vs versionedSource, req map[string]any) (any, bool, error) {
	cur, ver, has, conflict, err := vs.read(ctx, req)
	if err != nil {
		return nil, false, err
	}

	if conflict {
		if _, err = vs.resolveConflict(ctx, ddbOpDeleteItem, req, cur); err != nil {
			return nil, false, err
		}
	}

	if len(cur) == 0 {
		res, derr := ddbDelete(ctx, vs.ddb, vs.cfg.TableName, req)

		return res, false, derr
	}

	key := mapOf(req["key"])
	guard := versionGuard(condOf(req), has, ver)
	now := vs.conflict.clock()

	var item map[string]any

	if ttl := vs.baseTTL(); ttl > 0 {
		item, err = vs.tombstone(ctx, key, guard, ver+1, now, ttl)
	} else {
		item, err = vs.ddb.DeleteItemRaw(ctx, vs.cfg.TableName, key, guard)
		if err == nil {
			item = maps.Clone(item)
			stamp(item, ver+1, now)
			item[attrDeleted] = map[string]any{ddbTagBool: true}
		}
	}

	if err != nil {
		if isCondFailed(err) && vs.changedSince(ctx, vs.cfg.TableName, key, cur) {
			return nil, true, nil
		}

		return nil, false, checkFailedErr(req, err)
	}

	if err = vs.logDelta(ctx, key, item, now, layoutOrZero(req)); err != nil {
		return nil, false, err
	}

	return ddbPlainItem(item), false, nil
}

func layoutOrZero(req map[string]any) deltaLayout {
	l, _ := deltaLayoutOf(req)

	return l
}

func (vs versionedSource) baseTTL() int64 {
	if vs.cfg.DeltaSyncConfig == nil {
		return 0
	}

	return vs.cfg.DeltaSyncConfig.BaseTableTTL
}

func (vs versionedSource) tombstone(
	ctx context.Context, key map[string]any, guard *DDBExpression, ver int64, at time.Time, ttlMinutes int64,
) (map[string]any, error) {
	upd := DDBExpression{
		Expression: "SET " + nameVersion + " = " + valNew + ", " + nameChanged + " = " + valChanged + ", " +
			nameDeleted + " = " + valTrue + ", " + nameTTL + " = " + valTTL,
		Names: map[string]string{
			nameVersion: attrVersion, nameChanged: attrLastChanged, nameDeleted: attrDeleted, nameTTL: attrTTL,
		},
		Values: map[string]any{
			valNew: numAttr(ver), valChanged: numAttr(at.UnixMilli()), valTrue: map[string]any{ddbTagBool: true},
			valTTL: numAttr(at.Unix() + ttlMinutes*secondsPerMinute),
		},
	}

	return vs.ddb.UpdateItemRaw(ctx, vs.cfg.TableName, key, upd, guard)
}
