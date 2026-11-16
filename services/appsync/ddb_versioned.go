package appsync

import (
	"context"
	"fmt"
	"maps"
	"math"
	"regexp"
	"sort"
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
	ddb    DynamoDBBackend
	cfg    *DynamoDBDataSourceConfig
	dsName string
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

func rejectCustomDelta(req map[string]any) error {
	if req["customPartitionKey"] != nil || req["populateIndexFields"] == true {
		return fmt.Errorf("%w: customPartitionKey and populateIndexFields delta layouts", ErrUnsupportedDynamoDBOp)
	}

	return nil
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

func keyString(key map[string]any) string {
	names := make([]string, 0, len(key))
	for n := range key {
		names = append(names, n)
	}

	sort.Strings(names)

	parts := make([]string, len(names))

	for i, n := range names {
		parts[i] = fmt.Sprint(ddbPlain(key[n]))
	}

	return strings.Join(parts, "#")
}

// logDelta records one change in the delta table (ds_pk "<data source>:<date>", ds_sk "<time>:<key>:<version>").
func (vs versionedSource) logDelta(ctx context.Context, key, item map[string]any, at time.Time) error {
	table, ok := vs.delta()
	if !ok {
		return nil
	}

	ver, _ := versionOf(item)
	rec := maps.Clone(item)
	rec[attrDeltaPK] = strAttr(vs.dsName + ":" + at.Format(deltaDayLayout))
	rec[attrDeltaSK] = strAttr(at.Format(deltaTimeLayout) + ":" + keyString(key) + ":" + strconv.FormatInt(ver, 10))
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

func versionedWrite(ctx context.Context, vs versionedSource, req map[string]any, op string) (any, error) {
	if err := rejectCustomDelta(req); err != nil {
		return nil, err
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

func (vs versionedSource) read(ctx context.Context, req map[string]any) (map[string]any, int64, bool, error) {
	cur, err := vs.ddb.GetItemRaw(ctx, vs.cfg.TableName, mapOf(req["key"]))
	if err != nil {
		return nil, 0, false, err
	}

	ver, has := versionOf(cur)
	if want, ok := expectedVersion(req); ok && len(cur) > 0 && (!has || want != ver) {
		return nil, 0, false, conflictError(cur)
	}

	return cur, ver, has, nil
}

func versionedPutOnce(ctx context.Context, vs versionedSource, req map[string]any) (any, bool, error) {
	cur, ver, has, err := vs.read(ctx, req)
	if err != nil {
		return nil, false, err
	}

	now := time.Now().UTC()
	item := ddbPutItem(req)
	stamp(item, ver+1, now)

	err = vs.ddb.PutItemRaw(ctx, vs.cfg.TableName, item, versionGuard(condOf(req), has, ver))
	if err != nil {
		if isCondFailed(err) && vs.changedSince(ctx, vs.cfg.TableName, mapOf(req["key"]), cur) {
			return nil, true, nil
		}

		return nil, false, checkFailedErr(req, err)
	}

	if err = vs.logDelta(ctx, mapOf(req["key"]), item, now); err != nil {
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

	cur, ver, has, err := vs.read(ctx, req)
	if err != nil {
		return nil, false, err
	}

	now := time.Now().UTC()
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

	if err = vs.logDelta(ctx, mapOf(req["key"]), item, now); err != nil {
		return nil, false, err
	}

	return ddbPlainItem(item), false, nil
}

func versionedDeleteOnce(ctx context.Context, vs versionedSource, req map[string]any) (any, bool, error) {
	cur, ver, has, err := vs.read(ctx, req)
	if err != nil {
		return nil, false, err
	}

	if len(cur) == 0 {
		res, derr := ddbDelete(ctx, vs.ddb, vs.cfg.TableName, req)

		return res, false, derr
	}

	key := mapOf(req["key"])
	guard := versionGuard(condOf(req), has, ver)
	now := time.Now().UTC()

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

	if err = vs.logDelta(ctx, key, item, now); err != nil {
		return nil, false, err
	}

	return ddbPlainItem(item), false, nil
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
