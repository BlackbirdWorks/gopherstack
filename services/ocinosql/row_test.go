package ocinosql_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/nosql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (e *env) getRow(t *testing.T, table string, key ...string) nosql.GetRowResponse {
	t.Helper()

	resp, err := e.client.GetRow(t.Context(), nosql.GetRowRequest{
		TableNameOrId: new(table), CompartmentId: new(testCompartment), Key: key,
	})
	require.NoError(t, err)

	return resp
}

func TestRowPutGetDelete(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ddl := "CREATE TABLE rich (id LONG, big LONG, n NUMBER, d DOUBLE, ok BOOLEAN, at TIMESTAMP(3), " +
		"bin BINARY, tags ARRAY(STRING), m MAP(INTEGER), r RECORD(a STRING, b INTEGER), j JSON, " +
		"color ENUM(red, green), PRIMARY KEY(id))"
	e.mkTable(t, "rich", ddl)

	put := e.put(t, "rich", map[string]any{
		"id": 7, "big": int64(9007199254740993), "n": 12.5, "d": 1.5, "ok": true, "at": "2024-03-04T05:06:07.123456Z",
		"bin": "AQID", "tags": []string{"x", "y"}, "m": map[string]int{"k": 1},
		"r": map[string]any{"a": "s", "b": 2}, "j": map[string]any{"deep": []int{1, 2}}, "color": "red",
	})
	require.NotNil(t, put.Version)
	assert.Positive(t, *put.Usage.WriteUnitsConsumed)

	got := e.getRow(t, "rich", "id:7")
	assert.Equal(t, *put.Version, *got.Etag)
	assert.Equal(t, "2024-03-04T05:06:07.123Z", got.Value["at"], "timestamp truncated to column precision")
	assert.Equal(t, true, got.Value["ok"])
	assert.Equal(t, []any{"x", "y"}, got.Value["tags"])
	assert.Equal(t, "red", got.Value["color"])
	assert.Equal(t, map[string]any{"a": "s", "b": float64(2)}, got.Value["r"])
	assert.Positive(t, *got.Usage.ReadUnitsConsumed)

	raw, body, _ := rawDo(
		t,
		http.MethodGet,
		e.base+"/20190828/tables/rich/rows?compartmentId="+testCompartment+"&key=id:7",
		"",
	)
	require.Equal(t, http.StatusOK, raw)
	assert.Contains(t, body, `"big":9007199254740993`, "long values keep full precision")

	absent := e.getRow(t, "rich", "id:8")
	assert.Nil(t, absent.Value)

	del, err := e.client.DeleteRow(t.Context(), nosql.DeleteRowRequest{
		TableNameOrId: new(
			"rich",
		),
		CompartmentId:  new(testCompartment),
		Key:            []string{"id:7"},
		IsGetReturnRow: new(true),
	})
	require.NoError(t, err)
	assert.True(t, *del.IsSuccess)
	assert.Equal(t, *put.Version, *del.ExistingVersion)
	assert.Equal(t, "red", del.ExistingValue["color"])
	assert.Nil(t, e.getRow(t, "rich", "id:7").Value)

	del, err = e.client.DeleteRow(t.Context(), nosql.DeleteRowRequest{
		TableNameOrId: new("rich"), CompartmentId: new(testCompartment), Key: []string{"id:7"},
	})
	require.NoError(t, err)
	assert.False(t, *del.IsSuccess)
}

func TestRowPutOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		option       nosql.UpdateRowDetailsOptionEnum
		existing     bool
		returnRow    bool
		wantApplied  bool
		wantExisting bool
	}{
		{name: "absent on missing", option: nosql.UpdateRowDetailsOptionAbsent, wantApplied: true},
		{name: "absent on existing", option: nosql.UpdateRowDetailsOptionAbsent, existing: true},
		{
			name:         "absent on existing returns row",
			option:       nosql.UpdateRowDetailsOptionAbsent,
			existing:     true,
			returnRow:    true,
			wantExisting: true,
		},
		{name: "present on missing", option: nosql.UpdateRowDetailsOptionPresent},
		{name: "present on existing", option: nosql.UpdateRowDetailsOptionPresent, existing: true, wantApplied: true},
		{name: "no option overwrites", existing: true, wantApplied: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			e.mkTable(t, "t", usersDDL("t"))

			if tt.existing {
				e.put(t, "t", map[string]any{"id": 1, "name": "old"})
			}

			resp, err := e.client.UpdateRow(t.Context(), nosql.UpdateRowRequest{
				TableNameOrId: new("t"),
				Value:         map[string]any{"id": 1, "name": "new"}, CompartmentId: new(testCompartment),
				Option: tt.option, IsGetReturnRow: new(tt.returnRow),
			})
			require.NoError(t, err)

			assert.Equal(t, tt.wantApplied, resp.Version != nil)
			assert.Equal(t, tt.wantExisting, resp.ExistingValue != nil)

			want := "old"
			if tt.wantApplied {
				want = "new"
			}

			if tt.existing || tt.wantApplied {
				assert.Equal(t, want, e.getRow(t, "t", "id:1").Value["name"])
			}

			if tt.wantExisting {
				assert.Equal(t, "old", resp.ExistingValue["name"])
				assert.NotNil(t, resp.ExistingVersion)
			}
		})
	}
}

func TestRowVersionMatch(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.mkTable(t, "t", usersDDL("t"))

	v1 := e.put(t, "t", map[string]any{"id": 1, "name": "a"})

	stale, err := e.client.UpdateRow(t.Context(), nosql.UpdateRowRequest{
		TableNameOrId: new("t"), IfMatch: new("bogus"),
		Value: map[string]any{"id": 1, "name": "b"}, CompartmentId: new(testCompartment),
	})
	require.NoError(t, err)
	assert.Nil(t, stale.Version)

	ok, err := e.client.UpdateRow(t.Context(), nosql.UpdateRowRequest{
		TableNameOrId: new("t"), IfMatch: v1.Version,
		Value: map[string]any{"id": 1, "name": "b"}, CompartmentId: new(testCompartment),
	})
	require.NoError(t, err)
	require.NotNil(t, ok.Version)
	assert.NotEqual(t, *v1.Version, *ok.Version)

	del, err := e.client.DeleteRow(t.Context(), nosql.DeleteRowRequest{
		TableNameOrId: new("t"), CompartmentId: new(testCompartment), Key: []string{"id:1"}, IfMatch: v1.Version,
	})
	require.NoError(t, err)
	assert.False(t, *del.IsSuccess, "old version no longer matches")
	assert.Equal(t, "b", e.getRow(t, "t", "id:1").Value["name"])
}

func TestRowValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.mkTable(t, "t", "CREATE TABLE t (id INTEGER, s STRING, i INTEGER, b BOOLEAN, req STRING NOT NULL DEFAULT 'x', "+
		"arr ARRAY(INTEGER), e ENUM(a, b), bin BINARY, PRIMARY KEY(id))")

	tests := []struct {
		value map[string]any
		name  string
		exact bool
		ok    bool
	}{
		{name: "valid", value: map[string]any{"id": 1, "s": "x"}, ok: true},
		{name: "missing key", value: map[string]any{"s": "x"}},
		{name: "null key", value: map[string]any{"id": nil}},
		{name: "string for int", value: map[string]any{"id": 1, "i": "5"}},
		{name: "fraction for int", value: map[string]any{"id": 1, "i": 1.5}},
		{name: "int overflow", value: map[string]any{"id": 1, "i": int64(1) << 40}},
		{name: "int for string", value: map[string]any{"id": 1, "s": 5}},
		{name: "string for bool", value: map[string]any{"id": 1, "b": "true"}},
		{name: "bad array element", value: map[string]any{"id": 1, "arr": []any{1, "z"}}},
		{name: "bad enum", value: map[string]any{"id": 1, "e": "c"}},
		{name: "bad binary", value: map[string]any{"id": 1, "bin": "not base64!"}},
		{name: "null for not null", value: map[string]any{"id": 1, "req": nil}},
		{name: "extra ignored", value: map[string]any{"id": 1, "zzz": 1}, ok: true},
		{name: "extra exact", value: map[string]any{"id": 1, "zzz": 1}, exact: true},
		{name: "missing exact", value: map[string]any{"id": 1}, exact: true},
		{name: "case insensitive column", value: map[string]any{"ID": 2, "S": "y"}, ok: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.client.UpdateRow(t.Context(), nosql.UpdateRowRequest{
				TableNameOrId: new("t"),
				Value:         tt.value, CompartmentId: new(testCompartment), IsExactMatch: new(tt.exact),
			})
			if tt.ok {
				require.NoError(t, err)

				return
			}

			se := svcErr(t, err)
			assert.Equal(t, http.StatusBadRequest, se.GetHTTPStatusCode())
			assert.Equal(t, "InvalidParameter", se.GetCode())
		})
	}
}

func TestRowColumnDefault(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.mkTable(
		t,
		"t",
		"CREATE TABLE t (id INTEGER, req STRING NOT NULL DEFAULT 'x', n INTEGER DEFAULT 4, PRIMARY KEY(id))",
	)
	e.put(t, "t", map[string]any{"id": 1})

	got := e.getRow(t, "t", "id:1")
	assert.Equal(t, "x", got.Value["req"])
	assert.InDelta(t, 4, got.Value["n"], 0)
}

func TestRowKeyErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.mkTable(t, "t", "CREATE TABLE t (a INTEGER, b STRING, c STRING, PRIMARY KEY(SHARD(a), b))")

	tests := []struct {
		name string
		key  []string
	}{
		{name: "no key", key: nil},
		{name: "partial key", key: []string{"a:1"}},
		{name: "no colon", key: []string{"a=1", "b:x"}},
		{name: "non key column", key: []string{"a:1", "c:x"}},
		{name: "bad int", key: []string{"a:z", "b:x"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.client.GetRow(t.Context(), nosql.GetRowRequest{
				TableNameOrId: new("t"), CompartmentId: new(testCompartment), Key: tt.key,
			})
			assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

			_, err = e.client.DeleteRow(t.Context(), nosql.DeleteRowRequest{
				TableNameOrId: new("t"), CompartmentId: new(testCompartment), Key: tt.key,
			})
			assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
		})
	}

	_, err := e.client.GetRow(t.Context(), nosql.GetRowRequest{
		TableNameOrId: new("nope"), CompartmentId: new(testCompartment), Key: []string{"a:1"},
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestRowTTL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ttl      *int
		useTable *bool
		name     string
		want     time.Duration
	}{
		{name: "table default", want: 48 * time.Hour},
		{name: "explicit days", ttl: new(5), want: 5 * 24 * time.Hour},
		{name: "explicit zero never expires", ttl: new(0)},
		{name: "use table default flag", ttl: new(9), useTable: new(true), want: 48 * time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			e.mkTable(t, "t", usersDDL("t")+" USING TTL 48 HOURS")

			start := e.clock.Now()

			_, err := e.client.UpdateRow(t.Context(), nosql.UpdateRowRequest{
				TableNameOrId: new("t"),
				Value:         map[string]any{"id": 1}, CompartmentId: new(testCompartment),
				Ttl: tt.ttl, IsTtlUseTableDefault: tt.useTable,
			})
			require.NoError(t, err)

			got := e.getRow(t, "t", "id:1")
			if tt.want == 0 {
				assert.Nil(t, got.TimeOfExpiration)
				e.clock.Advance(1000 * 24 * time.Hour)
				assert.NotNil(t, e.getRow(t, "t", "id:1").Value)

				return
			}

			require.NotNil(t, got.TimeOfExpiration)
			assert.WithinDuration(t, start.Add(tt.want), got.TimeOfExpiration.Time, time.Second)

			e.clock.Advance(tt.want - time.Minute)
			assert.NotNil(t, e.getRow(t, "t", "id:1").Value, "still alive just before expiry")

			e.clock.Advance(2 * time.Minute)
			assert.Nil(t, e.getRow(t, "t", "id:1").Value, "expired rows are invisible")
			assert.Empty(t, e.query(t, "SELECT * FROM t", 0, nil).Items)
		})
	}

	e := newEnv(t, envOpts{})
	e.mkTable(t, "t", usersDDL("t"))

	_, err := e.client.UpdateRow(t.Context(), nosql.UpdateRowRequest{
		TableNameOrId: new("t"),
		Value:         map[string]any{"id": 1}, CompartmentId: new(testCompartment), Ttl: new(-1),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
}

func TestRowSweepRemovesExpired(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.mkTable(t, "t", usersDDL("t")+" USING TTL 1 HOURS")
	e.put(t, "t", map[string]any{"id": 1})

	e.clock.Advance(2 * time.Hour)
	e.backend.SweepRows()

	snap := string(e.backend.Snapshot(t.Context()))
	assert.NotContains(t, snap, `"rows"`, "expired row no longer stored")
}

func TestIdentityAndUUID(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.mkTable(t, "ids", "CREATE TABLE ids (id INTEGER GENERATED ALWAYS AS IDENTITY (START WITH 10 INCREMENT BY 5), "+
		"u STRING AS UUID GENERATED BY DEFAULT, v STRING, PRIMARY KEY(id))")

	r1 := e.put(t, "ids", map[string]any{"v": "a"})
	r2 := e.put(t, "ids", map[string]any{"v": "b"})
	assert.Equal(t, "10", *r1.GeneratedValue)
	assert.Equal(t, "15", *r2.GeneratedValue)

	got := e.getRow(t, "ids", "id:15")
	assert.Equal(t, "b", got.Value["v"])
	assert.Len(t, got.Value["u"], 36)

	_, err := e.client.UpdateRow(t.Context(), nosql.UpdateRowRequest{
		TableNameOrId: new("ids"),
		Value:         map[string]any{"id": 1, "v": "c"}, CompartmentId: new(testCompartment),
	})
	assert.Equal(
		t,
		http.StatusBadRequest,
		svcErr(t, err).GetHTTPStatusCode(),
		"ALWAYS identity rejects explicit values",
	)

	tbl := e.getTable(t, "ids")
	assert.True(t, *tbl.Schema.Identity.IsAlways)
	assert.Equal(t, "id", *tbl.Schema.Identity.ColumnName)

	e.mkTable(t, "byd", "CREATE TABLE byd (id LONG GENERATED BY DEFAULT AS IDENTITY, PRIMARY KEY(id))")
	assert.Equal(t, "1", *e.put(t, "byd", map[string]any{}).GeneratedValue)
	assert.Nil(t, e.put(t, "byd", map[string]any{"id": 50}).GeneratedValue)
	assert.Equal(t, "2", *e.put(t, "byd", map[string]any{}).GeneratedValue)

	_, err = e.client.UpdateRow(t.Context(), nosql.UpdateRowRequest{
		TableNameOrId: new("ids"),
		Value:         map[string]any{"u": "not-a-uuid", "v": "d"}, CompartmentId: new(testCompartment),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "invalid UUID is refused")
}

func TestListTableUsage(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.mkTable(t, "t", usersDDL("t"))

	empty, err := e.client.ListTableUsage(t.Context(), nosql.ListTableUsageRequest{
		TableNameOrId: new("t"), CompartmentId: new(testCompartment),
	})
	require.NoError(t, err)
	assert.Empty(t, empty.Items)

	e.put(t, "t", map[string]any{"id": 1, "name": "a"})
	e.put(t, "t", map[string]any{"id": 2, "name": "b"})
	e.getRow(t, "t", "id:1")
	e.clock.Advance(2 * time.Minute)
	e.getRow(t, "t", "id:2")

	resp, err := e.client.ListTableUsage(t.Context(), nosql.ListTableUsageRequest{
		TableNameOrId: new("t"), CompartmentId: new(testCompartment),
	})
	require.NoError(t, err)
	require.Len(t, resp.Items, 2)
	assert.Equal(t, 60, *resp.Items[0].SecondsInPeriod)
	assert.Equal(t, 2, *resp.Items[0].WriteUnits)
	assert.Equal(t, 1, *resp.Items[0].ReadUnits)
	assert.Equal(t, 0, *resp.Items[1].WriteUnits)
	assert.Equal(t, 1, *resp.Items[1].ReadUnits)
	assert.True(t, resp.Items[1].TimeStarted.After(resp.Items[0].TimeStarted.Time))

	start := e.clock.Now().Add(time.Minute)
	late, err := e.client.ListTableUsage(t.Context(), nosql.ListTableUsageRequest{
		TableNameOrId: new("t"), CompartmentId: new(testCompartment),
		TimeStart: &common.SDKTime{Time: start}, TimeEnd: &common.SDKTime{Time: start.Add(time.Hour)},
	})
	require.NoError(t, err)
	assert.Empty(t, late.Items)
}
