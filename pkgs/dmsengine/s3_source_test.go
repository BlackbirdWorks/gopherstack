package dmsengine_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/dmsengine"
)

const employeeDefinition = `{
  "TableCount": "1",
  "Tables": [{
    "TableName": "employee", "TablePath": "hr/employee/", "TableOwner": "hr",
    "TableColumns": [
      {"ColumnName": "Id", "ColumnType": "INT8", "ColumnNullable": "false", "ColumnIsPk": "true"},
      {"ColumnName": "LastName", "ColumnType": "STRING", "ColumnLength": "20", "ColumnNullable": "true"},
      {"ColumnName": "HireDate", "ColumnType": "DATETIME", "ColumnScale": "3", "ColumnNullable": "true"},
      {"ColumnName": "Rate", "ColumnType": "NUMERIC", "ColumnPrecision": "5", "ColumnScale": "2",
      "ColumnNullable": "true"}
    ],
    "TableColumnsTotal": "4"
  }]
}`

func s3SourceEndpoint(settings string) dmsengine.EndpointConfig {
	return dmsengine.EndpointConfig{
		Engine: "s3", S3Settings: settings, ExternalTableDefinition: employeeDefinition,
		PollInterval: time.Millisecond,
	}
}

func openS3Source(t *testing.T, objs *memObjects, settings string) dmsengine.Source {
	t.Helper()

	src, err := dmsengine.NewOpener(dmsengine.Stores{Objects: objs}).OpenSource(t.Context(), s3SourceEndpoint(settings))
	require.NoError(t, err)

	return src
}

func TestS3SourceTablesAndScan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		settings string
		objects  map[string]string
		want     []dmsengine.Row
	}{
		{
			name:     "plain files under the table path",
			settings: `{"BucketName":"b"}`,
			objects: map[string]string{
				"b/hr/employee/LOAD001.csv": "101,Smith,2014-06-04 10:00:00,12.50\n102,,2015-10-08 00:00:00,7\n",
				"b/hr/employee/LOAD002.csv": "103,Jones,2017-03-13 00:00:00,\n",
				"b/hr/employee/other.txt":   "ignored",
				"b/hr/department/LOAD1.csv": "ignored",
			},
			want: []dmsengine.Row{
				{"101", "Smith", "2014-06-04 10:00:00", "12.50"},
				{"102", nil, "2015-10-08 00:00:00", "7"},
				{"103", "Jones", "2017-03-13 00:00:00", nil},
			},
		},
		{
			name: "bucket folder, custom delimiters, header and null marker",
			settings: `{"BucketName":"b","BucketFolder":"src","CsvDelimiter":"|","CsvRowDelimiter":";",
			"IgnoreHeaderRows":1,"CsvNullValue":"\\N"}`,
			objects: map[string]string{
				"b/src/hr/employee/LOAD001.csv": "Id|LastName|HireDate|Rate;101|\\N|2014-06-04 10:00:00|1.5;",
			},
			want: []dmsengine.Row{{"101", nil, "2014-06-04 10:00:00", "1.5"}},
		},
		{
			name:     "leading operation column from a DMS S3 target",
			settings: `{"BucketName":"b"}`,
			objects:  map[string]string{"b/hr/employee/LOAD001.csv": "I,101,\"Smith, Jr\",2014-06-04 10:00:00,1\n"},
			want:     []dmsengine.Row{{"101", "Smith, Jr", "2014-06-04 10:00:00", "1"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := openS3Source(t, &memObjects{objs: tt.objects}, tt.settings)
			tables, err := src.Tables(t.Context())
			require.NoError(t, err)
			require.Len(t, tables, 1)

			tbl := tables[0]
			assert.Equal(t, "hr.employee", tbl.Key())
			assert.Equal(t, []string{"Id"}, tbl.PK)
			assert.Equal(t, "int8", tbl.Columns[0].DMS.Name)
			assert.Equal(t, 20, tbl.Columns[1].DMS.Length)
			assert.Equal(t, 2, tbl.Columns[3].DMS.Scale)
			assert.False(t, tbl.Columns[0].Nullable)

			var got []dmsengine.Row

			require.NoError(t, src.Scan(t.Context(), tbl, func(r dmsengine.Row) error {
				got = append(got, r)

				return nil
			}))
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestS3SourceCDCFiles(t *testing.T) {
	t.Parallel()

	objs := &memObjects{objs: map[string]string{
		"b/chg/CDC00001.csv": "INSERT,employee,hr,101,Smith,2014-06-04 10:00:00,1\n" +
			"U,employee,hr,101,Smith,2015-10-08 00:00:00,2\n",
		"b/chg/CDC00003.csv": "DELETE,employee,hr,101,Smith,2015-10-08 00:00:00,2\nI,other,hr,1,x,,\n",
	}}
	src := openS3Source(t, objs, `{"BucketName":"b","CdcPath":"chg"}`)

	tables, err := src.Tables(t.Context())
	require.NoError(t, err)

	lookup := func(schema, name string) (dmsengine.Table, bool) {
		if schema == "hr" && name == "employee" {
			return tables[0], true
		}

		return dmsengine.Table{}, false
	}

	type seen struct {
		op  dmsengine.Op
		pos string
		row dmsengine.Row
	}

	ctx, cancel := context.WithCancel(t.Context())
	got := make(chan seen, 16)
	done := make(chan error, 1)

	go func() {
		done <- src.Stream(ctx, dmsengine.StreamStart{}, lookup, func(c dmsengine.Change) error {
			row := c.After
			if c.Op == dmsengine.OpDelete {
				row = c.Before
			}

			got <- seen{op: c.Op, row: row, pos: c.Position}

			return nil
		})
	}()

	want := []seen{
		{
			op:  dmsengine.OpInsert,
			row: dmsengine.Row{"101", "Smith", "2014-06-04 10:00:00", "1"},
			pos: "chg/CDC00001.csv",
		},
		{
			op:  dmsengine.OpUpdate,
			row: dmsengine.Row{"101", "Smith", "2015-10-08 00:00:00", "2"},
			pos: "chg/CDC00001.csv",
		},
		{pos: "chg/CDC00001.csv"},
		{
			op:  dmsengine.OpDelete,
			row: dmsengine.Row{"101", "Smith", "2015-10-08 00:00:00", "2"},
			pos: "chg/CDC00003.csv",
		},
		{pos: "chg/CDC00003.csv"},
	}

	for _, w := range want {
		assert.Equal(t, w, <-got)
	}

	// A file that sorts before the last one applied is skipped, one that sorts after is picked up.
	require.NoError(t, objs.PutObject(t.Context(), "b", "chg/CDC00002.csv", []byte("I,employee,hr,9,late,,\n")))
	require.NoError(t, objs.PutObject(t.Context(), "b", "chg/CDC00004.csv", []byte("I,employee,hr,4,fresh,,\n")))

	assert.Equal(
		t,
		seen{op: dmsengine.OpInsert, row: dmsengine.Row{"4", "fresh", nil, nil}, pos: "chg/CDC00004.csv"},
		<-got,
	)
	assert.Equal(t, seen{pos: "chg/CDC00004.csv"}, <-got)

	cancel()
	require.NoError(t, <-done)
}

func TestS3SourceOpenErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		store dmsengine.ObjectStore
		name  string
		ep    dmsengine.EndpointConfig
	}{
		{
			name: "no bucket",
			ep: dmsengine.EndpointConfig{
				Engine:                  "s3",
				S3Settings:              `{}`,
				ExternalTableDefinition: employeeDefinition,
			},
			store: &memObjects{},
		},
		{
			name:  "no definition",
			ep:    dmsengine.EndpointConfig{Engine: "s3", S3Settings: `{"BucketName":"b"}`},
			store: &memObjects{},
		},
		{name: "parquet", ep: dmsengine.EndpointConfig{
			Engine:                  "s3",
			S3Settings:              `{"BucketName":"b","DataFormat":"parquet"}`,
			ExternalTableDefinition: employeeDefinition,
		}, store: &memObjects{}},
		{name: "bad type", ep: dmsengine.EndpointConfig{
			Engine:     "s3",
			S3Settings: `{"BucketName":"b"}`,
			ExternalTableDefinition: `{"Tables":[{"TableName":"t","TableOwner":"o",
			"TableColumns":[{"ColumnName":"c","ColumnType":"MAP"}]}]}`,
		}, store: &memObjects{}},
		{name: "column total mismatch", ep: dmsengine.EndpointConfig{
			Engine:     "s3",
			S3Settings: `{"BucketName":"b"}`,
			ExternalTableDefinition: `{"Tables":[{"TableName":"t","TableOwner":"o","TableColumnsTotal":"2",
			"TableColumns":[{"ColumnName":"c","ColumnType":"INT4"}]}]}`,
		}, store: &memObjects{}},
		{name: "no store", ep: dmsengine.EndpointConfig{
			Engine:                  "s3",
			S3Settings:              `{"BucketName":"b"}`,
			ExternalTableDefinition: employeeDefinition,
		}, store: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := dmsengine.NewOpener(dmsengine.Stores{Objects: tt.store}).OpenSource(t.Context(), tt.ep)
			require.Error(t, err)
		})
	}
}

func TestS3SourceStreamNeedsCdcPath(t *testing.T) {
	t.Parallel()

	src := openS3Source(t, &memObjects{}, `{"BucketName":"b"}`)

	err := src.Stream(t.Context(), dmsengine.StreamStart{}, nil, func(dmsengine.Change) error { return nil })
	require.ErrorIs(t, err, dmsengine.ErrS3Source)
}

func TestApplySecret(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		secret  string
		want    dmsengine.EndpointConfig
		wantErr bool
	}{
		{
			name:   "numeric port",
			secret: `{"username":"admin","password":"pw","host":"db.example","port":3306,"engine":"mysql","dbname":"ignored"}`,
			want:   dmsengine.EndpointConfig{User: "admin", Password: "pw", Server: "db.example", Port: 3306},
		},
		{
			name:   "string port as the guide shows",
			secret: `{"username":"admin","password":"pw","port":"8190","host":"db.example"}`,
			want:   dmsengine.EndpointConfig{User: "admin", Password: "pw", Server: "db.example", Port: 8190},
		},
		{name: "not json", secret: `plain`, wantErr: true},
		{name: "no host", secret: `{"username":"a","password":"b","port":1}`, wantErr: true},
		{name: "no port", secret: `{"username":"a","password":"b","host":"h"}`, wantErr: true},
		{name: "bad port", secret: `{"username":"a","password":"b","host":"h","port":"x"}`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ep := dmsengine.EndpointConfig{Database: "appdb"}
			err := dmsengine.ApplySecret(&ep, tt.secret)

			if tt.wantErr {
				require.ErrorIs(t, err, dmsengine.ErrSecret)

				return
			}

			require.NoError(t, err)

			tt.want.Database = "appdb"
			assert.Equal(t, tt.want, ep)
		})
	}
}
