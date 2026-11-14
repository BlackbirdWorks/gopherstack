package athena_test

import (
	"maps"
	"testing"

	"github.com/iskorotkov/avro/v2/ocf"
	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/athena"
)

func TestSDK_AvroColumnsDerivedFromSchema(t *testing.T) {
	t.Parallel()

	bare := func(params map[string]string) *athena.GlueTableDef {
		d := eventsAvroDef()
		d.Columns = nil
		d.Parameters = params

		return d
	}

	cases := []struct {
		params map[string]string
		extra  map[string][]byte
		name   string
	}{
		{map[string]string{"avro.schema.literal": eventsAvroSchema}, nil, "literal"},
		{
			map[string]string{"avro.schema.url": "s3://lake/schemas/events.avsc"},
			map[string][]byte{"lake/schemas/events.avsc": []byte(eventsAvroSchema)}, "url",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			objects := map[string][]byte{"lake/events/a.avro": eventsAvroFile(t, ocf.Null)}
			maps.Copy(objects, tc.extra)

			client := sdkClientFor(t, objects, map[string]*athena.GlueTableDef{"shop/events": bare(tc.params)})

			got := sdkRun(
				t,
				client,
				"SELECT id, name, day, price, tags[2], attrs['x'], owner.first FROM events WHERE id = 1",
			)
			assert.Equal(t, [][]string{{"1", "ann", "2024-03-05", "12.34", "b", "7", "Ann"}}, got.rows)
		})
	}
}
