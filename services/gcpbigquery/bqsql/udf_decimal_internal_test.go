package bqsql

import (
	"database/sql/driver"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecimalFunctionsRejectMalformedArguments(t *testing.T) {
	t.Parallel()

	defs := map[string]udfDef{}
	addDecimalUDFs(defs)

	good := mustDecimal(t, "1.5").SQLText()
	pool := []driver.Value{
		nil, "", "x", "#", good[:64], good + "0", "#" + good[1:] + "", []byte("blob"), int64(1), int64(0), int64(-1),
		float64(1.5), float64(-1e300), good, int64(1 << 62),
	}

	for name, d := range defs {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			arities := []int{int(d.n)}
			if d.n < 0 {
				arities = []int{1, 2, 3, 4, 5}
			}

			for _, n := range arities {
				for _, first := range pool {
					for _, second := range pool {
						args := make([]driver.Value, n)
						for i := range args {
							args[i] = pool[(i+1)%len(pool)]
						}

						args[0] = first
						if n > 1 {
							args[1] = second
						}

						assert.NotPanics(t, func() { _, _ = d.fn(args) }, "%s(%v)", name, args)
					}
				}
			}
		})
	}
}

func mustDecimal(t *testing.T, s string) Decimal {
	t.Helper()

	d, err := ParseDecimal(KindNumeric, s)
	if err != nil {
		t.Fatal(err)
	}

	return d
}

func TestDecimalAggregatesRejectMalformedArguments(t *testing.T) {
	t.Parallel()

	for _, avg := range []bool{false, true} {
		a := &decAgg{sum: new(big.Int), avg: avg, kind: KindNumeric}

		for _, v := range []driver.Value{nil, "x", []byte("b"), int64(3), float64(1), mustDecimal(t, "1").SQLText()} {
			assert.NotPanics(t, func() {
				_ = a.Step(nil, []driver.Value{v})
				_ = a.WindowInverse(nil, []driver.Value{v})
				_, _ = a.WindowValue(nil)
			})
		}
	}

	m := &decMomentAgg{sum: new(big.Int), sumSq: new(big.Int)}

	for _, v := range []driver.Value{nil, "x", int64(1), mustDecimal(t, "2").SQLText()} {
		assert.NotPanics(t, func() {
			_ = m.Step(nil, []driver.Value{v})
			_, _ = m.WindowValue(nil)
			_ = m.WindowInverse(nil, []driver.Value{v})
		})
	}
}
