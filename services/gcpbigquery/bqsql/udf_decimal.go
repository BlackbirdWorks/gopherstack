package bqsql

import (
	"database/sql/driver"
	"math"
	"math/big"
	"slices"
	"strings"

	sqlitedriver "modernc.org/sqlite"
)

func decFail(err error) error {
	return errBQ("%s", strings.TrimPrefix(err.Error(), ErrDecimal.Error()+": "))
}

func decKindArg(v driver.Value) Kind {
	if n, ok := v.(int64); ok && n == 1 {
		return KindBigNumeric
	}

	return KindNumeric
}

func decArg(v driver.Value, k Kind) (Decimal, error) {
	s, ok := v.(string)
	if !ok {
		return Decimal{}, errBQ("expected a NUMERIC value")
	}

	d, err := DecodeSQLText(s, k)
	if err != nil {
		return Decimal{}, decFail(err)
	}

	return d, nil
}

func decOut(d Decimal, err error) (driver.Value, error) {
	if err != nil {
		return nil, decFail(err)
	}

	return d.SQLText(), nil
}

func decUnary(op func(Decimal) (Decimal, error)) udfFn {
	return func(a []driver.Value) (driver.Value, error) {
		if a[0] == nil {
			return nil, nil
		}

		k := decKindArg(a[len(a)-1])

		x, err := decArg(a[0], k)
		if err != nil {
			return nil, err
		}

		return decOut(op(x))
	}
}

func decBinary(op func(x, y Decimal) (Decimal, error)) udfFn {
	return func(a []driver.Value) (driver.Value, error) {
		if a[0] == nil || a[1] == nil {
			return nil, nil
		}

		k := decKindArg(a[argc2])

		x, err := decArg(a[0], k)
		if err != nil {
			return nil, err
		}

		y, err := decArg(a[1], k)
		if err != nil {
			return nil, err
		}

		return decOut(op(x, y))
	}
}

const maxDigitsArg = 1000

func clampDigits(n int64) int { return int(max(min(n, maxDigitsArg), -maxDigitsArg)) }

func decRound(a []driver.Value) (driver.Value, error) {
	if anyNull(a[:argc2]) {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	k := decKindArg(a[argc4-1])

	x, err := decArg(a[0], k)
	if err != nil {
		return nil, err
	}

	digits, _ := a[1].(int64)
	mode := RoundHalfAwayFromZero

	if m, _ := a[argc2].(int64); m == 1 {
		mode = RoundHalfEven
	}

	return decOut(x.Round(clampDigits(digits), mode))
}

func decTrunc(a []driver.Value) (driver.Value, error) {
	if anyNull(a[:argc2]) {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	x, err := decArg(a[0], decKindArg(a[argc2]))
	if err != nil {
		return nil, err
	}

	digits, _ := a[1].(int64)

	return x.Trunc(clampDigits(digits)).SQLText(), nil
}

func decFromInt(a []driver.Value) (driver.Value, error) {
	if a[0] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	n, err := toInt(a[0])
	if err != nil {
		return nil, err
	}

	return decOut(DecimalFromInt(decKindArg(a[1]), n))
}

func decFromFloat(a []driver.Value) (driver.Value, error) {
	if a[0] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	f, err := toFloat(a[0])
	if err != nil {
		return nil, err
	}

	return decOut(DecimalFromFloat(decKindArg(a[1]), f))
}

func decFromStr(parse func(Kind, string) (Decimal, error)) udfFn {
	return func(a []driver.Value) (driver.Value, error) {
		if a[0] == nil {
			return nil, nil
		}

		return decOut(parse(decKindArg(a[1]), toStr(a[0])))
	}
}

func decConvert(a []driver.Value) (driver.Value, error) {
	if a[0] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	k := decKindArg(a[1])

	d, err := DecodeSQLText(toStr(a[0]), KindBigNumeric)
	if err != nil {
		return nil, decFail(err)
	}

	return decOut(d.As(k))
}

func decConstrain(a []driver.Value) (driver.Value, error) {
	if a[0] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	d, err := decArg(a[0], decKindArg(a[argc4-1]))
	if err != nil {
		return nil, err
	}

	p, _ := a[1].(int64)
	s, _ := a[argc2].(int64)

	return decOut(d.Constrain(int(p), int(s)))
}

func decToFloat(a []driver.Value) (driver.Value, error) {
	if a[0] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	d, err := decArg(a[0], KindBigNumeric)
	if err != nil {
		return nil, err
	}

	return d.Float64(), nil
}

func decToInt(a []driver.Value) (driver.Value, error) {
	if a[0] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	d, err := decArg(a[0], KindBigNumeric)
	if err != nil {
		return nil, err
	}

	n, err := d.Int64()
	if err != nil {
		return nil, decFail(err)
	}

	return n, nil
}

func decToStr(a []driver.Value) (driver.Value, error) {
	if a[0] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	d, err := decArg(a[0], KindBigNumeric)
	if err != nil {
		return nil, err
	}

	return d.String(), nil
}

func decSign(x Decimal) (Decimal, error) {
	return DecimalFromInt(x.Kind(), int64(x.Sign()))
}

func decNeg(x Decimal) (Decimal, error) { return x.Neg() }

func decAbs(x Decimal) (Decimal, error) { return x.Abs() }

func decCeil(x Decimal) (Decimal, error) { return x.Ceil() }

func decFloor(x Decimal) (Decimal, error) { return x.Floor() }

func decSqrt(x Decimal) (Decimal, error) { return x.Sqrt() }

func decExp(x Decimal) (Decimal, error) { return x.Exp() }

func decLn(x Decimal) (Decimal, error) { return x.Ln() }

func decLog10(x Decimal) (Decimal, error) { return x.Log10() }

func decGenerateArray(a []driver.Value) (driver.Value, error) {
	if anyNull(a[:argc3]) {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	k := decKindArg(a[argc3])

	lo, err := decArg(a[0], k)
	if err != nil {
		return nil, err
	}

	hi, err := decArg(a[1], k)
	if err != nil {
		return nil, err
	}

	step, err := decArg(a[argc2], k)
	if err != nil {
		return nil, err
	}

	return decGenerate(lo, hi, step)
}

const (
	maxGenerated = 1_000_000
	squaredScale = 2 * bigNumericScale
)

func decGenerate(lo, hi, step Decimal) (driver.Value, error) {
	if step.Sign() == 0 {
		return nil, errBQ("Sequence step in the series can't be 0.")
	}

	var sb strings.Builder

	sb.WriteByte('[')

	n := 0

	for cur, ok := lo, inSeries(lo, hi, step); ok; {
		if n >= maxGenerated {
			return nil, errBQ("A generated array is too large")
		}

		if n > 0 {
			sb.WriteByte(',')
		}

		sb.WriteString(`"` + cur.SQLText() + `"`)

		n++

		next, err := cur.Add(step)
		ok = err == nil && inSeries(next, hi, step)
		cur = next
	}

	sb.WriteByte(']')

	return sb.String(), nil
}

func inSeries(cur, hi, step Decimal) bool {
	if step.Sign() > 0 {
		return cur.Cmp(hi) <= 0
	}

	return cur.Cmp(hi) >= 0
}

func decJSON(a []driver.Value) (driver.Value, error) {
	if a[0] == nil {
		return "null", nil
	}

	d, err := decArg(a[0], KindBigNumeric)
	if err != nil {
		return nil, err
	}

	return d.String(), nil
}

func addDecimalUDFs(d map[string]udfDef) {
	d["dec_add"] = udfDef{n: argc3, fn: decBinary(Decimal.Add)}
	d["dec_sub"] = udfDef{n: argc3, fn: decBinary(Decimal.Sub)}
	d["dec_mul"] = udfDef{n: argc3, fn: decBinary(Decimal.Mul)}
	d["dec_div"] = udfDef{n: argc3, fn: decBinary(Decimal.Quo)}
	d["dec_mod"] = udfDef{n: argc3, fn: decBinary(Decimal.Mod)}
	d["dec_intdiv"] = udfDef{n: argc3, fn: decBinary(Decimal.IntDiv)}
	d["dec_pow"] = udfDef{n: argc3, fn: decBinary(Decimal.Pow)}
	d["dec_log"] = udfDef{n: argc3, fn: decBinary(Decimal.Log)}
	d["dec_neg"] = udfDef{n: argc2, fn: decUnary(decNeg)}
	d["dec_abs"] = udfDef{n: argc2, fn: decUnary(decAbs)}
	d["dec_sign"] = udfDef{n: argc2, fn: decUnary(decSign)}
	d["dec_ceil"] = udfDef{n: argc2, fn: decUnary(decCeil)}
	d["dec_floor"] = udfDef{n: argc2, fn: decUnary(decFloor)}
	d["dec_sqrt"] = udfDef{n: argc2, fn: decUnary(decSqrt)}
	d["dec_exp"] = udfDef{n: argc2, fn: decUnary(decExp)}
	d["dec_ln"] = udfDef{n: argc2, fn: decUnary(decLn)}
	d["dec_log10"] = udfDef{n: argc2, fn: decUnary(decLog10)}
	d["dec_round"] = udfDef{n: argc4, fn: decRound}
	d["dec_trunc"] = udfDef{n: argc3, fn: decTrunc}
	d["dec_from_int"] = udfDef{n: argc2, fn: decFromInt}
	d["dec_from_float"] = udfDef{n: argc2, fn: decFromFloat}
	d["dec_from_str"] = udfDef{n: argc2, fn: decFromStr(ParseDecimal)}
	d["dec_parse"] = udfDef{n: argc2, fn: decFromStr(ParseNumericText)}
	d["dec_convert"] = udfDef{n: argc2, fn: decConvert}
	d["dec_constrain"] = udfDef{n: argc4, fn: decConstrain}
	d["dec_to_float"] = udfDef{n: 1, fn: decToFloat}
	d["dec_to_int"] = udfDef{n: 1, fn: decToInt}
	d["dec_to_str"] = udfDef{n: 1, fn: decToStr}
	d["dec_json"] = udfDef{n: 1, fn: decJSON}
	d["format_dec"] = udfDef{n: -1, fn: udfFormatDec}
	d["dec_generate_array"] = udfDef{n: argc4, fn: decGenerateArray}
}

type decAgg struct {
	sum  *big.Int
	n    int64
	kind Kind
	avg  bool
}

func (a *decAgg) Step(_ *sqlitedriver.FunctionContext, args []driver.Value) error {
	return a.apply(args, false)
}

func (a *decAgg) WindowInverse(_ *sqlitedriver.FunctionContext, args []driver.Value) error {
	return a.apply(args, true)
}

func (a *decAgg) apply(args []driver.Value, remove bool) error {
	if args[0] == nil {
		return nil
	}

	v, err := decodeScaled(toStr(args[0]))
	if err != nil {
		return decFail(err)
	}

	if remove {
		a.sum.Sub(a.sum, v)
		a.n--

		return nil
	}

	a.sum.Add(a.sum, v)
	a.n++

	return nil
}

func (a *decAgg) WindowValue(*sqlitedriver.FunctionContext) (driver.Value, error) {
	if a.n == 0 {
		return nil, nil //nolint:nilnil // an empty group has no sum
	}

	var (
		d   Decimal
		err error
	)

	if a.avg {
		den := big.NewInt(a.n)
		if a.kind == KindNumeric {
			den.Mul(den, pow10(narrowShift))
		}

		d, err = newDecimal(a.kind, divRound(new(big.Int).Set(a.sum), den), "AVG")
	} else {
		d, err = decimalFromScaled(a.kind, new(big.Int).Set(a.sum), "SUM")
	}

	if err != nil {
		return nil, decFail(err)
	}

	return d.SQLText(), nil
}

func (a *decAgg) Final(*sqlitedriver.FunctionContext) {}

func registerDecimalAggregates() {
	for name, spec := range map[string]struct {
		kind Kind
		avg  bool
	}{
		"bq_dec_sum_n": {KindNumeric, false}, "bq_dec_sum_b": {KindBigNumeric, false},
		"bq_dec_avg_n": {KindNumeric, true}, "bq_dec_avg_b": {KindBigNumeric, true},
	} {
		sqlitedriver.MustRegisterFunction(name, &sqlitedriver.FunctionImpl{
			NArgs: 1,
			MakeAggregate: func(sqlitedriver.FunctionContext) (sqlitedriver.AggregateFunction, error) {
				return &decAgg{avg: spec.avg, kind: spec.kind, sum: new(big.Int)}, nil
			},
		})
	}
}

// decFormatArg is a decoded decimal handed to the FORMAT implementation; its Go type cannot collide with any SQL value.
type decFormatArg struct {
	text string
	kind Kind
}

func udfFormatDec(a []driver.Value) (driver.Value, error) {
	if len(a) < argc2 {
		return nil, errBQ("Too few arguments to FORMAT")
	}

	mask := toStr(a[0])
	args := slices.Clone(a[1:])

	for j := range len(mask) {
		if j+1 >= len(args) || mask[j] == '-' || args[j+1] == nil {
			continue
		}

		k := KindNumeric
		if mask[j] == 'B' {
			k = KindBigNumeric
		}

		d, err := decArg(args[j+1], k)
		if err != nil {
			return nil, err
		}

		args[j+1] = decFormatArg{text: d.String(), kind: k}
	}

	return udfFormat(args)
}

// decMomentAgg keeps exact power sums so VARIANCE and STDDEV of decimals only round once, on the final FLOAT64.
type decMomentAgg struct {
	sum    *big.Int
	sumSq  *big.Int
	n      int64
	sample bool
	root   bool
}

func (a *decMomentAgg) Step(_ *sqlitedriver.FunctionContext, args []driver.Value) error {
	return a.apply(args, 1)
}

func (a *decMomentAgg) WindowInverse(_ *sqlitedriver.FunctionContext, args []driver.Value) error {
	return a.apply(args, -1)
}

func (a *decMomentAgg) apply(args []driver.Value, sign int64) error {
	if args[0] == nil {
		return nil
	}

	v, err := decodeScaled(toStr(args[0]))
	if err != nil {
		return decFail(err)
	}

	a.sum.Add(a.sum, new(big.Int).Mul(v, big.NewInt(sign)))
	a.sumSq.Add(a.sumSq, new(big.Int).Mul(new(big.Int).Mul(v, v), big.NewInt(sign)))
	a.n += sign

	return nil
}

func (a *decMomentAgg) WindowValue(*sqlitedriver.FunctionContext) (driver.Value, error) {
	denom := a.n
	if a.sample {
		denom--
	}

	if a.n <= 0 || denom <= 0 {
		return nil, nil //nolint:nilnil // too few rows
	}

	num := new(big.Int).Mul(a.sumSq, big.NewInt(a.n))
	num.Sub(num, new(big.Int).Mul(a.sum, a.sum))

	den := new(big.Int).Mul(big.NewInt(a.n), big.NewInt(denom))
	den.Mul(den, pow10(squaredScale))

	v, _ := new(big.Rat).SetFrac(num, den).Float64()
	if a.root {
		return math.Sqrt(v), nil
	}

	return v, nil
}

func (a *decMomentAgg) Final(*sqlitedriver.FunctionContext) {}

func registerDecimalMoments() {
	for name, spec := range map[string][2]bool{
		"bq_dec_stddev_samp": {true, true}, "bq_dec_stddev_pop": {false, true}, "bq_dec_var_samp": {true, false},
		"bq_dec_var_pop": {false, false},
	} {
		sample, root := spec[0], spec[1]

		sqlitedriver.MustRegisterFunction(name, &sqlitedriver.FunctionImpl{
			NArgs: 1,
			MakeAggregate: func(sqlitedriver.FunctionContext) (sqlitedriver.AggregateFunction, error) {
				return &decMomentAgg{sum: new(big.Int), sumSq: new(big.Int), sample: sample, root: root}, nil
			},
		})
	}
}
