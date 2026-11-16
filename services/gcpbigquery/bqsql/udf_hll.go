package bqsql

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"slices"
	"strings"

	sqlitedriver "modernc.org/sqlite"
)

// HLL_COUNT.* use a HyperLogLog++ style sketch (sparse mode at 25 bits, then dense registers). The byte layout is the
// emulator's own: it is not byte-compatible with BigQuery's serialised sketches.
const (
	hllMinPrecision                                                       = 10
	hllMaxPrecision                                                       = 24
	hllDefaultPrec                                                        = 15
	hllSparsePrecision                                                    = 25
	hllMagic                                                              = "GSHLL1"
	hllHeaderLen                                                          = len(hllMagic) + 3
	hllModeSparse                                                         = 0
	hllModeDense                                                          = 1
	hllTagInt, hllTagNumeric, hllTagBigNumeric, hllTagString, hllTagBytes = 1, 2, 3, 4, 5
	sparseDiv                                                             = 2
	hashBits                                                              = 64
	wordBits                                                              = 32
	hllAlpha1                                                             = 0.7213
	hllAlpha2                                                             = 1.079
)

func hllTypeTag(name string) byte {
	tags := map[string]byte{"INT64": hllTagInt, "NUMERIC": hllTagNumeric, "BIGNUMERIC": hllTagBigNumeric,
		litString: hllTagString, "BYTES": hllTagBytes,
	}

	return tags[strings.ToUpper(name)]
}

type hllSketch struct {
	sparse map[uint32]uint8
	dense  []uint8
	tag    byte
	prec   uint8
}

func newHLL(tag, prec byte) *hllSketch {
	return &hllSketch{tag: tag, prec: prec, sparse: map[uint32]uint8{}}
}

func (s *hllSketch) sparseLimit() int { return 1 << (s.prec - sparseDiv) }

func hllHash(v driver.Value) uint64 {
	switch t := v.(type) {
	case int64:
		var b [8]byte

		binary.LittleEndian.PutUint64(b[:], uint64(t)) //nolint:gosec // bit pattern

		return farmFingerprint64(b[:])
	case float64:
		return farmFingerprint64([]byte(fmt.Sprintf("%v", t)))
	}

	return farmFingerprint64(bytesOf(v))
}

func (s *hllSketch) add(h uint64) {
	idx := uint32(h >> (hashBits - hllSparsePrecision))
	rest := h<<hllSparsePrecision | 1<<(hllSparsePrecision-1)
	rho := uint8(bits.LeadingZeros64(rest)) + 1 //nolint:gosec // at most 40

	if s.dense != nil {
		s.denseSet(idx, rho)

		return
	}

	if old, ok := s.sparse[idx]; !ok || rho > old {
		s.sparse[idx] = rho
	}

	if len(s.sparse) > s.sparseLimit() {
		s.toDense()
	}
}

// denseIndex converts a sparse (idx25, rho25) pair to the register index and rank at the sketch precision.
func denseIndex(idx uint32, rho, prec uint8) (uint32, uint8) {
	shift := hllSparsePrecision - prec
	low := idx & (1<<shift - 1)
	reg := idx >> shift

	if low != 0 {
		return reg, uint8(bits.LeadingZeros32(low<<(wordBits-shift))) + 1 //nolint:gosec // at most 25
	}

	return reg, shift + rho
}

func (s *hllSketch) denseSet(idx uint32, rho uint8) {
	reg, r := denseIndex(idx, rho, s.prec)
	if r > s.dense[reg] {
		s.dense[reg] = r
	}
}

func (s *hllSketch) toDense() {
	if s.dense != nil {
		return
	}

	s.dense = make([]uint8, 1<<s.prec)

	for idx, rho := range s.sparse {
		s.denseSet(idx, rho)
	}

	s.sparse = nil
}

// downgrade lowers the precision of dense registers to prec.
func (s *hllSketch) downgrade(prec uint8) {
	if prec >= s.prec {
		return
	}

	if s.dense == nil {
		s.prec = prec

		return
	}

	shift := s.prec - prec
	out := make([]uint8, 1<<prec)

	for j, r := range s.dense {
		if r == 0 {
			continue
		}

		low := uint32(j) & (1<<shift - 1)
		nr := r + shift

		if low != 0 {
			nr = uint8(bits.LeadingZeros32(low<<(wordBits-shift))) + 1 //nolint:gosec // at most 14
		}

		if reg := uint32(j) >> shift; nr > out[reg] {
			out[reg] = nr
		}
	}

	s.dense, s.prec = out, prec
}

func (s *hllSketch) merge(o *hllSketch) {
	if o.prec < s.prec {
		s.downgrade(o.prec)
	}

	if o.dense == nil && s.dense == nil {
		for idx, rho := range o.sparse {
			if old, ok := s.sparse[idx]; !ok || rho > old {
				s.sparse[idx] = rho
			}
		}

		if len(s.sparse) > s.sparseLimit() {
			s.toDense()
		}

		return
	}

	s.toDense()

	if o.dense == nil {
		for idx, rho := range o.sparse {
			s.denseSet(idx, rho)
		}

		return
	}

	cp := &hllSketch{dense: slices.Clone(o.dense), prec: o.prec, tag: o.tag}
	cp.downgrade(s.prec)

	for j, r := range cp.dense {
		if r > s.dense[j] {
			s.dense[j] = r
		}
	}
}

func (s *hllSketch) estimate() int64 {
	if s.dense == nil {
		m := float64(uint64(1) << hllSparsePrecision)
		v := m - float64(len(s.sparse))

		return int64(math.Round(m * math.Log(m/v)))
	}

	m := float64(len(s.dense))
	sum, zeros := 0.0, 0

	for _, r := range s.dense {
		sum += math.Ldexp(1, -int(r))

		if r == 0 {
			zeros++
		}
	}

	alpha := hllAlpha1 / (1 + hllAlpha2/m)
	est := alpha * m * m / sum

	if est <= 2.5*m && zeros > 0 {
		est = m * math.Log(m/float64(zeros))
	}

	return int64(math.Round(est))
}

func (s *hllSketch) marshal() []byte {
	out := append([]byte(hllMagic), s.tag, s.prec, hllModeSparse)

	if s.dense != nil {
		out[len(out)-1] = hllModeDense

		return append(out, s.dense...)
	}

	idxs := make([]uint32, 0, len(s.sparse))
	for idx := range s.sparse {
		idxs = append(idxs, idx)
	}

	slices.Sort(idxs)

	for _, idx := range idxs {
		out = binary.AppendUvarint(out, uint64(idx))
		out = append(out, s.sparse[idx])
	}

	return out
}

func unmarshalHLL(b []byte) (*hllSketch, error) {
	if len(b) < hllHeaderLen || string(b[:len(hllMagic)]) != hllMagic {
		return nil, errBQ("The input is not a valid HLL++ sketch")
	}

	s := &hllSketch{tag: b[len(hllMagic)], prec: b[len(hllMagic)+1]}
	body := b[hllHeaderLen:]

	if s.prec < hllMinPrecision || s.prec > hllMaxPrecision {
		return nil, errBQ("The input is not a valid HLL++ sketch")
	}

	if b[hllHeaderLen-1] == hllModeDense {
		if len(body) != 1<<s.prec {
			return nil, errBQ("The input is not a valid HLL++ sketch")
		}

		s.dense = slices.Clone(body)

		return s, nil
	}

	s.sparse = map[uint32]uint8{}

	for len(body) > 0 {
		idx, n := binary.Uvarint(body)
		if n <= 0 || n >= len(body) {
			return nil, errBQ("The input is not a valid HLL++ sketch")
		}

		s.sparse[uint32(idx)] = body[n] //nolint:gosec // 25 bit index
		body = body[n+1:]
	}

	return s, nil
}

type hllInitAgg struct{ sk *hllSketch }

func (a *hllInitAgg) Step(_ *sqlitedriver.FunctionContext, args []driver.Value) error {
	if args[0] == nil {
		return nil
	}

	if a.sk == nil {
		prec, err := toInt(args[1])
		if err != nil || prec < hllMinPrecision || prec > hllMaxPrecision {
			return errBQ("HLL_COUNT.INIT precision must be between %d and %d", hllMinPrecision, hllMaxPrecision)
		}

		a.sk = newHLL(hllTypeTag(toStr(args[2])), byte(prec))
	}

	a.sk.add(hllHash(args[0]))

	return nil
}

func (a *hllInitAgg) WindowInverse(*sqlitedriver.FunctionContext, []driver.Value) error {
	return errNoWindow
}

func (a *hllInitAgg) WindowValue(*sqlitedriver.FunctionContext) (driver.Value, error) {
	if a.sk == nil {
		return nil, nil //nolint:nilnil // no input rows
	}

	return a.sk.marshal(), nil
}

func (a *hllInitAgg) Final(*sqlitedriver.FunctionContext) {}

type hllMergeAgg struct {
	acc     *hllSketch
	partial bool
}

func (a *hllMergeAgg) Step(_ *sqlitedriver.FunctionContext, args []driver.Value) error {
	if args[0] == nil {
		return nil
	}

	sk, err := unmarshalHLL(bytesOf(args[0]))
	if err != nil {
		return err
	}

	if a.acc == nil {
		a.acc = sk

		return nil
	}

	if a.acc.tag != sk.tag {
		return errBQ("Cannot merge HLL++ sketches initialized on different types")
	}

	a.acc.merge(sk)

	return nil
}

func (a *hllMergeAgg) WindowInverse(*sqlitedriver.FunctionContext, []driver.Value) error {
	return errNoWindow
}

func (a *hllMergeAgg) WindowValue(*sqlitedriver.FunctionContext) (driver.Value, error) {
	if a.partial {
		if a.acc == nil {
			return nil, nil //nolint:nilnil // no input rows
		}

		return a.acc.marshal(), nil
	}

	if a.acc == nil {
		return int64(0), nil
	}

	return a.acc.estimate(), nil
}

func (a *hllMergeAgg) Final(*sqlitedriver.FunctionContext) {}

func udfHLLExtract(a []driver.Value) (driver.Value, error) {
	if a[0] == nil {
		return int64(0), nil
	}

	sk, err := unmarshalHLL(bytesOf(a[0]))
	if err != nil {
		return nil, err
	}

	return sk.estimate(), nil
}

func addHLLUDFs(d map[string]udfDef) {
	d["hll_extract"] = udfDef{n: 1, fn: udfHLLExtract}
}

func registerHLLAggregates() {
	sqlitedriver.MustRegisterFunction("bq_hll_init", &sqlitedriver.FunctionImpl{
		NArgs: argc3,
		MakeAggregate: func(sqlitedriver.FunctionContext) (sqlitedriver.AggregateFunction, error) {
			return &hllInitAgg{}, nil
		},
	})

	for name, partial := range map[string]bool{"bq_hll_merge": false, "bq_hll_merge_partial": true} {
		sqlitedriver.MustRegisterFunction(name, &sqlitedriver.FunctionImpl{
			NArgs: 1,
			MakeAggregate: func(sqlitedriver.FunctionContext) (sqlitedriver.AggregateFunction, error) {
				return &hllMergeAgg{partial: partial}, nil
			},
		})
	}
}

func hllFuncs(t map[string]*fnDef) {
	t["HLL_COUNT.EXTRACT"] = def("hll_extract", 1, 1, fixed(KindInt))
	t["HLL_COUNT.MERGE"] = custom(1, 1, genHLLMerge("bq_hll_merge", KindInt))
	t["HLL_COUNT.MERGE_PARTIAL"] = custom(1, 1, genHLLMerge("bq_hll_merge_partial", KindBytes))
	t["HLL_COUNT.INIT"] = custom(1, argc2, genHLLInit)
}

func genHLLMerge(fname string, ret Kind) func(c *compiler, n *node, sc *scope) (*gx, error) {
	return func(c *compiler, n *node, sc *scope) (*gx, error) {
		a, err := c.args(n, sc)
		if err != nil {
			return nil, err
		}

		if a[0].typ.Kind != KindBytes && a[0].typ.Kind != KindUnknown {
			return nil, invalid("%s requires a BYTES sketch", n.name)
		}

		sql, err := c.aggSQL(n, sc, fname, []string{a[0].sql}, "")
		if err != nil {
			return nil, err
		}

		return &gx{sql: sql, typ: scalar(ret)}, nil
	}
}

func genHLLInit(c *compiler, n *node, sc *scope) (*gx, error) {
	a, err := c.args(n, sc)
	if err != nil {
		return nil, err
	}

	var tag string

	switch a[0].typ.Kind {
	case KindInt:
		tag = "INT64"
	case KindNumeric:
		tag = "NUMERIC"
	case KindBigNumeric:
		tag = "BIGNUMERIC"
	case KindString:
		tag = litString
	case KindBytes:
		tag = "BYTES"
	default:
		return nil, invalid("HLL_COUNT.INIT does not support input of type %s", a[0].typ.Kind.SQLName())
	}

	prec := "15"
	if len(a) > 1 {
		prec = a[1].sql
	}

	sql, err := c.aggSQL(n, sc, "bq_hll_init", []string{a[0].sql, prec, quoteStr(tag)}, "")
	if err != nil {
		return nil, err
	}

	return &gx{sql: sql, typ: scalar(KindBytes)}, nil
}
