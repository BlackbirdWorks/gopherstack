package athena

import (
	"context"
	"sort"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/iceberg"
	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
)

const (
	propRewriteDataThreshold   = "optimize_rewrite_data_file_threshold"
	propRewriteDeleteThreshold = "optimize_rewrite_delete_file_threshold"
	defaultDataThreshold       = 5
	defaultDeleteThreshold     = 2
	defaultTargetFileBytes     = 512 << 20
	minFileRatioNum            = 3
	minFileRatioDen            = 4
	maxFileRatioNum            = 9
	maxFileRatioDen            = 5
)

func intProp(props map[string]string, key string, def int) int {
	if n, err := strconv.Atoi(props[key]); err == nil && n > 0 {
		return n
	}

	return def
}

// deleteRefs maps each position delete file to the data files it names.
func (l *tableLoader) deleteRefs(ctx context.Context, dels []fileEntry) (map[string]map[string]bool, error) {
	out := make(map[string]map[string]bool, len(dels))

	for _, d := range dels {
		data, err := l.readURI(ctx, d.entry.DataFile.Path)
		if err != nil {
			return nil, icebergFailf("ICEBERG_CANNOT_OPEN_SPLIT", "Cannot open delete file %s", d.entry.DataFile.Path)
		}

		pf, err := parquetio.Open(data)
		if err != nil {
			return nil, icebergFailf(
				"ICEBERG_CANNOT_OPEN_SPLIT",
				"Cannot open delete file %s: %v",
				d.entry.DataFile.Path,
				err,
			)
		}

		refs := map[string]bool{}

		err = pf.ForEach([]string{colFilePath}, func(vals []any) error {
			if s, ok := vals[0].(string); ok {
				refs[s] = true
			}

			return nil
		})
		if err != nil {
			return nil, icebergFailf(
				"ICEBERG_CURSOR_ERROR",
				"Cannot read delete file %s: %v",
				d.entry.DataFile.Path,
				err,
			)
		}

		out[d.entry.DataFile.Path] = refs
	}

	return out, nil
}

type optGroup struct {
	files   []string
	rows    []int
	deletes bool
}

type optInputs struct {
	refs    map[string]map[string]bool
	byFile  map[string][]int
	matched map[string]bool
	files   snapshotFiles
}

func specKey(spec int, part map[string]any) string {
	return strconv.Itoa(spec) + "|" + partitionKey(part)
}

func (p *writePlan) optimizeInputs(
	ctx context.Context, l *tableLoader, res *sqlResult, snap iceberg.Snapshot,
) (*optInputs, error) {
	files, err := l.snapshotFiles(ctx, snap)
	if err != nil {
		return nil, err
	}

	refs, err := l.deleteRefs(ctx, files.deletes)
	if err != nil {
		return nil, err
	}

	in := &optInputs{files: files, refs: refs, byFile: map[string][]int{}, matched: map[string]bool{}}

	for r, row := range res.rows {
		in.byFile[row[0]] = append(in.byFile[row[0]], r)

		if v := row[dmlHidden]; v == "1" || v == strTrue {
			in.matched[row[0]] = true
		}
	}

	return in, nil
}

// groups buckets the data files worth rewriting by partition.
func (in *optInputs) groups(props map[string]string) ([]string, map[string]*optGroup) {
	delThr := intProp(props, propRewriteDeleteThreshold, defaultDeleteThreshold)
	minSize := int64(defaultTargetFileBytes * minFileRatioNum / minFileRatioDen)
	maxSize := int64(defaultTargetFileBytes * maxFileRatioNum / maxFileRatioDen)
	matchedParts := map[string]bool{}

	for _, f := range in.files.data {
		if in.matched[f.entry.DataFile.Path] {
			matchedParts[specKey(f.spec, f.entry.DataFile.Partition)] = true
		}
	}

	groups := map[string]*optGroup{}

	var keys []string

	for _, f := range in.files.data {
		df := f.entry.DataFile
		key := specKey(f.spec, df.Partition)
		inScope := in.matched[df.Path] || (len(in.byFile[df.Path]) == 0 && matchedParts[key])
		heavy := in.deleteCount(df.Path) >= delThr

		if !inScope || (df.FileSize >= minSize && df.FileSize <= maxSize && !heavy) {
			continue
		}

		g, found := groups[key]
		if !found {
			g = &optGroup{}
			groups[key], keys = g, append(keys, key)
		}

		g.files = append(g.files, df.Path)
		g.rows = append(g.rows, in.byFile[df.Path]...)
		g.deletes = g.deletes || heavy
	}

	sort.Strings(keys)

	return keys, groups
}

func (in *optInputs) deleteCount(dataPath string) int {
	n := 0

	for _, r := range in.refs {
		if r[dataPath] {
			n++
		}
	}

	return n
}

func (in *optInputs) change(props map[string]string) (iceberg.Change, []int) {
	dataThr := intProp(props, propRewriteDataThreshold, defaultDataThreshold)
	ch := iceberg.Change{Operation: iceberg.OpReplace, RemoveData: map[string]bool{}, RemoveDeletes: map[string]bool{}}

	var rewrite []int

	keys, groups := in.groups(props)

	for _, k := range keys {
		g := groups[k]
		if len(g.files) < dataThr && !g.deletes {
			continue
		}

		for _, f := range g.files {
			ch.RemoveData[f] = true
		}

		rewrite = append(rewrite, g.rows...)
	}

	for dp, r := range in.refs {
		if len(r) > 0 && allRemoved(r, ch.RemoveData) {
			ch.RemoveDeletes[dp] = true
		}
	}

	sort.Ints(rewrite)

	return ch, rewrite
}

func allRemoved(refs, removed map[string]bool) bool {
	for ref := range refs {
		if !removed[ref] {
			return false
		}
	}

	return true
}

func (p *writePlan) applyOptimize(
	ctx context.Context, l *tableLoader, res *sqlResult, limits EngineLimits,
) statementOutcome {
	snap, ok := p.ice.meta.CurrentSnapshot()
	if !ok {
		return stmtOK()
	}

	in, err := p.optimizeInputs(ctx, l, res, snap)
	if err != nil {
		return engineFailure(err)
	}

	ch, rewrite := in.change(p.ice.meta.Properties)
	if len(ch.RemoveData) == 0 {
		return stmtOK()
	}

	if len(rewrite) > 0 {
		added := p.sliceRows(res, rewrite, dmlHidden+1)

		src := make([]int, len(added.columns))
		for i := range src {
			src[i] = i
		}

		newFiles, out := p.writeIcebergFiles(ctx, added, src, limits)
		if out.failed {
			return out
		}

		ch.AddData = newFiles
	}

	return p.commitIcebergChange(ctx, ch, false)
}
