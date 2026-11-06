package gremlin

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

func TestStepsMalformedArgsNeverPanic(t *testing.T) {
	t.Parallel()

	argForms := map[string]string{
		"none":      "",
		"string":    `"x"`,
		"int":       `1`,
		"float":     `1.5`,
		"bool":      `true`,
		"traversal": `__.out()`,
		"many":      `"a", 1, "b", 2, "c", 3`,
		"nested":    `__.out(), __.in(), __.values("n")`,
		"mixed":     `1, "x", __.out()`,
		"neg":       `-1`,
	}
	prefixes := []string{
		"g.V().", "g.E().", "g.V().out().", "g.",
		"g.V().order().", "g.V().group().", "g.V().groupCount().", "g.V().project('a').",
		"g.V().choose(__.out()).", "g.V().repeat(__.out()).", "g.V().addE('k').", "g.V().addV('q').",
		"g.V().select('a').", "g.V().path().", "g.V().tree().", "g.V().sack().", "g.V().union(__.out()).",
		"g.V().local(__.out()).", "g.V().repeat(__.out()).", "g.V().emit().", "g.V().until(__.out()).", "g.V().mergeV([:]).", "g.V().math('_+1').", "g.V().fold().", "g.V().coalesce(__.out()).",
	}

	names := make([]string, 0)
	for name := range buildSteps() {
		names = append(names, name)
	}

	for _, mod := range []string{"by", "from", "to", modTimes, modUntil, modEmit, "option", "with", "read", "write", "property"} {
		if _, dup := buildSteps()[mod]; !dup {
			names = append(names, mod)
		}
	}

	sort.Strings(names)
	require.NotEmpty(t, names)

	for _, name := range names {
		for form, args := range argForms {
			for _, prefix := range prefixes {
				script := fmt.Sprintf("%s%s(%s)", prefix, name, args)

				t.Run(name+"/"+form+"/"+prefix, func(t *testing.T) {
					t.Parallel()

					g := opencypher.NewGraph()
					e := NewEngine()
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

					defer cancel()

					_, err := e.Eval(ctx, g, `g.addV("p").property("n",1).as("a").addV("p").property("n",2).addE("k").from("a")`, nil)
					require.NoError(t, err)

					require.NotPanics(t, func() { _, _ = e.Eval(ctx, g, script, nil) }, script)
				})
			}
		}
	}
}

func FuzzEval(f *testing.F) {
	for _, s := range []string{
		`g.V().out().values("n").fold()`,
		`g.V().repeat(__.out()).times(2).emit().path().by("n")`,
		`g.V().group().by("n").by(__.count())`,
		`g.addV("p").property("n",1).as("a").addE("k").to("a")`,
		`g.V().has("n", P.gt(1)).order().by("n", Order.desc).limit(1).sample(1)`,
		`g.V().math("_+1").by("n")`,
		`g.V().choose(__.out(), __.in(), __.both()).tail(1).range(0,1)`,
		`g.V().project("a","b").by("n").by(__.out()).select("a").tree()`,
	} {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, src string) {
		g := opencypher.NewGraph()
		e := NewEngine()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

		defer cancel()

		_, _ = e.Eval(ctx, g, `g.addV("p").property("n",1).as("a").addV("p").property("n",2).addE("k").from("a")`, nil)
		_, _ = e.Eval(ctx, g, src, nil)
	})
}
