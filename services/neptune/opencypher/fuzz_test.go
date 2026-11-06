package opencypher_test

import (
	"context"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

func FuzzExecute(f *testing.F) {
	for _, s := range []string{
		"MATCH (n) RETURN n",
		"CREATE (a:P {n:1})-[:K]->(b:P) RETURN a, b",
		"MATCH (a)-[r*1..3]->(b) WHERE a.n > $x RETURN count(*), collect(b.n) ORDER BY a.n SKIP 1 LIMIT 2",
		"UNWIND [1,2,3] AS x WITH x, x*2 AS y RETURN [i IN range(0,y) WHERE i%2=0 | i]",
		"MERGE (n:P {n:1}) ON CREATE SET n.c=1 ON MATCH SET n.m=1 REMOVE n.z DELETE n",
		"RETURN substring('abc',1), size([]), head([]), toInteger('x'), {a:1}.a, [1,2][5]",
	} {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, src string) {
		g := opencypher.NewGraph()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

		defer cancel()

		_, _ = g.Execute(ctx, "CREATE (a:P {n:1})-[:K]->(b:P {n:2})", nil)
		_, _ = g.Execute(ctx, src, map[string]any{"x": 1})
	})
}
