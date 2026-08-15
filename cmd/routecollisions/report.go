package main

import (
	"fmt"
	"os"
	"sort"
	"strings"
)

// literalsOverlap reports whether a claim evaluated FIRST (winner) would
// intercept a request meant for a claim evaluated LATER (loser): a prefix
// winner shadows any loser literal it is a string-prefix of (or an
// identical literal); an exact winner only shadows an identical literal
// (an exact match can never swallow a longer loser path).
func literalsOverlap(winner, loser claim) bool {
	if winner.Kind == kindPrefix {
		return strings.HasPrefix(loser.Literal, winner.Literal) || winner.Literal == loser.Literal
	}

	return winner.Literal == loser.Literal
}

type overlapPair struct {
	WinnerC claim
	LoserC  claim
	Winner  svcInfo
	Loser   svcInfo
}

func printCollisionReport(results []svcInfo) {
	pairs := findOverlapPairs(results)
	sortOverlapPairs(pairs)
	renderOverlapPairs(results, pairs)
}

// findOverlapPairs walks every pair of services in router evaluation order
// (results is pre-sorted priority desc, then registration order asc, so for
// any i < j, results[i] is evaluated strictly before results[j]) and
// collects every claim pair whose literals overlap.
func findOverlapPairs(results []svcInfo) []overlapPair {
	var pairs []overlapPair

	for i, a := range results {
		for _, b := range results[i+1:] {
			pairs = append(pairs, overlapPairsBetween(a, b)...)
		}
	}

	return pairs
}

func overlapPairsBetween(a, b svcInfo) []overlapPair {
	var pairs []overlapPair

	for _, ac := range a.Claims {
		for _, bc := range b.Claims {
			if ac.Segment != bc.Segment {
				continue
			}

			if literalsOverlap(ac, bc) {
				pairs = append(pairs, overlapPair{Winner: a, WinnerC: ac, Loser: b, LoserC: bc})
			}
		}
	}

	return pairs
}

func sortOverlapPairs(pairs []overlapPair) {
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].Winner.Guarded != pairs[j].Winner.Guarded {
			return !pairs[i].Winner.Guarded // unguarded winners first (higher risk)
		}

		if pairs[i].WinnerC.Segment != pairs[j].WinnerC.Segment {
			return pairs[i].WinnerC.Segment < pairs[j].WinnerC.Segment
		}

		return pairs[i].Loser.Dir < pairs[j].Loser.Dir
	})
}

func renderOverlapPairs(results []svcInfo, pairs []overlapPair) {
	fmt.Fprintf(
		os.Stdout,
		"%d services have a RouteMatcher with extracted path claims; %d literal-overlap candidate pairs found\n\n",
		len(results),
		len(pairs),
	)

	for _, p := range pairs {
		fmt.Fprintf(os.Stdout, "%-24s [%s %-20q prio=%d reg=%d] shadows %-24s [%s %-20q prio=%d reg=%d]  (%s)\n",
			p.Winner.Dir, p.WinnerC.KindStr, p.WinnerC.Literal, p.Winner.Priority, p.Winner.RegOrder,
			p.Loser.Dir, p.LoserC.KindStr, p.LoserC.Literal, p.Loser.Priority, p.Loser.RegOrder,
			riskLabel(p))
	}
}

func riskLabel(p overlapPair) string {
	risk := "guarded"
	if !p.Winner.Guarded {
		risk = "UNGUARDED-WINNER"
	}

	lguard := "unguarded"
	if p.Loser.Guarded {
		lguard = "guarded"
	}

	return risk + "/" + lguard
}
