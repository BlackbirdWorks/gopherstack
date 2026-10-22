package gremlin

import (
	"regexp"
	"strings"
)

func isPredicateName(name string) bool {
	switch name {
	case "eq", "neq", "lt", "lte", "gt", "gte", opInside, opOutside, opBetween, opWithin, opWithout,
		opStartsWith, opEndingWith, opContaining, opNotStarts, opNotEnds, opNotContain,
		opRegex, opNotRegex:
		return true
	}

	return false
}

func newPred(op string, args []any) Pred {
	return Pred{Op: op, Args: args}
}

// bareToken maps the unqualified enum names Gremlin-Groovy scripts import statically.
func bareToken(name string) (Token, bool) {
	switch name {
	case tokID, tokLabel, tokKey, tokValue:
		return Token{Kind: kindT, Name: name}, true
	case "asc", nameDesc, "shuffle", "incr", "decr":
		return Token{Kind: kindOrder, Name: name}, true
	case nameLocal, "global":
		return Token{Kind: kindScope, Name: name}, true
	case "keys", "values":
		return Token{Kind: kindColumn, Name: name}, true
	case dirOut, dirIn, dirBoth:
		return Token{Kind: kindDirection, Name: name}, true
	case "single", cardList, cardSet:
		return Token{Kind: kindCardinality, Name: name}, true
	case "first", "last", "all", "mixed":
		return Token{Kind: kindPop, Name: name}, true
	case stepSum, "sumLong", "min", "max", "assign", opAnd, opOr, "addAll", "mult", "div":
		return Token{Kind: kindOperator, Name: name}, true
	case "none", "any":
		return Token{Kind: kindPick, Name: name}, true
	}

	return Token{}, false
}

func enumToken(kind, name string) (Token, bool) {
	t := Token{Kind: kind, Name: name}

	if names, ok := enumNames(kind); ok {
		return t, strings.Contains(names, " "+name+" ")
	}

	return Token{}, false
}

func enumNames(kind string) (string, bool) {
	switch kind {
	case kindT:
		return " id label key value ", true
	case kindOrder:
		return " asc desc shuffle incr decr ", true
	case kindDirection:
		return " OUT IN BOTH ", true
	case kindScope:
		return " local global ", true
	case kindColumn:
		return " keys values ", true
	case kindPop:
		return " first last all mixed ", true
	case kindCardinality:
		return " single list set ", true
	case kindOperator:
		return " sum sumLong min max assign and or addAll mult div minus ", true
	case kindMerge:
		return " onCreate onMatch outV inV ", true
	case kindPick:
		return " none any ", true
	}

	return "", false
}

// test evaluates the predicate against v.
func (p Pred) test(v any) bool {
	switch p.Op {
	case opAnd:
		return p.all(v)
	case opOr:
		return p.any(v)
	case "not":
		return len(p.Children) > 0 && !p.Children[0].test(v)
	}

	if text, ok := textOp(p.Op); ok {
		return testText(text, v, p.Args)
	}

	return testValue(p.Op, v, p.Args)
}

func (p Pred) all(v any) bool {
	for _, c := range p.Children {
		if !c.test(v) {
			return false
		}
	}

	return true
}

func (p Pred) any(v any) bool {
	for _, c := range p.Children {
		if c.test(v) {
			return true
		}
	}

	return false
}

func textOp(op string) (string, bool) {
	switch op {
	case opStartsWith, opEndingWith, opContaining, opNotStarts, opNotEnds, opNotContain,
		opRegex, opNotRegex:
		return op, true
	}

	return "", false
}

func testText(op string, v any, args []any) bool {
	s, ok := v.(string)
	if !ok || len(args) != 1 {
		return false
	}

	arg, ok := args[0].(string)
	if !ok {
		return false
	}

	switch op {
	case opStartsWith:
		return strings.HasPrefix(s, arg)
	case opEndingWith:
		return strings.HasSuffix(s, arg)
	case opContaining:
		return strings.Contains(s, arg)
	case opNotStarts:
		return !strings.HasPrefix(s, arg)
	case opNotEnds:
		return !strings.HasSuffix(s, arg)
	case opNotContain:
		return !strings.Contains(s, arg)
	}

	re, err := regexp.Compile(arg)
	if err != nil {
		return false
	}

	return re.MatchString(s) == (op == opRegex)
}

func testValue(op string, v any, args []any) bool {
	switch op {
	case opWithin:
		return inList(v, flattenArgs(args))
	case opWithout:
		return !inList(v, flattenArgs(args))
	case opBetween, opInside, opOutside:
		return testRange(op, v, args)
	}

	if len(args) != 1 {
		return false
	}

	return testCompare(op, v, args[0])
}

func flattenArgs(args []any) []any {
	if len(args) == 1 {
		switch t := args[0].(type) {
		case []any:
			return t
		case Set:
			return t
		}
	}

	return args
}

func inList(v any, list []any) bool {
	for _, e := range list {
		if equalValues(v, e) {
			return true
		}
	}

	return false
}

func testCompare(op string, v, arg any) bool {
	switch op {
	case "eq":
		return equalValues(v, arg)
	case "neq":
		return !equalValues(v, arg)
	}

	c, ok := compareValues(v, arg)
	if !ok {
		return false
	}

	switch op {
	case "lt":
		return c < 0
	case "lte":
		return c <= 0
	case "gt":
		return c > 0
	case "gte":
		return c >= 0
	}

	return false
}

func testRange(op string, v any, args []any) bool {
	if len(args) != 2 { //nolint:mnd // low and high bounds
		return false
	}

	lo, lok := compareValues(v, args[0])
	hi, hok := compareValues(v, args[1])

	if !lok || !hok {
		return false
	}

	switch op {
	case opBetween:
		return lo >= 0 && hi < 0
	case opInside:
		return lo > 0 && hi < 0
	}

	return lo < 0 || hi > 0
}
