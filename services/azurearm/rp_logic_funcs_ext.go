package azurearm

import (
	"encoding/base64"
	"unicode"
)

func addLogicExtFuncs(m map[string]logicFunc) {
	m["slice"] = eagerLogic("slice", argsTwo, argsThree, logicSlice)
	m["nthindexof"] = eagerLogic("nthIndexOf", argsThree, argsThree, logicNthIndexOf)
	m["binary"] = eagerLogic("binary", 1, 1, func(a []any) (any, error) {
		return base64.StdEncoding.EncodeToString([]byte(logicString(a[0]))), nil
	})
	m["decodedatauri"] = eagerLogic("decodeDataUri", 1, 1, logicDataURIToString)
	m["xml"] = eagerLogic("xml", 1, 1, logicXML)
	m["xpath"] = eagerLogic("xpath", argsTwo, argsTwo, logicXPath)

	addLogicZoneFuncs(m)
}

// logicSliceBound resolves a possibly negative index against length n, clamped to [0, n].
func logicSliceBound(i int64, n int) int {
	if i < 0 {
		i += int64(n)
	}

	return int(max(0, min(i, int64(n))))
}

func logicSlice(a []any) (any, error) {
	s, err := logicArgString("slice", a[0])
	if err != nil {
		return nil, err
	}

	start, ok := logicInt(a[1])
	if !ok {
		return nil, exprErr("slice expects an integer start index")
	}

	runes := []rune(s)
	from, to := logicSliceBound(start, len(runes)), len(runes)

	if len(a) == argsThree {
		end, endOK := logicInt(a[2])
		if !endOK {
			return nil, exprErr("slice expects an integer end index")
		}

		to = logicSliceBound(end, len(runes))
	}

	if from >= to {
		return "", nil
	}

	return string(runes[from:to]), nil
}

func foldRunes(s string) []rune {
	r := []rune(s)
	for i, c := range r {
		r[i] = unicode.ToLower(c)
	}

	return r
}

func runesAt(hay, needle []rune, at int) bool {
	if at < 0 || at+len(needle) > len(hay) {
		return false
	}

	for i, c := range needle {
		if hay[at+i] != c {
			return false
		}
	}

	return true
}

// logicNthIndexOf counts non-overlapping matches from the start, or from the end for a negative occurrence.
// The reference does not state case handling; it follows indexOf and lastIndexOf (ignore case).
func logicNthIndexOf(a []any) (any, error) {
	hay, needle := foldRunes(logicString(a[0])), foldRunes(logicString(a[1]))

	occ, ok := logicInt(a[2])
	if !ok || occ == 0 {
		return nil, exprErr("the occurrence of nthIndexOf must be a non-zero integer")
	}

	if len(needle) == 0 {
		return int64(-1), nil
	}

	count := int64(0)

	if occ > 0 {
		for i := 0; i+len(needle) <= len(hay); i++ {
			if runesAt(hay, needle, i) {
				count++

				if count == occ {
					return int64(i), nil
				}

				i += len(needle) - 1
			}
		}

		return int64(-1), nil
	}

	for i := len(hay) - len(needle); i >= 0; i-- {
		if runesAt(hay, needle, i) {
			count--

			if count == occ {
				return int64(i), nil
			}

			i -= len(needle) - 1
		}
	}

	return int64(-1), nil
}
