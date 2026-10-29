package opencypher

// Access is the extra permission a query needs beyond read; every query reads.
type Access struct {
	Write  bool
	Delete bool
}

// QueryAccess classifies query by its clauses; an unparsable query needs only read (it fails when run).
func QueryAccess(query string) Access {
	q, err := parse(query)
	if q == nil || err != nil {
		return Access{}
	}

	var a Access

	for _, c := range q.clauses {
		switch c.(type) {
		case createClause, mergeClause:
			a.Write = true
		case setClause:
			a.Write, a.Delete = true, true
		case removeClause, deleteClause:
			a.Delete = true
		}
	}

	return a
}
