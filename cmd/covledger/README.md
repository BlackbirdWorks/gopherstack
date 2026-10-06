# covledger

Queryable record of which service was audited for which bug class
(`coverage.yaml`: service x class x verdict x date x commit). It records work
performed; it detects nothing. An absent row means unknown, never clean.

```bash
go run ./cmd/covledger                        # validate + per-class summary
go run ./cmd/covledger -class wrong_wire_key  # services with no row (targets)
go run ./cmd/covledger -service iot           # one service's rows
go run ./cmd/covledger -inapplicable          # recorded refusals, with reasoning
go run ./cmd/covledger -add -service iot -class wrong_enum_value -verdict clean \
  -commit abc1234 -source commit               # append a validated row
```

Verdicts: `fixed`, `clean`, `inapplicable` (needs `-reasoning`). Add
`-subject Op.param` to record one refusal inside a pass; subject rows do not
count as class coverage. Append rows in the same commit as the fix.
Classes are the closed set in `ledger.go`.
