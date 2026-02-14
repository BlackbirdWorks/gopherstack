---
trigger: always_on
---

## Testing
- Tests should always be table tests.
- Tests should always be parallel unless a environment variable is involved
- Test should always cover at least 85% of the logic. 
- `make test` to run all the unit tests
- `make integration-test` to run all the int tests
- use t.Context() in tests
- never use t.Fatal or t.Error, only use require and asserts from testify