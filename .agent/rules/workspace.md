---
trigger: always_on
---

 - All code must pass `make lint`
 - Errors should be sentinel errors
 - logging must be via slog
 - Avoid break statements. Any break can be a another function with a fast return 
 - Avoid anonymous structs
 - break common functionality to packages under /pkgs
 - write idiomatic go
 - All service operations must have a metric recorded 
 - All service operations must have extensive unit tests
 - All service operations must have integration tests using the go aws sdk v2
