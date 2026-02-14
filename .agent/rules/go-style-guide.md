---
trigger: always_on
---

 - All code must pass `make lint`
 - Errors should be sentinel errors
 - logging must be via slog
 - Avoid break statements. Any break can be a another function with a fast return 
 - Avoid anonymous structs
 - break common functionality to packages under /pkgs