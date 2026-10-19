package bedrockagentcorecontrol

import "github.com/blackbirdworks/gopherstack/pkgs/restroute"

func notFound(format string, args ...any) error { return restroute.NotFound(format, args...) }

func conflict(format string, args ...any) error { return restroute.Conflict(format, args...) }

func invalid(format string, args ...any) error { return restroute.Validation(format, args...) }
