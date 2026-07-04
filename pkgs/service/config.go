package service

import "github.com/blackbirdworks/gopherstack/pkgs/config"

// AccountRegion returns the configured AWS account ID and region from the
// AppContext's Config when it implements config.Provider, or ("", "") otherwise.
//
// It collapses the boilerplate every service provider repeats:
//
//	if cp, ok := ctx.Config.(config.Provider); ok {
//		cfg := cp.GetGlobalConfig()
//		accountID = cfg.GetAccountID()
//		region = cfg.GetRegion()
//	}
//
// The empty fallback matches that hand-rolled behaviour exactly, so backends
// keep applying their own defaults when no config is wired.
func AccountRegion(ctx *AppContext) (string, string) {
	if cp, ok := ctx.Config.(config.Provider); ok {
		cfg := cp.GetGlobalConfig()

		return cfg.GetAccountID(), cfg.GetRegion()
	}

	return "", ""
}

// AccountRegionOrDefault is like AccountRegion but falls back to
// config.DefaultAccountID and config.DefaultRegion when no config.Provider is
// present. It collapses the most common provider idiom:
//
//	accountID := config.DefaultAccountID
//	region := config.DefaultRegion
//	if cp, ok := ctx.Config.(config.Provider); ok {
//		cfg := cp.GetGlobalConfig()
//		accountID = cfg.GetAccountID()
//		region = cfg.GetRegion()
//	}
func AccountRegionOrDefault(ctx *AppContext) (string, string) {
	if cp, ok := ctx.Config.(config.Provider); ok {
		cfg := cp.GetGlobalConfig()

		return cfg.GetAccountID(), cfg.GetRegion()
	}

	return config.DefaultAccountID, config.DefaultRegion
}
