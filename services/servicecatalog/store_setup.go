package servicecatalog

import "github.com/blackbirdworks/gopherstack/pkgs/store"

func registerAllTables(b *InMemoryBackend) {
	r := b.registry
	b.portfolios = store.Register(r, "portfolios", store.New(func(v *Portfolio) string { return v.ID }))
	b.products = store.Register(r, "products", store.New(func(v *Product) string { return v.ID }))
	b.artifacts = store.Register(r, "artifacts", store.New(func(v *Artifact) string { return v.ID }))
	b.associations = store.Register(r, "associations", store.New(func(v *Association) string { return v.Key }))
	b.principals = store.Register(r, "principals", store.New(func(v *Principal) string { return v.Key }))
	b.constraints = store.Register(r, "constraints", store.New(func(v *Constraint) string { return v.ID }))
	b.tagOptions = store.Register(r, "tagOptions", store.New(func(v *TagOption) string { return v.ID }))
	b.tagOptionLink = store.Register(r, "tagOptionLinks", store.New(func(v *TagOptionLink) string { return v.Key }))
	b.budgetLinks = store.Register(r, "budgetLinks", store.New(func(v *BudgetLink) string { return v.Key }))
	b.actions = store.Register(r, "serviceActions", store.New(func(v *ServiceAction) string { return v.ID }))
	b.actionLinks = store.Register(
		r,
		"serviceActionLinks",
		store.New(func(v *ServiceActionLink) string { return v.Key }),
	)
	b.shares = store.Register(r, "shares", store.New(func(v *Share) string { return v.Key }))
	b.provisioned = store.Register(
		r,
		"provisionedProducts",
		store.New(func(v *ProvisionedProduct) string { return v.ID }),
	)
	b.records = store.Register(r, "records", store.New(func(v *Record) string { return v.ID }))
	b.copies = store.Register(r, "copyOperations", store.New(func(v *CopyOperation) string { return v.Token }))
	b.plans = store.Register(r, "plans", store.New(func(v *Plan) string { return v.ID }))
	b.accounts = store.Register(r, "accounts", store.New(func(v *AccountState) string { return v.AccountID }))
}
