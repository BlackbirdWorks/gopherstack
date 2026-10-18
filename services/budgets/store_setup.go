package budgets

import "github.com/blackbirdworks/gopherstack/pkgs/store"

func budgetKey(v *BudgetRecord) string { return v.Key }
func actionKey(v *ActionRecord) string { return v.Key }

func registerAllTables(b *InMemoryBackend) {
	b.budgets = store.Register(b.registry, "budgets", store.New(budgetKey))
	b.actions = store.Register(b.registry, "actions", store.New(actionKey))
}
