package azuremonitor

import "github.com/google/uuid"

func newUUID() string { return uuid.NewString() }
