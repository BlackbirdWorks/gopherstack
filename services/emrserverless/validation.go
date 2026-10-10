package emrserverless

import "regexp"

const maxApplicationNameLen = 64

var resourceNameRe = regexp.MustCompile(`^[A-Za-z0-9._/#-]{1,64}$`)
