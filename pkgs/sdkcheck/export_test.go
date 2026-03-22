package sdkcheck

// This file exports internal symbols for use by the external test package (sdkcheck_test).
// It is compiled only during testing.

// BuildSDKMethodSet exposes buildSDKMethodSet for testing.
func BuildSDKMethodSet(sdkClientPtr any) map[string]bool {
	return buildSDKMethodSet(sdkClientPtr)
}

// BuildSet exposes buildSet for testing.
func BuildSet(items []string) (map[string]bool, []string) {
	return buildSet(items)
}

// FindStale exposes findStale for testing.
func FindStale(notImplSet, sdkMethods map[string]bool) []string {
	return findStale(notImplSet, sdkMethods)
}

// FindOverlapping exposes findOverlapping for testing.
func FindOverlapping(a, b map[string]bool) []string {
	return findOverlapping(a, b)
}

// FindUnaccounted exposes findUnaccounted for testing.
func FindUnaccounted(sdkMethods, supportedSet, notImplSet map[string]bool) []string {
	return findUnaccounted(sdkMethods, supportedSet, notImplSet)
}
