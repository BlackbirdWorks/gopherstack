package gcpsecretmanager

// knownLocation reports whether loc has a Secret Manager regional endpoint.
// Source: google.golang.org/api@v0.304.0 secretmanager/v1/secretmanager-api.json "endpoints".
func knownLocation(loc string) bool {
	switch loc {
	case "africa-south1", "asia-east1", "asia-east2", "asia-northeast1",
		"asia-northeast2", "asia-northeast3", "asia-south1", "asia-south2",
		"asia-southeast1", "asia-southeast2", "asia-southeast3", "australia-southeast1",
		"australia-southeast2", "ca", "ch", "eu",
		"europe-central2", "europe-north1", "europe-north2", "europe-southwest1",
		"europe-west1", "europe-west10", "europe-west12", "europe-west15",
		"europe-west2", "europe-west3", "europe-west4", "europe-west6",
		"europe-west8", "europe-west9", "in", "me-central1",
		"me-central2", "me-west1", "northamerica-northeast1", "northamerica-northeast2",
		"northamerica-south1", "sa", "southamerica-east1", "southamerica-west1",
		"us", "us-central1", "us-central2", "us-east1",
		"us-east4", "us-east5", "us-east7", "us-south1",
		"us-west1", "us-west2", "us-west3", "us-west4":
		return true
	default:
		return false
	}
}
