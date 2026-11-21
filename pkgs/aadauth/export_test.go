package aadauth

// KeyGenerated reports whether the RSA key has been generated yet.
func (iss *Issuer) KeyGenerated() bool { return iss.generated.Load() }
