package datasync

import "fmt"

// cmkSecretConfigWire mirrors CmkSecretConfig (SecretArn, KmsKeyArn) on the
// wire; used identically for CreateLocation*/UpdateLocation* input parsing
// and DescribeLocation* output serialization (botocore datasync 2018-11-09
// model, shape CmkSecretConfig).
type cmkSecretConfigWire struct {
	SecretArn string `json:"SecretArn,omitempty"`
	KmsKeyArn string `json:"KmsKeyArn,omitempty"`
}

// customSecretConfigWire mirrors CustomSecretConfig (SecretArn,
// SecretAccessRoleArn) on the wire (botocore datasync 2018-11-09 model,
// shape CustomSecretConfig).
type customSecretConfigWire struct {
	SecretArn           string `json:"SecretArn,omitempty"`
	SecretAccessRoleArn string `json:"SecretAccessRoleArn,omitempty"`
}

func cmkSecretConfigFromWire(w *cmkSecretConfigWire) *CmkSecretConfig {
	if w == nil {
		return nil
	}

	return &CmkSecretConfig{SecretArn: w.SecretArn, KmsKeyArn: w.KmsKeyArn}
}

func cmkSecretConfigToWire(c *CmkSecretConfig) *cmkSecretConfigWire {
	if c == nil {
		return nil
	}

	return &cmkSecretConfigWire{SecretArn: c.SecretArn, KmsKeyArn: c.KmsKeyArn}
}

func customSecretConfigFromWire(w *customSecretConfigWire) *CustomSecretConfig {
	if w == nil {
		return nil
	}

	return &CustomSecretConfig{SecretArn: w.SecretArn, SecretAccessRoleArn: w.SecretAccessRoleArn}
}

func customSecretConfigToWire(c *CustomSecretConfig) *customSecretConfigWire {
	if c == nil {
		return nil
	}

	return &customSecretConfigWire{SecretArn: c.SecretArn, SecretAccessRoleArn: c.SecretAccessRoleArn}
}

// validateSecretConfig rejects requests that set both CmkSecretConfig and
// CustomSecretConfig: "Do not provide both CmkSecretConfig and
// CustomSecretConfig parameters for the same request" (botocore datasync
// 2018-11-09 model, CreateLocationSmbRequest.CmkSecretConfig documentation).
func validateSecretConfig(cmk *cmkSecretConfigWire, custom *customSecretConfigWire) error {
	if cmk != nil && custom != nil {
		return fmt.Errorf("%w: cannot provide both CmkSecretConfig and CustomSecretConfig", errInvalidRequest)
	}

	return nil
}
