package ocivault

import "time"

type vaultWire struct {
	TimeCreated        time.Time                 `json:"timeCreated"`
	TimeOfDeletion     *time.Time                `json:"timeOfDeletion,omitempty"`
	FreeformTags       map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags        map[string]map[string]any `json:"definedTags,omitempty"`
	CompartmentID      string                    `json:"compartmentId"`
	CryptoEndpoint     string                    `json:"cryptoEndpoint"`
	DisplayName        string                    `json:"displayName"`
	ID                 string                    `json:"id"`
	LifecycleState     string                    `json:"lifecycleState"`
	ManagementEndpoint string                    `json:"managementEndpoint"`
	VaultType          string                    `json:"vaultType"`
	WrappingKeyID      string                    `json:"wrappingkeyId"`
	IsPrimary          bool                      `json:"isPrimary"`
}

func vaultView(v *Vault, hostPort, state string) vaultWire {
	if state == "" {
		state = v.State
	}

	return vaultWire{
		TimeCreated: v.TimeCreated, TimeOfDeletion: v.TimeOfDeletion, FreeformTags: v.FreeformTags,
		DefinedTags: v.DefinedTags, CompartmentID: v.CompartmentID,
		CryptoEndpoint:     "http://" + v.Prefix + "-crypto.localhost" + hostPort,
		ManagementEndpoint: "http://" + v.Prefix + "-management.localhost" + hostPort,
		DisplayName:        v.DisplayName, ID: v.ID, LifecycleState: state, VaultType: v.VaultType,
		WrappingKeyID: v.WrappingKeyID, IsPrimary: true,
	}
}

type vaultSummaryWire struct {
	TimeCreated        time.Time                 `json:"timeCreated"`
	TimeOfDeletion     *time.Time                `json:"timeOfDeletion,omitempty"`
	FreeformTags       map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags        map[string]map[string]any `json:"definedTags,omitempty"`
	CompartmentID      string                    `json:"compartmentId"`
	CryptoEndpoint     string                    `json:"cryptoEndpoint"`
	DisplayName        string                    `json:"displayName"`
	ID                 string                    `json:"id"`
	LifecycleState     string                    `json:"lifecycleState"`
	ManagementEndpoint string                    `json:"managementEndpoint"`
	VaultType          string                    `json:"vaultType"`
}

func vaultSummary(v *Vault, hostPort string) vaultSummaryWire {
	w := vaultView(v, hostPort, "")

	return vaultSummaryWire{
		TimeCreated: w.TimeCreated, TimeOfDeletion: w.TimeOfDeletion, FreeformTags: w.FreeformTags,
		DefinedTags: w.DefinedTags, CompartmentID: w.CompartmentID, CryptoEndpoint: w.CryptoEndpoint,
		DisplayName: w.DisplayName, ID: w.ID, LifecycleState: w.LifecycleState,
		ManagementEndpoint: w.ManagementEndpoint, VaultType: w.VaultType,
	}
}

type keyWire struct {
	TimeCreated            time.Time                 `json:"timeCreated"`
	TimeOfDeletion         *time.Time                `json:"timeOfDeletion,omitempty"`
	AutoKeyRotationDetails *RotationDetails          `json:"autoKeyRotationDetails,omitempty"`
	FreeformTags           map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags            map[string]map[string]any `json:"definedTags,omitempty"`
	CompartmentID          string                    `json:"compartmentId"`
	CurrentKeyVersion      string                    `json:"currentKeyVersion"`
	DisplayName            string                    `json:"displayName"`
	ID                     string                    `json:"id"`
	LifecycleState         string                    `json:"lifecycleState"`
	ProtectionMode         string                    `json:"protectionMode"`
	VaultID                string                    `json:"vaultId"`
	KeyShape               KeyShape                  `json:"keyShape"`
	IsPrimary              bool                      `json:"isPrimary"`
	IsAutoRotationEnabled  bool                      `json:"isAutoRotationEnabled"`
}

func keyView(k *Key, state string) keyWire {
	if state == "" {
		state = k.State
	}

	return keyWire{
		TimeCreated: k.TimeCreated, TimeOfDeletion: k.TimeOfDeletion, AutoKeyRotationDetails: k.Rotation,
		FreeformTags: k.FreeformTags, DefinedTags: k.DefinedTags, CompartmentID: k.CompartmentID,
		CurrentKeyVersion: k.CurrentVersion, DisplayName: k.DisplayName, ID: k.ID, LifecycleState: state,
		ProtectionMode: k.ProtectionMode, VaultID: k.VaultID, KeyShape: k.Shape, IsPrimary: true,
		IsAutoRotationEnabled: k.AutoRotation,
	}
}

type keySummaryWire struct {
	TimeCreated           time.Time                 `json:"timeCreated"`
	FreeformTags          map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags           map[string]map[string]any `json:"definedTags,omitempty"`
	CompartmentID         string                    `json:"compartmentId"`
	DisplayName           string                    `json:"displayName"`
	ID                    string                    `json:"id"`
	LifecycleState        string                    `json:"lifecycleState"`
	ProtectionMode        string                    `json:"protectionMode"`
	Algorithm             string                    `json:"algorithm"`
	VaultID               string                    `json:"vaultId"`
	IsAutoRotationEnabled bool                      `json:"isAutoRotationEnabled"`
}

func keySummary(k *Key) keySummaryWire {
	return keySummaryWire{
		TimeCreated: k.TimeCreated, FreeformTags: k.FreeformTags, DefinedTags: k.DefinedTags,
		CompartmentID: k.CompartmentID, DisplayName: k.DisplayName, ID: k.ID, LifecycleState: k.State,
		ProtectionMode: k.ProtectionMode, Algorithm: k.Shape.Algorithm, VaultID: k.VaultID,
		IsAutoRotationEnabled: k.AutoRotation,
	}
}

type keyVersionWire struct {
	TimeCreated    time.Time  `json:"timeCreated"`
	TimeOfDeletion *time.Time `json:"timeOfDeletion,omitempty"`
	CompartmentID  string     `json:"compartmentId"`
	ID             string     `json:"id"`
	KeyID          string     `json:"keyId"`
	VaultID        string     `json:"vaultId"`
	PublicKey      string     `json:"publicKey,omitempty"`
	LifecycleState string     `json:"lifecycleState"`
	Origin         string     `json:"origin"`
	IsPrimary      bool       `json:"isPrimary"`
	IsAutoRotated  bool       `json:"isAutoRotated"`
}

func keyVersionView(k *Key, kv *KeyVersion, state string) keyVersionWire {
	if state == "" {
		state = kv.State
	}

	return keyVersionWire{
		TimeCreated: kv.TimeCreated, TimeOfDeletion: kv.TimeOfDeletion, CompartmentID: k.CompartmentID,
		ID: kv.ID, KeyID: k.ID, VaultID: k.VaultID, LifecycleState: state, Origin: kv.Origin,
		IsPrimary: true, IsAutoRotated: kv.AutoRotated,
	}
}

func keyVersionFull(k *Key, kv *KeyVersion, state string) keyVersionWire {
	w := keyVersionView(k, kv, state)
	if k.Shape.Algorithm != algAES {
		w.PublicKey = publicKeyPEM(kv.Material)
	}

	return w
}

type keyVersionSummaryWire struct {
	TimeCreated    time.Time  `json:"timeCreated"`
	TimeOfDeletion *time.Time `json:"timeOfDeletion,omitempty"`
	CompartmentID  string     `json:"compartmentId"`
	ID             string     `json:"id"`
	KeyID          string     `json:"keyId"`
	Origin         string     `json:"origin"`
	VaultID        string     `json:"vaultId"`
	LifecycleState string     `json:"lifecycleState"`
	IsAutoRotated  bool       `json:"isAutoRotated"`
}

func keyVersionSummary(k *Key, kv *KeyVersion) keyVersionSummaryWire {
	return keyVersionSummaryWire{
		TimeCreated: kv.TimeCreated, TimeOfDeletion: kv.TimeOfDeletion, CompartmentID: k.CompartmentID, ID: kv.ID,
		KeyID: k.ID, Origin: kv.Origin, VaultID: k.VaultID, LifecycleState: kv.State, IsAutoRotated: kv.AutoRotated,
	}
}
