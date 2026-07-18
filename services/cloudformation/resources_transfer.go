package cloudformation

import "fmt"

// ---- Transfer ----

func (rc *ResourceCreator) createTransferServer(
	logicalID string,
	props map[string]any,
	_, _ map[string]string,
) (string, error) {
	if rc.backends.Transfer == nil {
		return logicalID + "-stub", nil
	}

	var protocols []string
	if rawList, ok := props["Protocols"].([]any); ok {
		for _, v := range rawList {
			if s, ok2 := v.(string); ok2 {
				protocols = append(protocols, s)
			}
		}
	}
	if len(protocols) == 0 {
		protocols = []string{"SFTP"}
	}

	server, err := rc.backends.Transfer.Backend.CreateServer(protocols, nil)
	if err != nil {
		return "", fmt.Errorf("create Transfer server: %w", err)
	}

	return server.ServerID, nil
}

func (rc *ResourceCreator) deleteTransferServer(serverID string) error {
	if rc.backends.Transfer == nil {
		return nil
	}

	_ = rc.backends.Transfer.Backend.StopServer(serverID)

	return rc.backends.Transfer.Backend.DeleteServer(serverID)
}
