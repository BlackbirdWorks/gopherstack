import re

with open("services/iotwireless/persistence.go", "r") as f:
    c = f.read()

# Struct
c = c.replace('	ImportTasks            map[string]*WirelessDeviceImportTask `json:"importTasks,omitempty"`',
              '	ImportTasks            map[string]*WirelessDeviceImportTask `json:"importTasks,omitempty"`\n\tSingleImportTasks      map[string]*SingleWirelessDeviceImportTask `json:"singleImportTasks,omitempty"`\n\tGatewayTasks           map[string]*GatewayTask `json:"gatewayTasks,omitempty"`\n\tGatewayTaskDefs        map[string]*GatewayTaskDefinition `json:"gatewayTaskDefs,omitempty"`\n\tPositions              map[string]map[string]any `json:"positions,omitempty"`\n\tQueuedMessages         map[string][]QueuedMessage `json:"queuedMessages,omitempty"`')

# buildSnapshotLocked
c = c.replace('		ImportTasks:            b.importTasks,',
              '		ImportTasks:            b.importTasks,\n\t\tSingleImportTasks:      b.singleImportTasks,\n\t\tGatewayTasks:           b.gatewayTasks,\n\t\tGatewayTaskDefs:        b.gatewayTaskDefs,\n\t\tPositions:              b.positions,\n\t\tQueuedMessages:         b.queuedMessages,')

# Restore (restoreSnapshotLocked)
c = c.replace('	b.importTasks = snap.ImportTasks',
              '	b.importTasks = snap.ImportTasks\n\tb.singleImportTasks = snap.SingleImportTasks\n\tb.gatewayTasks = snap.GatewayTasks\n\tb.gatewayTaskDefs = snap.GatewayTaskDefs\n\tb.positions = snap.Positions\n\tb.queuedMessages = snap.QueuedMessages')

with open("services/iotwireless/persistence.go", "w") as f:
    f.write(c)

