import re

with open("services/cloudwatch/persistence.go", "r") as f:
    c = f.read()

# Struct
c = c.replace('	Region           string                              `json:"region"`',
              '	Region           string                              `json:"region"`\n\tTotalMetrics     int                                 `json:"totalMetrics"`')

# Snapshot
c = c.replace('		Region:           b.region,',
              '		Region:           b.region,\n\t\tTotalMetrics:     b.totalMetrics,')

# Restore
c = c.replace('	b.region = snap.Region',
              '	b.region = snap.Region\n\tb.totalMetrics = snap.TotalMetrics')

with open("services/cloudwatch/persistence.go", "w") as f:
    f.write(c)

