import re

with open("services/mediaconvert/persistence.go", "r") as f:
    c = f.read()

# Struct
c = c.replace('	Queues        map[string]*Queue            `json:"queues"`',
              '	Queues        map[string]*Queue            `json:"queues"`\n\tQueries       map[string]*jobsQuery        `json:"queries,omitempty"`')

# Snapshot
c = c.replace('		Queues:        copyQueues(b.queues),',
              '		Queues:        copyQueues(b.queues),\n\t\tQueries:       b.queries,')

# Restore
c = c.replace('	b.queues = snap.Queues',
              '	b.queues = snap.Queues\n\tb.queries = snap.Queries')

with open("services/mediaconvert/persistence.go", "w") as f:
    f.write(c)

