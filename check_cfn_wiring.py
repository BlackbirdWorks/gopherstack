import re

with open("services/cloudformation/resources.go", "r") as f:
    resources = f.read()

with open("services/cloudformation/provider.go", "r") as f:
    provider = f.read()
    
struct_lines = resources.split("type ServiceBackends struct {")[1].split("}")[0].splitlines()
fields = []
for line in struct_lines:
    line = line.split("//")[0].strip()
    if not line: continue
    parts = line.split()
    if len(parts) >= 2:
        if parts[0] not in ["AccountID", "Region"]:
            fields.append(parts[0])
            
extract_content = provider.split("func extractCoreBackends")[1]

missing = []
for f in fields:
    if f"backends.{f}" not in extract_content:
        missing.append(f)
        
print("Missing wiring:")
for m in missing:
    print(m)
