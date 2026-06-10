import re
import os

services = ["ec2", "iam"]

for service in services:
    backend_file = f"services/{service}/backend.go"
    persistence_file = f"services/{service}/persistence.go"
    
    with open(backend_file, "r") as f:
        backend_content = f.read()
    
    with open(persistence_file, "r") as f:
        persistence_content = f.read()
        
    backend_fields = []
    in_struct = False
    for line in backend_content.splitlines():
        if "type InMemoryBackend struct {" in line:
            in_struct = True
            continue
        if in_struct:
            if line.startswith("}"):
                break
            clean_line = line.split("//")[0].strip()
            if not clean_line:
                continue
            parts = clean_line.split()
            if len(parts) >= 2:
                name = parts[0]
                typ = "".join(parts[1:])
                # filter ephemeral fields
                if name in ["mu", "compute", "dnsRegistrar", "comprehensive", "lifecycleOnce", "faultsMu", "stopCh", "accountID", "region", "endpoint", "ebsDefaultKmsKeyID", "imageBlockPublicAccess", "snapshotBlockPublicAccess", "defaultCreditSpec", "accountAliases"]:
                    continue
                if name.startswith("eniID") or name.startswith("instanceID"):
                    continue # indices
                if name[0].islower():
                    backend_fields.append((name, typ))
                    
    snap_fields = {}
    in_struct = False
    for line in persistence_content.splitlines():
        if "type backendSnapshot struct {" in line:
            in_struct = True
            continue
        if in_struct:
            if line.startswith("}"):
                break
            clean_line = line.split("//")[0].strip()
            if not clean_line:
                continue
            parts = clean_line.split()
            if len(parts) >= 2:
                name = parts[0]
                typ = parts[1]
                lower_name = name[0].lower() + name[1:]
                snap_fields[lower_name] = typ
                snap_fields[name] = typ # also store original capitalized

    missing = []
    for f_name, f_type in backend_fields:
        if f_name not in snap_fields and f_name+"s" not in snap_fields and f_name.replace("s", "") not in snap_fields:
            if "chan" in f_type or "func" in f_type or f_type == "sync.Once": continue
            # ensure it's not already in snapshot under different name
            found = False
            for s_name in snap_fields:
                if s_name.lower() == f_name.lower():
                    found = True
                    break
            if not found:
                missing.append((f_name, f_type))
    
    if not missing:
        continue
        
    print(f"Fixing {service}...")
    
    # Update backendSnapshot struct
    new_fields = []
    for f_name, f_type in missing:
        cap_name = f_name[0].upper() + f_name[1:]
        if cap_name == "Vpcs": cap_name = "VPCs"
        elif "Id" in cap_name: cap_name = cap_name.replace("Id", "ID")
        new_fields.append(f"\t{cap_name} {f_type} `json:\"{f_name},omitempty\"`")
        
    # insert before the closing brace of backendSnapshot
    lines = persistence_content.splitlines()
    for i, line in enumerate(lines):
        if "type backendSnapshot struct {" in line:
            # find end
            j = i
            while not lines[j].startswith("}"):
                j += 1
            lines.insert(j, "\n".join(new_fields))
            break
            
    persistence_content = "\n".join(lines)
    
    # Update Snapshot function
    lines = persistence_content.splitlines()
    for i, line in enumerate(lines):
        if "snap := backendSnapshot{" in line:
            j = i
            while not lines[j].strip().startswith("}"):
                j += 1
            
            assignments = []
            for f_name, f_type in missing:
                cap_name = f_name[0].upper() + f_name[1:]
                if cap_name == "Vpcs": cap_name = "VPCs"
                elif "Id" in cap_name: cap_name = cap_name.replace("Id", "ID")
                assignments.append(f"\t\t{cap_name}: b.{f_name},")
            
            lines.insert(j, "\n".join(assignments))
            break
            
    persistence_content = "\n".join(lines)
    
    # Update Restore function
    lines = persistence_content.splitlines()
    for i, line in enumerate(lines):
        if "func (b *InMemoryBackend) Restore" in line or "func (b *InMemoryBackend) restore(" in line:
            j = i
            # find end of func
            open_braces = 0
            found_start = False
            for k in range(j, len(lines)):
                if "{" in lines[k]:
                    open_braces += lines[k].count("{")
                    found_start = True
                if "}" in lines[k]:
                    open_braces -= lines[k].count("}")
                if found_start and open_braces == 0:
                    # found end of func. We insert right before the `return nil`
                    l = k - 1
                    while "return" not in lines[l] and l > j:
                        l -= 1
                    
                    restores = []
                    for f_name, f_type in missing:
                        cap_name = f_name[0].upper() + f_name[1:]
                        if cap_name == "Vpcs": cap_name = "VPCs"
                        elif "Id" in cap_name: cap_name = cap_name.replace("Id", "ID")
                        
                        if "map[" in f_type:
                            restores.append(f"\tif snap.{cap_name} != nil {{\n\t\tb.{f_name} = snap.{cap_name}\n\t}} else {{\n\t\tb.{f_name} = make({f_type})\n\t}}")
                        else:
                            restores.append(f"\tb.{f_name} = snap.{cap_name}")
                            
                    lines.insert(l, "\n".join(restores))
                    break
            break
            
    persistence_content = "\n".join(lines)
    
    with open(persistence_file, "w") as f:
        f.write(persistence_content)
        
print("Done")
