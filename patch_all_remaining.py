import re
import os
import glob

services = ["elbv2", "iotwireless", "kinesis", "cloudwatch", "mediaconvert"]

def get_backend_fields(svc):
    backend_path = f"services/{svc}/backend.go"
    if not os.path.exists(backend_path):
        return {}
    
    with open(backend_path, "r") as f:
        content = f.read()
        
    match = re.search(r'type InMemoryBackend struct \{(.*?)\}', content, re.DOTALL)
    if not match: return {}
    
    fields = {}
    for line in match.group(1).split("\n"):
        line = line.split("//")[0].strip()
        if not line: continue
        parts = line.split()
        if len(parts) >= 2:
            name = parts[0]
            typ = " ".join(parts[1:])
            # ignore unexported and embedded fields
            if name[0].isupper() and name != "RWMutex" and name != "Mutex":
                fields[name] = typ
    return fields

def get_snapshot_fields(svc):
    pers_path = f"services/{svc}/persistence.go"
    if not os.path.exists(pers_path): return {}
    
    with open(pers_path, "r") as f:
        content = f.read()
        
    match = re.search(r'type backendSnapshot struct \{(.*?)\}', content, re.DOTALL)
    if not match: return {}
    
    fields = {}
    for line in match.group(1).split("\n"):
        line = line.split("//")[0].strip()
        if not line or line.startswith("`"): continue
        parts = line.split()
        if len(parts) >= 2:
            name = parts[0]
            typ = parts[1]
            if name[0].isupper():
                fields[name] = typ
    return fields

for svc in services:
    pers_path = f"services/{svc}/persistence.go"
    if not os.path.exists(pers_path):
        print(f"No persistence.go for {svc}")
        continue
        
    b_fields = get_backend_fields(svc)
    s_fields = get_snapshot_fields(svc)
    
    missing = []
    for k, v in b_fields.items():
        if k not in s_fields:
            missing.append((k, v))
            
    if not missing:
        print(f"{svc} has no missing fields.")
        continue
        
    print(f"{svc} missing: {len(missing)} fields")
    
    with open(pers_path, "r") as f:
        content = f.read()
        
    # Inject missing fields into backendSnapshot
    struct_match = re.search(r'type backendSnapshot struct \{(.*?)\}', content, re.DOTALL)
    if struct_match:
        old_struct = struct_match.group(1)
        new_struct = old_struct
        if not new_struct.endswith("\n"):
            new_struct += "\n"
        for k, v in missing:
            json_tag = k[:1].lower() + k[1:]
            new_struct += f'\t{k} {v} `json:"{json_tag}"`\n'
        content = content.replace(old_struct, new_struct)
        
    # Inject into Snapshot
    snap_match = re.search(r'(func \([^)]+\) Snapshot\(\) \[\]byte \{\s+.*?snap := backendSnapshot\{)(.*?)(\n\t\})', content, re.DOTALL)
    if snap_match:
        old_snap = snap_match.group(2)
        new_snap = old_snap
        if not new_snap.endswith("\n"): new_snap += "\n"
        for k, _ in missing:
            new_snap += f'\t\t{k}: b.{k},\n'
        content = content.replace(snap_match.group(1) + old_snap + snap_match.group(3),
                                  snap_match.group(1) + new_snap + snap_match.group(3))
                                  
    # Inject into Restore
    rest_match = re.search(r'(func \([^)]+\) Restore\(.*?\{\s+.*?json\.Unmarshal.*?\n)(.*?)(return nil\n\})', content, re.DOTALL)
    if rest_match:
        old_rest = rest_match.group(2)
        new_rest = old_rest
        for k, _ in missing:
            new_rest += f'\tb.{k} = snap.{k}\n'
        content = content.replace(rest_match.group(1) + old_rest + rest_match.group(3),
                                  rest_match.group(1) + new_rest + rest_match.group(3))
                                  
    with open(pers_path, "w") as f:
        f.write(content)
        
    print(f"Patched {svc}")

