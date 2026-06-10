import re
import os

services = ["cognitoidp", "apigateway", "elbv2", "iotwireless", "kinesis", "cloudwatch", "ec2", "mediaconvert", "iam"]

for service in services:
    backend_file = f"services/{service}/backend.go"
    persistence_file = f"services/{service}/persistence.go"
    
    if not os.path.exists(backend_file) or not os.path.exists(persistence_file):
        continue
        
    with open(backend_file, "r") as f:
        backend_content = f.read()
    
    with open(persistence_file, "r") as f:
        persistence_content = f.read()
        
    backend_fields = {}
    in_struct = False
    for line in backend_content.splitlines():
        if "type InMemoryBackend struct {" in line:
            in_struct = True
            continue
        if in_struct:
            if line.startswith("}"):
                break
            line = line.split("//")[0].strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) >= 2:
                name = parts[0]
                if name == "mu" or name == "accountID" or name == "region" or name == "endpoint": continue
                if name[0].islower():
                    typ = "".join(parts[1:])
                    backend_fields[name] = typ
                    
    snap_fields = {}
    in_struct = False
    for line in persistence_content.splitlines():
        if "type backendSnapshot struct {" in line:
            in_struct = True
            continue
        if in_struct:
            if line.startswith("}"):
                break
            line = line.split("//")[0].strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) >= 2:
                name = parts[0]
                typ = parts[1]
                lower_name = name[0].lower() + name[1:]
                if lower_name == "aPIs": lower_name = "apis"
                if lower_name == "aPIKeys": lower_name = "apiKeys"
                snap_fields[lower_name] = typ
                
    missing = []
    for f_name, f_type in backend_fields.items():
        if f_name not in snap_fields and f_name + "s" not in snap_fields and f_name.replace("s", "") not in snap_fields:
            missing.append((f_name, f_type))
            
    if missing:
        print(f"[{service}] Missing in snapshot:")
        for m in missing:
            print(f"  {m[0]}: {m[1]}")
