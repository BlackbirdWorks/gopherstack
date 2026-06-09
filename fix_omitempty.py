import os
import glob
import re

files = []
for d in ["services/ec2/", "services/cognitoidp/", "services/iam/"]:
    files.extend(glob.glob(d + "*.go"))

for file in files:
    with open(file, "r") as f:
        content = f.read()

    new_content = []
    lines = content.splitlines()
    for line in lines:
        # if the line contains a json tag with omitempty
        if '`json:' in line and ',omitempty"' in line:
            # simple heuristic: if it's not a pointer, map, slice, string, integer, bool, float
            # but wait, omitempty works for int, float, bool, string, pointer, slice, map.
            # It does NOT work for struct types (like time.Time, xml.Name, custom structs).
            
            clean_line = line.split("//")[0].strip()
            parts = clean_line.split()
            if len(parts) >= 2:
                # The type is usually the second token, except if there are tags
                # E.g.: Field time.Time `json:"field,omitempty"`
                typ = parts[1]
                # If it's not a pointer, slice, or map, and it's a struct (capitalized or time.Time or xml.Name)
                if not typ.startswith("*") and not typ.startswith("[]") and not typ.startswith("map[") and typ not in ["string", "int", "int64", "int32", "float64", "float32", "bool"]:
                    # strip omitempty
                    line = line.replace(',omitempty"', '"')
        new_content.append(line)
        
    with open(file, "w") as f:
        f.write("\n".join(new_content))
        
