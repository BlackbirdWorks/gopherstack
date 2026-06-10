import os
import glob
import re

files = glob.glob("services/iam/*.go") + glob.glob("services/ec2/*.go") + glob.glob("services/cognitoidp/*.go")

for file in files:
    if file.endswith("persistence.go"):
        continue
    
    with open(file, "r") as f:
        content = f.read()
        
    # we want to strip ` json:"..."` from all lines
    new_lines = []
    for line in content.splitlines():
        if ' json:"' in line and '`' in line:
            line = re.sub(r'[ \t]*json:"[^"]*"', '', line)
            # if the backticks are now empty, remove them
            line = line.replace('``', '')
        new_lines.append(line)
        
    with open(file, "w") as f:
        f.write("\n".join(new_lines))
        
