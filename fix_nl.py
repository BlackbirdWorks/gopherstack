import re

for file in ["services/ec2/persistence.go", "services/iam/persistence.go"]:
    with open(file, "r") as f:
        content = f.read()
    
    content = content.replace("}\n\treturn nil", "}\n\n\treturn nil")
    
    with open(file, "w") as f:
        f.write(content)
