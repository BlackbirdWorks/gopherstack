import re

with open("services/cloudformation/provider.go", "r") as f:
    content = f.read()

# Add imports if they don't exist
imports = [
    'backupbackend "github.com/blackbirdworks/gopherstack/services/backup"',
    'elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"',
    'wafv2backend "github.com/blackbirdworks/gopherstack/services/wafv2"',
]

for imp in imports:
    if imp not in content:
        content = content.replace('"github.com/blackbirdworks/gopherstack/services/bedrockruntime"', f'{imp}\n\t"github.com/blackbirdworks/gopherstack/services/bedrockruntime"')

# Add interface methods
interface_methods = [
    "GetBackupHandler() service.Registerable",
    "GetELBv2Handler() service.Registerable",
    "GetWAFv2Handler() service.Registerable",
]

for meth in interface_methods:
    if meth not in content:
        content = content.replace("GetCognitoIdentityHandler() service.Registerable", f"GetCognitoIdentityHandler() service.Registerable\n\t{meth}")

# Add to extractAllServiceBackends
extract_statements = [
    "backends.Backup, _ = getHandler[*backupbackend.Handler](bp.GetBackupHandler())",
    "backends.ELBv2, _ = getHandler[*elbv2backend.Handler](bp.GetELBv2Handler())",
    "backends.WAFv2, _ = getHandler[*wafv2backend.Handler](bp.GetWAFv2Handler())",
]

for stmt in extract_statements:
    if stmt not in content:
        content = content.replace("backends.MemoryDB, _ = getHandler[*memorydb.Handler](bp.GetMemoryDBHandler())", f"backends.MemoryDB, _ = getHandler[*memorydb.Handler](bp.GetMemoryDBHandler())\n\t{stmt}")

with open("services/cloudformation/provider.go", "w") as f:
    f.write(content)
