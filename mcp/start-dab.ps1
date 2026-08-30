# Decrypts the DPAPI-protected SQL connection string (per-user, per-machine) and launches
# the DAB MCP server with it as an in-memory env var only — never written to disk in plaintext.
# Only this Windows account, on this machine, can decrypt sql-conn.dat.

$ErrorActionPreference = "Stop"
$mcpDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$credFile = Join-Path $mcpDir ".config\sql-conn.dat"

if (-not (Test-Path $credFile)) {
    Write-Error "Encrypted credential file not found: $credFile"
    exit 1
}

$encrypted = Get-Content $credFile
$secureString = ConvertTo-SecureString -String $encrypted
$bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureString)
try {
    $env:SQL_CONN_STRING = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($bstr)
} finally {
    [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
}

Set-Location $mcpDir
# DAB's MCP-stdio mode still binds a local Kestrel HTTP listener internally; pin it to a
# dedicated port so it never collides with anything else already using 5000/5001.
$env:ASPNETCORE_URLS = "http://127.0.0.1:5179"
dotnet tool run dab start --mcp-stdio --config "$mcpDir\dab-config.json"
