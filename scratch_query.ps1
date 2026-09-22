$ErrorActionPreference = "Stop"
$mcpDir = "C:\Users\ronenaa\ADM_SQL\mcp"
$credFile = Join-Path $mcpDir ".config\sql-conn.dat"

$enc = Get-Content $credFile
$ss = ConvertTo-SecureString -String $enc
$bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($ss)
try {
    $conn = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($bstr)
} finally {
    [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
}

$sql = Get-Content -Raw $args[0]

$c = New-Object System.Data.SqlClient.SqlConnection $conn
$c.Open()
try {
    $cmd = $c.CreateCommand()
    $cmd.CommandText = $sql
    $cmd.CommandTimeout = 180
    $r = $cmd.ExecuteReader()
    do {
        $cols = @()
        for ($i = 0; $i -lt $r.FieldCount; $i++) { $cols += $r.GetName($i) }
        if ($cols.Count -gt 0) { Write-Output ($cols -join " | ") }
        while ($r.Read()) {
            $vals = @()
            for ($i = 0; $i -lt $r.FieldCount; $i++) {
                $v = $r.GetValue($i)
                $vals += $(if ($v -is [DBNull]) { "NULL" } else { "$v" })
            }
            Write-Output ($vals -join " | ")
        }
        Write-Output "---"
    } while ($r.NextResult())
    $r.Close()
} finally { $c.Close() }
