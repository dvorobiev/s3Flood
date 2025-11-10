param(
  [switch]$Global
)

$ErrorActionPreference = 'Stop'

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
  Write-Error "Python is required"
}

if (-not (Get-Command aws -ErrorAction SilentlyContinue)) {
  try { pipx install awscli } catch { python -m pip install --user awscli }
}

python -m pip install --user -e .
Write-Output "Install complete. Use: python -m s3flood.cli ..."
