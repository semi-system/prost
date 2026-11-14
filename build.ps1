param([ValidateSet('test','build','fmt','lint')][string]$Task='test')
$ErrorActionPreference='Stop'
$workspacePath = $PSScriptRoot
$linuxPath = (wsl --exec wslpath -a $workspacePath).Trim()
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
$cargoArguments = switch ($Task) {
    'test' { @('cargo','test','--workspace','--locked') }
    'build' { @('cargo','build','--workspace','--locked') }
    'fmt' { @('cargo','fmt','--all','--','--check') }
    'lint' { @('cargo','clippy','--workspace','--all-targets','--locked','--','-D','warnings') }
}
wsl -u root --exec docker build -f "${linuxPath}/Dockerfile.dev" -t prost-dev $linuxPath
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
$testThreadCount = if ([string]::IsNullOrEmpty($env:RUST_TEST_THREADS)) { '4' } else { $env:RUST_TEST_THREADS }
wsl -u root --exec docker run --rm -v "${linuxPath}:/work" -v prost-target:/work/target -v prost-cargo:/usr/local/cargo/registry -w /work -e CARGO_BUILD_JOBS=2 -e "RUST_TEST_THREADS=$testThreadCount" prost-dev @cargoArguments
exit $LASTEXITCODE
