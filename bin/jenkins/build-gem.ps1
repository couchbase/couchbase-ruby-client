# Copyright 2020-Present Couchbase, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

$VerbosePreference = 'Continue'

Write-Host "PowerShell Version: $($PSVersionTable.PSVersion)"

# Workaround for "Filename too long" errors during git clone
git config --global core.longpaths true

$projectRoot = Resolve-Path -Path "$PSScriptRoot\..\.."
Write-Host "--- Project root: ${projectRoot}"

$rubyVersions = "3.3.0", "3.2.2", "3.1.4", "3.0.6"
$defaultRubyVersion = $rubyVersions[0]

$originalPath = $env:PATH

foreach ($rubyVersion in $rubyVersions) {
    Remove-Item -Path "${projectRoot}\Gemfile.lock" -ErrorAction SilentlyContinue

    $majorMinorVersion = "${rubyVersion}" -replace '(\d+)\.(\d+)\.(\d+)', '$1$2'
    $installPath = "C:\Ruby${majorMinorVersion}-x64"

    $env:PATH = "$installPath\bin;$installPath\msys64\mingw64\bin;$installPath\msys64\usr\bin;$originalPath"

    $rubyBinaryPath = "${installPath}\bin\ruby.exe"
    Write-Host "---" (& "${rubyBinaryPath}" --version) "(${rubyBinaryPath})"
    $rubyAbiVersion = (& "${rubyBinaryPath}" -rrbconfig -e "print RbConfig::CONFIG['ruby_version']")
    Write-Host "--- ABI: ${rubyAbiVersion}"

    $bundleScriptPath = "${installPath}\bin\bundle.bat"
    if (-not (Test-Path "${bundleScriptPath}")) {
        $bundleScriptPath = "${installPath}\bin\bundle.cmd"
    }
    Write-Host "---" (& "$bundleScriptPath" --version) "(${bundleScriptPath})"

    $gemScriptPath = "${installPath}\bin\gem.bat"
    if (-not (Test-Path "${gemScriptPath}")) {
        $gemScriptPath = "${installPath}\bin\gem.cmd"
    }
    Write-Host "--- rubygems" (& "${gemScriptPath}" --version) "(${gemScriptPath})"

    $bundlePath = "$env:LOCALAPPDATA\vendor\bundle\${rubyVersion}"
    Write-Host "--- Install dependencies in ${bundlePath}"
    $arguments = @(
        "config",
        "set",
        "--local",
        "path",
        "`"${bundlePath}`""
    )
    $process = Start-Process -Wait -NoNewWindow -PassThru -FilePath $bundleScriptPath -ArgumentList $arguments
    if (-not $process.ExitCode -eq 0) {
        Write-Host "Unable to set bundler path. Exit code $($process.ExitCode)"
        exit 1
    }
    $process = Start-Process -Wait -NoNewWindow -PassThru -FilePath $bundleScriptPath -ArgumentList "install"
    if (-not $process.ExitCode -eq 0) {
        Write-Host "Unable to install dependencies using bundler. Exit code $($process.ExitCode)"
        exit 1
    }

    $gemVersion = (& ruby -r"${projectRoot}\lib\couchbase\version.rb" -e "puts Couchbase::VERSION[:sdk]")
    $sourceGemPath = "${projectRoot}\pkg\couchbase-${gemVersion}.gem"
    if (Test-Path "${sourceGemPath}") {
        Write-Host "--- Found source package for ${gemVersion} (${sourceGemPath})"
    } else {
        $buildNumber = if ($env:BUILD_NUMBER) { $env:BUILD_NUMBER } else { 0 }
        Write-Host "--- Inject build number ${buildNumber} into version ${gemVersion}"
        $arguments = @(
            "`"${projectRoot}\bin\jenkins\patch-version.rb`"",
            $buildNumber
        )
        $process = Start-Process -Wait -NoNewWindow -PassThru -FilePath $rubyBinaryPath -ArgumentList $arguments
        if (-not $process.ExitCode -eq 0) {
            Write-Host "Unable to inject build number. Exit code $($process.ExitCode)"
            exit 1
        }
        $gemVersion = (& ruby -r"${projectRoot}\lib\couchbase\version.rb" -e "puts Couchbase::VERSION[:sdk]")
        $sourceGemPath = "${projectRoot}\pkg\couchbase-${gemVersion}.gem"
        Write-Host "--- Build source package for ${gemVersion}"
        $process = Start-Process -Wait -NoNewWindow -PassThru -FilePath $bundleScriptPath -ArgumentList "exec", "rake", "build"
        if (-not $process.ExitCode -eq 0) {
            Write-Host "Unable to build source package. Exit code $($process.ExitCode)"
            exit 1
        }
    }

    $precompiledPath = "${projectRoot}\pkg\binary\${rubyAbiVersion}"
    New-Item -ItemType Directory -Path $precompiledPath -Force
    Write-Host "--- Build binary package for ${gemVersion} using ruby ${rubyVersion} (${precompiledPath})"

    $env:CB_STATIC_BORINGSSL = 1
    $env:CB_STATIC_STDLIB = 1
    $env:CB_REMOVE_EXT_DIRECTORY = 1
    $arguments = @(
        "exec",
        "gem",
        "compile",
        "--prune",
        "--output",
        "`"${precompiledPath}`"",
        "`"${sourceGemPath}`""
    )
    $process = Start-Process -Wait -NoNewWindow -PassThru -FilePath $bundleScriptPath -ArgumentList $arguments
    if (-not $process.ExitCode -eq 0) {
        Write-Host "Unable to build binary package. Exit code $($process.ExitCode)"
        exit 1
    }
}

$majorMinorVersion = "${defaultRubyVersion}" -replace '(\d+)\.(\d+)\.(\d+)', '$1$2'
$defaultRubyPath = "C:\Ruby${majorMinorVersion}-x64"
$defaultRubyBinaryPath = "${defaultRubyPath}\bin\ruby.exe"
$env:PATH = "$defaultRubyPath\bin;$defaultRubyPath\msys64\mingw64\bin;$defaultRubyPath\msys64\usr\bin;$originalPath"

Write-Host "--- Repackage binary gems into single `"fat`" gem"
$process = Start-Process -Wait -NoNewWindow -PassThru -FilePath $defaultRubyBinaryPath -ArgumentList "`"${projectRoot}\bin\jenkins\repackage-extension.rb`""
if (-not $process.ExitCode -eq 0) {
    Write-Host "Unable to repackage binaries into `"fat`" package. Exit code $($process.ExitCode)"
    exit 1
}

# vim: et ts=4 sw=4 sts=4
