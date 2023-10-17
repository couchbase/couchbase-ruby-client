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

Write-Host "--- COMPUTERNAME=$env:COMPUTERNAME"
Write-Host "--- NODE_NAME=$env:NODE_NAME"
Write-Host "--- NODE_LABELS=$env:NODE_LABELS"

Get-Volume | Select-Object DriveLetter, FileSystemLabel, SizeRemaining, Size | Format-Table -AutoSize
Write-Host "--- PowerShell Version: $($PSVersionTable.PSVersion)"
Write-Host "--- Installed Ruby Versions:"
Get-ChildItem -Path "C:\" -Directory -Filter "Ruby*" | Format-Table -AutoSize

$brokenRubyUninstaller = "C:\tools\ruby31\unins000.exe"
if (Test-Path $brokenRubyUninstaller) {
    $process = Start-Process -Wait -NoNewWindow -FilePath $brokenRubyUninstaller -ArgumentList "/VERYSILENT", "/SUPPRESSMSGBOXES", "/NORESTART"
    if ($process.ExitCode -eq 0) {
        Write-Host "--- Suspicious Ruby 3.1 uninstalled"
    } else {
        Start-Process -Wait -NoNewWindow -FilePath choco -ArgumentList uninstall, ruby
        Remove -Recurse -Force "C:\tools\ruby31"
    }
    Get-ChildItem -Path "C:\tools" -Directory | Format-Table -AutoSize
}

[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12, [Net.SecurityProtocolType]::Tls11, [Net.SecurityProtocolType]::Tls

$installPath = "$env:LOCALAPPDATA\CMake"
if (Test-Path "${installPath}\bin\cmake.exe") {
    Write-Host "--- Installed CMake version (${installPath}):" (& "${installPath}\bin\cmake.exe" --version)
} else {
    $cmakeVersion = "3.28.1"
    $url = "https://github.com/Kitware/CMake/releases/download/v${cmakeVersion}/cmake-${cmakeVersion}-windows-x86_64.zip"
    Write-Host "--- Downloading CMake version ${cmakeVersion} from ${url}"
    $outputFilePath = "CMake${cmakeVersion}.zip"
    Invoke-WebRequest -Uri $url -OutFile $outputFilePath
    Write-Host "--- Download complete to ${outputFilePath}"

    Write-Host "--- Extract ${outputFilePath} to ${installPath}"
    Expand-Archive -Path ${outputFilePath} -DestinationPath ${installPath} -Force
    Get-ChildItem "${installPath}\cmake-${cmakeVersion}-windows-x86_64" | Move-Item -Destination "${installPath}" -Force

    Write-Host "--- Installation complete for CMake version ${cmakeVersion}"
    Remove-Item $outputFilePath
    Write-Host "--- Installed CMake version (${installPath}):" (& "${installPath}\bin\cmake.exe" --version)
}

$rubyVersions = "3.3.0", "3.2.2", "3.1.4", "3.0.6"

foreach ($rubyVersion in $rubyVersions) {
    $majorMinorVersion = $rubyVersion -replace '(\d+)\.(\d+)\.(\d+)', '$1$2'
    $installPath = "C:\Ruby${majorMinorVersion}-x64"

    if (Test-Path "${installPath}\bin\ruby.exe") {
        Write-Host "--- Ruby ${rubyVersion} is already installed in ${installPath}. Skipping download and installation."
        Write-Host "--- Installed Ruby version:" (& "${installPath}\bin\ruby.exe" --version)
        continue
    }

    $url = "https://github.com/oneclick/rubyinstaller2/releases/download/RubyInstaller-${rubyVersion}-1/rubyinstaller-devkit-${rubyVersion}-1-x64.exe"
    Write-Host "--- Downloading RubyInstaller version ${rubyVersion} from ${url}"

    $outputFilePath = "RubyInstaller-${rubyVersion}-1-x64.exe"
    Invoke-WebRequest -Uri $url -OutFile $outputFilePath
    $outputFilePath = Resolve-Path -Path $outputFilePath

    Write-Host "--- Download complete to ${outputFilePath}. Installing Ruby version ${rubyVersion}"

    $process = Start-Process -Wait -NoNewWindow -PassThru -FilePath $outputFilePath -ArgumentList "/NORESTART", "/VERYSILENT", "/CURRENTUSER", "/LOG=install-${rubyVersion}.log", "/NoPath", "/InstallDir:$installPath"
    if ($process.ExitCode -eq 0) {
        Write-Host "--- Installation complete for Ruby version ${rubyVersion}"
    } else {
        Write-Host "--- Installation failed with exit code $($process.ExitCode)"
        exit 1
    }

    Write-Host "--- Installed Ruby version:" (& "${installPath}\bin\ruby.exe" --version)

    Remove-Item $outputFilePath
}

# vim: et ts=4 sw=4 sts=4
