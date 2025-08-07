#########################################
#
# COMMON MODULES TO SUPPORT CICD PIPELINE
#
#########################################

# offline download from git-repo
$downloadFolderPath = "$home/Downloads"
if (-not (Test-Path -Path $downloadFolderPath -PathType Container)) {
    New-Item -Path $downloadFolderPath -ItemType Directory
}
$tarSourceUrl = (
    Invoke-RestMethod -Uri https://api.github.com/repos/azure/azure-powershell/releases/latest |
    Select-Object -ExpandProperty assets | Where-Object content_type -eq 'application/x-gzip'
).browser_download_url
$fileName = Split-Path -Path $tarSourceUrl -Leaf
$downloadFilePath = Join-Path -Path $downloadFolderPath -ChildPath $fileName
Invoke-WebRequest -Uri $tarSourceUrl -OutFile $downloadFilePath
if ($PSVersionTable.PSVersion.Major -le 5 -or $IsWindows -eq $true) {
    Unblock-File -Path $downloadFilePath
}
tar zxf $downloadFilePath -C $downloadFolderPath
.$downloadFolderPath/InstallModule.ps1

# # required moduels list
# [string[]]  $requiredModules = @("Az.KeyVault", "Az.Storage")

# # loop the required module variables and install all mentioned modules
# $requiredModules | ForEach-Object { Import-Module -Name $_ }