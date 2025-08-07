[CmdletBinding()]
param (
    # [string] name of the storge account
    [Parameter(Mandatory = $true)]
    [string] $StorageAccountName,

    # [string] name of the resource group of the storge account
    [Parameter(Mandatory = $true)]
    [string] $StorageAccountResourceGroupName,

    # [string] Path of the storage content list
    [Parameter(Mandatory = $true)]
    [string] $StorageContentFilePath
    
)

#region global variables

[string[]] $containerNames = @("onelake", "mdm")  # Define multiple containers here
[string[]]  $containerDirectoryNames = @("raw/", "bronze/", "silver/", "gold/")

#endregion global variables


#region Function Get-StorageAccount

function Get-StorageAccount {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true)]
        [string] $ResourceGroupName,

        [Parameter(Mandatory = $true)]
        [string] $StorageAccountName
    )
    
    "##[section]Validate storage account"
    $storageAccount = Get-AzStorageAccount -ResourceGroupName $ResourceGroupName -Name $StorageAccountName -ErrorAction SilentlyContinue

    if ($null -eq $storageAccount) {
        Write-Output "::error file=app.js,line=1::Cannot find Azure storage account: [$($StorageAccountName)]"
    } else {
        Write-Output "Found Azure storage account: [$($StorageAccountName)]"
    }

    return $storageAccount
}

#endregion Function Get-StorageAccount

#region Function Create-StorageContainer 
function New-StorageContainer{
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true)]
        $StorageAccountObject,

        [Parameter(Mandatory = $true)]
        [string] $StorageContainerName
    )

    Write-Host "Getting storage container context.."  

    ## Get the storage account context  
    $ctx=$StorageAccountObject.Context      
 
    ## Check if the storage container exists  
    if(Get-AzStorageContainer -Name $StorageContainerName -Context $ctx -ErrorAction SilentlyContinue) {  
        Write-Host "Container [" $StorageContainerName "] already exists."  
    }  
    else {  
        Write-Host "Container [" $StorageContainerName "] does not exist. Will create it now.."   
        ## Create a new Azure Storage Account  
        New-AzStorageContainer -Name $StorageContainerName -Context $ctx -Permission Off  
        Write-Host "Container [" $StorageContainerName "] has been created successfully."  
    }       
}

#endregion Function Create-StorageContainer 

#region Function Create-StorageContainerDirectoriesr  
function New-StorageContainerDirectories {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true)]
        $StorageAccountObject,
        
        [Parameter(Mandatory = $true)]
        [string] $StorageContainerName,

        [Parameter(Mandatory = $true)]
        [string[]] $StorageContainerDirectories
    )

    # Write-Host "Getting storage container context.."  

    # Get the storage account context  
    $ctx=$StorageAccountObject.Context

    foreach ($dirName in $StorageContainerDirectories) {

        $folderExists = Get-AzDataLakeGen2Item -FileSystem $StorageContainerName -Path $dirName -Context $ctx -ErrorAction SilentlyContinue
        if ($folderExists) {
            Write-Host "Path [$($dirName)] exists in container $($StorageContainerName)"
        } else {
            Write-Host "Creating directory [" $($dirName) "] under storage container [" $($StorageContainerName) "]."
            New-AzDataLakeGen2Item -Context $ctx -FileSystem $StorageContainerName -Path $dirName -Directory | Out-Null
            Write-Host "Successfully created directory [" $($dirName) "] under storage container [" $($StorageContainerName) "]."
        }
    }
}
#endregion Function Create-StorageContainerDirectories  


#region Function Set-StorageBlobContents  
function Set-StorageBlobContents {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true)]
        $TargetStorageAccountObject,
        
        [Parameter(Mandatory = $true)]
        [string] $TargetStorageContainerName,

        [Parameter(Mandatory = $true)]
        [string] $StorageContentFilePath        
    )

    # Write-Host "Getting storage container context.."
    
    $filesList = Get-Content $StorageContentFilePath -Raw | `
                    Replace-StringTokens -StorageAccountName $TargetStorageAccountObject.StorageAccountName | ConvertFrom-Json

    foreach($file in $filesList){
        Write-Host "Uploading.."
        # https://sadwhdev02.blob.core.windows.net/onelake/gold/model_data/lead_score_model.rds
        Write-Host "    Source: [" $($file.source.file_path) "]."
        Write-Host "    Destination: [" $($file.target.file_path) "]."

        # Parse URL to extract storage account properties
        $sourceStorageProperties = Get-StorageAccountPropertiesFromURL -StorageContentURL $file.source.file_path 
        $targetStorageProperties = Get-StorageAccountPropertiesFromURL -StorageContentURL $file.target.file_path
        # Use extracted storage account properties to get object
        $sourceAzContext = Get-AzContext -ListAvailable | ?{$_.Subscription.Name -eq "$($file.source.subscription_name)"}
        $sourceStorageAccountObject = Get-AzStorageAccount -DefaultProfile $sourceAzContext  | Where-Object { $_.StorageAccountName -eq $sourceStorageProperties.StorageAccountName }
        # Download blob content from blob storage into default folder
        Get-AzStorageBlobContent -Container $sourceStorageProperties.ContainerName -Blob $sourceStorageProperties.BlobName `
            -Context $sourceStorageAccountObject.Context -Force
        # Upload file to blob storage but without changing the name
        Set-AzStorageBlobContent -File $targetStorageProperties.BlobName -Blob $targetStorageProperties.BlobName  `
            -Container $TargetStorageContainerName -Context $TargetStorageAccountObject.Context -Force

        Write-Host "Upload completed."  
    }
}
#endregion Function Set-StorageBlobContents

#region Function Get-StorageAccountPropertiesFromURL
function Get-StorageAccountPropertiesFromURL {
    [CmdletBinding()][OutputType([PSCustomObject])]
    param (
        [Parameter(Mandatory = $true)]
        $StorageContentURL
    )

    $regex='https:\/\/(.*)?.(blob|file|dfs).core.windows.net\/([a-z0-9](?!.*--)[a-z0-9-]{1,61}[a-z0-9])\/(.*)?'
    # $accountName = $StorageContentURL | Select-String -Pattern $regex | ForEach-Object { $_.Matches.Groups[1].Value} 
    $matchGroups = $StorageContentURL | Select-String -Pattern $regex | ForEach-Object { $_.Matches.Groups} 

    $storageAccountObject = [PSCustomObject]@{
        AbsoluteUrl = $matchGroups[0].value
        StorageAccountName = $matchGroups[1].value
        StorageType = $matchGroups[2].value
        ContainerName = $matchGroups[3].value
        BlobName = $matchGroups[4].value
    }

    return $storageAccountObject
}
#endregion Function Get-StorageAccountPropertiesFromURL

#region Function Replace-StringTokens
function Replace-StringTokens {
    [CmdletBinding()][OutputType([string])]
    param (
        [Parameter(Mandatory = $true, ValueFromPipeline = $true)]
        $TextToReplace,

        [Parameter(Mandatory = $true)]
        $StorageAccountName
    )
    
    $updatedText = $TextToReplace -replace "##TARGET_STORAGE_ACCOUNT_NAME##", $StorageAccountName

    return $updatedText
}
#endregion Function Replace-StringTokens

#region Main

"##[section]Get storage account"
$storageAccountObject = Get-AzStorageAccount -ResourceGroupName $StorageAccountResourceGroupName -StorageAccountName $StorageAccountName

foreach ($containerName in $containerNames) {
    "##[section]Create storage account container: $containerName"
    New-StorageContainer -StorageAccountObject $storageAccountObject -StorageContainerName $containerName

    "##[section]Create storage account container directories for: $containerName"
    New-StorageContainerDirectories -StorageAccountObject $storageAccountObject -StorageContainerName $containerName -StorageContainerDirectories $containerDirectoryNames

    # "##[section]Copy blob contents to storage account"
    Set-StorageBlobContents -TargetStorageAccountObject $storageAccountObject -TargetStorageContainerName $containerName -StorageContentFilePath $StorageContentFilePath

}



#endregion Main
