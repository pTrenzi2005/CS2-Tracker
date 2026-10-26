<#
.SYNOPSIS
    One-time setup that Terraform cannot do for itself. Run as a subscription Owner after az login.

.DESCRIPTION
    DEV and PROD share one subscription. This script creates in it:
      rg-cs2-tfstate        the Terraform state account and the Terraform identity
      stcs2trackertfstate   container tfstate. Entra ID access only, blob versioning, 30 days of soft delete
      id-cs2-terraform      managed identity for GitHub Actions, federated with environment infra (apply)
                            and pull_request (plan)
      rg-cs2-prod           resource group for the PROD stack. Terraform creates everything inside it

    id-cs2-terraform gets Contributor and User Access Administrator on rg-cs2-prod only, plus what it needs
    to add the PROD listen rule to the shared Event Hub. It cannot change DEV.

    The script never calls Microsoft Graph and needs no app registration, so the tenant's MFA rule for
    Graph and its app registration policy do not block it. Running it again changes nothing.
    If you change a state parameter, change infra/backend.hcl and the state variables of infra/databricks too.

.EXAMPLE
    az login
    ./infra/bootstrap/bootstrap.ps1
#>
[CmdletBinding()]
param(
    [string]$SubscriptionId = '3fc38270-4a82-4fa3-8e33-99c47ac04c9e',
    [string]$Location = 'swedencentral',
    [string]$GitHubRepository = 'pTrenzi2005/CS2-Tracker',
    # GitHub's OIDC subject names the repository as owner@owner_id/name@repository_id.
    [string]$GitHubSubjectRepository = 'pTrenzi2005@202133242/CS2-Tracker@1368773264',
    [string]$StateResourceGroup = 'rg-cs2-tfstate',
    [string]$StateStorageAccount = 'stcs2trackertfstate',
    [string]$StateContainer = 'tfstate',
    [string]$TerraformIdentity = 'id-cs2-terraform',
    [string]$ProdResourceGroup = 'rg-cs2-prod',
    [string]$EventHubResourceGroup = 'CS2-Tracker-Project',
    [string]$EventHubNamespace = 'evhcs2tracker',
    [string]$EventHubName = 'cs2tracker_evh'
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Invoke-Az {
    $output = & az @args --only-show-errors
    if ($LASTEXITCODE -ne 0) {
        throw "az $($args -join ' ') failed with exit code $LASTEXITCODE."
    }
    $output
}

function Invoke-AzJson {
    (Invoke-Az @args -o json) -join "`n" | ConvertFrom-Json
}

# The oid claim of the ARM token. Asking Graph for it would require MFA in this tenant.
function Get-SignedInObjectId {
    $token = Invoke-Az account get-access-token --query accessToken -o tsv
    $payload = $token.Split('.')[1].Replace('-', '+').Replace('_', '/')
    $payload = $payload.PadRight($payload.Length + (4 - $payload.Length % 4) % 4, '=')
    $claims = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($payload)) | ConvertFrom-Json
    $claims.oid
}

# Object IDs and an explicit principal type keep az away from Graph, and a new identity does not have to
# replicate first.
function Grant-Role {
    param(
        [string]$PrincipalId,
        [string]$PrincipalType,
        [string]$Role,
        [string]$Scope
    )
    $label = "  $Role on $(Split-Path $Scope -Leaf)"
    $count = Invoke-Az role assignment list --scope $Scope --role $Role --fill-principal-name false `
        --query "[?principalId=='$PrincipalId'] | length(@)" -o tsv
    if ([int]$count -gt 0) {
        Write-Host "$label`: exists"
        return
    }
    Invoke-Az role assignment create --assignee-object-id $PrincipalId --assignee-principal-type $PrincipalType `
        --role $Role --scope $Scope | Out-Null
    Write-Host "$label`: granted"
}

Write-Host "Subscription $SubscriptionId"
Invoke-Az account set --subscription $SubscriptionId | Out-Null
$tenantId = Invoke-Az account show --query tenantId -o tsv
$userObjectId = Get-SignedInObjectId

$subscriptionScope = "/subscriptions/$SubscriptionId"
$stateGroupScope = "$subscriptionScope/resourceGroups/$StateResourceGroup"
$prodScope = "$subscriptionScope/resourceGroups/$ProdResourceGroup"
$namespaceScope = "$subscriptionScope/resourceGroups/$EventHubResourceGroup/providers/Microsoft.EventHub/namespaces/$EventHubNamespace"
$eventHubScope = "$namespaceScope/eventhubs/$EventHubName"

Write-Host 'Resource groups'
foreach ($group in @($StateResourceGroup, $ProdResourceGroup)) {
    Invoke-Az group create --name $group --location $Location --tags project=cs2-tracker managed_by=bootstrap | Out-Null
    Write-Host "  $group"
}

Write-Host 'Terraform state'
Invoke-Az storage account create --name $StateStorageAccount --resource-group $StateResourceGroup --location $Location `
    --sku Standard_LRS --kind StorageV2 --min-tls-version TLS1_2 --https-only true `
    --allow-blob-public-access false --allow-shared-key-access false | Out-Null
Invoke-Az storage account blob-service-properties update --account-name $StateStorageAccount `
    --resource-group $StateResourceGroup --enable-versioning true `
    --enable-delete-retention true --delete-retention-days 30 `
    --enable-container-delete-retention true --container-delete-retention-days 30 | Out-Null
$stateAccountId = Invoke-Az storage account show --name $StateStorageAccount --resource-group $StateResourceGroup --query id -o tsv
# container-rm goes through ARM, so no data-plane role is needed yet.
$containerExists = Invoke-Az storage container-rm exists --storage-account $StateStorageAccount `
    --resource-group $StateResourceGroup --name $StateContainer --query exists -o tsv
if ($containerExists -ne 'true') {
    Invoke-Az storage container-rm create --storage-account $StateStorageAccount --resource-group $StateResourceGroup `
        --name $StateContainer | Out-Null
}
Write-Host "  $StateStorageAccount/$StateContainer"

Write-Host "Identity $TerraformIdentity"
$identity = Invoke-AzJson identity create --name $TerraformIdentity --resource-group $StateResourceGroup --location $Location
$credentials = [ordered]@{
    'github-infra'        = "repo:${GitHubSubjectRepository}:environment:infra"
    'github-pull-request' = "repo:${GitHubSubjectRepository}:pull_request"
}
foreach ($name in $credentials.Keys) {
    Invoke-Az identity federated-credential create --identity-name $TerraformIdentity --resource-group $StateResourceGroup `
        --name $name --issuer https://token.actions.githubusercontent.com --subject $credentials[$name] `
        --audiences api://AzureADTokenExchange | Out-Null
    Write-Host "  federated: $($credentials[$name])"
}

Write-Host "Roles for $TerraformIdentity"
Grant-Role -PrincipalId $identity.principalId -PrincipalType ServicePrincipal -Role 'Contributor' -Scope $prodScope
# The access connector needs a role on the PROD storage account.
Grant-Role -PrincipalId $identity.principalId -PrincipalType ServicePrincipal -Role 'User Access Administrator' -Scope $prodScope
# infra/azure reads this identity's client ID for infra/databricks.
Grant-Role -PrincipalId $identity.principalId -PrincipalType ServicePrincipal -Role 'Reader' -Scope $stateGroupScope
Grant-Role -PrincipalId $identity.principalId -PrincipalType ServicePrincipal -Role 'Storage Blob Data Contributor' -Scope $stateAccountId
# The PROD listen rule lives on the shared hub. Contributor on the hub alone leaves the rest of DEV untouched.
Grant-Role -PrincipalId $identity.principalId -PrincipalType ServicePrincipal -Role 'Reader' -Scope $namespaceScope
Grant-Role -PrincipalId $identity.principalId -PrincipalType ServicePrincipal -Role 'Contributor' -Scope $eventHubScope

Write-Host 'Roles for you (local terraform runs)'
Grant-Role -PrincipalId $userObjectId -PrincipalType User -Role 'Storage Blob Data Contributor' -Scope $stateAccountId

$repositoryVariables = [ordered]@{
    AZURE_TENANT_ID       = $tenantId
    AZURE_SUBSCRIPTION_ID = $SubscriptionId
    TF_CLIENT_ID          = $identity.clientId
}

Write-Host 'GitHub'
if (Get-Command gh -ErrorAction SilentlyContinue) {
    foreach ($environment in @('infra', 'prod')) {
        gh api --method PUT "repos/$GitHubRepository/environments/$environment" --silent
        if ($LASTEXITCODE -ne 0) { throw "Creating the GitHub environment $environment failed." }
        Write-Host "  environment $environment"
    }
    foreach ($name in $repositoryVariables.Keys) {
        gh variable set $name --repo $GitHubRepository --body $repositoryVariables[$name]
        if ($LASTEXITCODE -ne 0) { throw "Setting the GitHub variable $name failed." }
        Write-Host "  variable $name"
    }
}
else {
    Write-Host '  gh is not installed. In the repository settings, create the environments infra and prod,'
    Write-Host '  and these repository variables:'
    foreach ($name in $repositoryVariables.Keys) {
        Write-Host "    $name = $($repositoryVariables[$name])"
    }
}

Write-Host ''
Write-Host 'Done. Role assignments can take a few minutes to apply. Then:'
Write-Host '  terraform -chdir=infra/azure init -backend-config="../backend.hcl"'
Write-Host '  terraform -chdir=infra/azure apply'
Write-Host '  terraform -chdir=infra/databricks init -backend-config="../backend.hcl"'
Write-Host '  terraform -chdir=infra/databricks apply'
