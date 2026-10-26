# GitHub Actions signs in to Azure with OIDC as one of these identities, then the Databricks CLI uses the
# Azure CLI token. Managed identities need no app registration and no Databricks account admin.
# Their Databricks side (workspace access, catalog grants) is in infra/databricks.

locals {
  github_issuer = "https://token.actions.githubusercontent.com"

  github_identities = {
    # Plans and deploys the bundle on merge to main, runs the nightly drift check, owns what the bundle creates,
    # and runs the PROD workloads. Only jobs in the prod environment can sign in as it. Pull requests cannot:
    # an app must be deployed by its run_as identity, so a separate read-only planner could not load the bundle.
    deployer = {
      name = "id-cs2-prod-deployer"
      subjects = {
        environment-prod = "repo:${var.github_repository}:environment:prod"
      }
    }
  }

  github_credentials = merge([
    for identity, settings in local.github_identities : {
      for name, subject in settings.subjects : "${identity}/${name}" => {
        identity = identity
        name     = name
        subject  = subject
      }
    }
  ]...)
}

data "azurerm_user_assigned_identity" "terraform" {
  name                = var.terraform_identity.name
  resource_group_name = var.terraform_identity.resource_group_name
}

resource "azurerm_user_assigned_identity" "github" {
  for_each            = local.github_identities
  name                = each.value.name
  resource_group_name = data.azurerm_resource_group.prod.name
  location            = var.location
  tags                = local.tags
}

resource "azurerm_federated_identity_credential" "github" {
  for_each                  = local.github_credentials
  name                      = each.value.name
  user_assigned_identity_id = azurerm_user_assigned_identity.github[each.value.identity].id
  issuer                    = local.github_issuer
  audience                  = ["api://AzureADTokenExchange"]
  subject                   = each.value.subject
}
