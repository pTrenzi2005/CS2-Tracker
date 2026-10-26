# GitHub Actions signs in to Azure with OIDC as one of these identities, then the Databricks CLI uses the
# Azure CLI token. Managed identities need no app registration and no Databricks account admin.
# Their Databricks side (workspace access, catalog grants) is in infra/databricks.

locals {
  github_issuer = "https://token.actions.githubusercontent.com"

  github_identities = {
    # Deploys the bundle on merge to main, owns what the bundle creates, and runs the PROD workloads.
    deployer = {
      name = "id-cs2-prod-deployer"
      subjects = {
        environment-prod = "repo:${var.github_repository}:environment:prod"
      }
    }
    # Read only. Runs bundle plan on pull requests and the nightly drift check on main.
    planner = {
      name = "id-cs2-prod-planner"
      subjects = {
        pull-request = "repo:${var.github_repository}:pull_request"
        branch-main  = "repo:${var.github_repository}:ref:refs/heads/main"
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
