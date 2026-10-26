locals {
  tags = {
    project    = "cs2-tracker"
    env        = "dev"
    managed_by = "terraform"
  }

  # The DEV external locations, created by hand before this stack. The ci target keeps schema storage in
  # <layer>/ci/uc and the raw volume in bronze/ci/raw, so CI never writes next to dev data.
  location_privileges = {
    cs2_bronze_loc = ["CREATE_MANAGED_STORAGE", "CREATE_EXTERNAL_VOLUME", "READ_FILES", "WRITE_FILES"]
    cs2_silver_loc = ["CREATE_MANAGED_STORAGE"]
    cs2_gold_loc   = ["CREATE_MANAGED_STORAGE"]
  }
}

data "azurerm_resource_group" "dev" {
  name = var.resource_group_name
}

# Pull requests sign in as this identity and deploy the ci target. It reaches nothing in PROD.
resource "azurerm_user_assigned_identity" "ci" {
  name                = "id-cs2-ci"
  resource_group_name = data.azurerm_resource_group.dev.name
  location            = var.location
  tags                = local.tags
}

resource "azurerm_federated_identity_credential" "ci" {
  name                      = "pull-request"
  user_assigned_identity_id = azurerm_user_assigned_identity.ci.id
  issuer                    = "https://token.actions.githubusercontent.com"
  audience                  = ["api://AzureADTokenExchange"]
  subject                   = "repo:${var.github_subject_repository}:pull_request"
}

# Creating a SQL warehouse needs allow_cluster_create. The workspace is serverless, so no classic cluster exists.
# The DEV secret scope gives users MANAGE, so the pipeline can read the connection string without an ACL here.
resource "databricks_service_principal" "ci" {
  application_id        = azurerm_user_assigned_identity.ci.client_id
  display_name          = "id-cs2-ci"
  workspace_access      = true
  databricks_sql_access = true
  allow_cluster_create  = true
}

# Test data only, so force_destroy lets the stack drop it with its schemas.
resource "databricks_catalog" "ci" {
  name           = var.catalog_name
  comment        = "Integration tests from pull requests. Schemas come from the bundle's ci target."
  storage_root   = "abfss://bronze@${var.storage_account_name}.dfs.core.windows.net/ci/catalog"
  isolation_mode = "ISOLATED"
  force_destroy  = true
}

resource "databricks_workspace_binding" "ci" {
  securable_type = "catalog"
  securable_name = databricks_catalog.ci.name
  workspace_id   = var.workspace_id
}

# MANAGE lets CI grant the app's service principal USE CATALOG, which the app's volume resource does.
resource "databricks_grant" "catalog_ci" {
  catalog    = databricks_catalog.ci.name
  principal  = databricks_service_principal.ci.application_id
  privileges = ["ALL_PRIVILEGES", "MANAGE"]
}

resource "databricks_grant" "location_ci" {
  for_each          = local.location_privileges
  external_location = each.key
  principal         = databricks_service_principal.ci.application_id
  privileges        = each.value
}
