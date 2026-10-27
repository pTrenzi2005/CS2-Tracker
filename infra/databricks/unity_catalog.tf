# DEV and PROD share the account's swedencentral metastore, so every securable here is bound to the PROD
# workspace only. The bundle creates the schemas and the volume inside the catalog.
# Grants use databricks_grant, which leaves other principals alone: the bundle and the app add their own grants.

resource "databricks_storage_credential" "lake" {
  name           = "cs2_prod_storage_cred"
  comment        = "Access connector of the PROD storage account."
  owner          = local.uc_owner
  isolation_mode = "ISOLATION_MODE_ISOLATED"

  azure_managed_identity {
    access_connector_id = local.azure.access_connector_id
  }
}

resource "databricks_workspace_binding" "storage_credential" {
  securable_type = "storage_credential"
  securable_name = databricks_storage_credential.lake.name
  workspace_id   = local.workspace_id
}

resource "databricks_grant" "terraform_storage_credential" {
  storage_credential = databricks_storage_credential.lake.name
  principal          = databricks_service_principal.terraform.application_id
  privileges         = ["MANAGE"]
}

resource "databricks_external_location" "lake" {
  for_each        = local.location_privileges
  name            = "cs2_prod_${each.key}"
  url             = "abfss://${each.key}@${local.azure.storage_account_name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.lake.name
  comment         = "PROD ${each.key} container."
  owner           = local.uc_owner
  isolation_mode  = "ISOLATION_MODE_ISOLATED"

  depends_on = [databricks_workspace_binding.storage_credential]
}

resource "databricks_workspace_binding" "external_location" {
  for_each       = databricks_external_location.lake
  securable_type = "external_location"
  securable_name = each.value.name
  workspace_id   = local.workspace_id
}

resource "databricks_grant" "terraform_location" {
  for_each          = databricks_external_location.lake
  external_location = each.value.name
  principal         = databricks_service_principal.terraform.application_id
  privileges        = ["MANAGE"]
}

resource "databricks_grant" "deployer_location" {
  for_each          = { for name, privileges in local.location_privileges : name => privileges if length(privileges) > 0 }
  external_location = databricks_external_location.lake[each.key].name
  principal         = databricks_service_principal.deployer.application_id
  privileges        = each.value
}

resource "databricks_catalog" "prod" {
  name           = var.catalog_name
  comment        = "PROD medallion. Schemas and the raw volume come from the bundle, deployed by CD only."
  storage_root   = databricks_external_location.lake["catalog"].url
  owner          = local.uc_owner
  isolation_mode = "ISOLATED"

  depends_on = [databricks_workspace_binding.external_location]

  lifecycle {
    prevent_destroy = true
  }
}

resource "databricks_workspace_binding" "catalog" {
  securable_type = "catalog"
  securable_name = databricks_catalog.prod.name
  workspace_id   = local.workspace_id
}

resource "databricks_grant" "catalog_terraform" {
  catalog    = databricks_catalog.prod.name
  principal  = databricks_service_principal.terraform.application_id
  privileges = ["MANAGE"]
}

# MANAGE lets the deployer grant the app's service principal USE CATALOG, which the app's volume resource does.
resource "databricks_grant" "catalog_deployer" {
  catalog    = databricks_catalog.prod.name
  principal  = databricks_service_principal.deployer.application_id
  privileges = ["ALL_PRIVILEGES", "MANAGE"]
}

# The binding limits account users to people in the PROD workspace. SELECT on gold comes from the bundle.
resource "databricks_grant" "catalog_readers" {
  catalog    = databricks_catalog.prod.name
  principal  = "account users"
  privileges = ["USE_CATALOG"]
}
