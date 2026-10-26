# Workspace principals. Without account admin, groups created here are workspace-local: they work in job,
# pipeline, and app permissions (the bundle's cs2-operators and cs2-readers), but not in Unity Catalog grants.
# Catalog grants therefore name service principals, the operator, and account users.

data "databricks_group" "admins" {
  display_name = "admins"
}

data "databricks_user" "operator" {
  user_name = var.operator_user_name
}

# Contributor on the resource group already makes it an admin at sign-in. Explicit, so CI can run Terraform
# before the identity ever signs in.
resource "databricks_service_principal" "terraform" {
  application_id = local.azure.terraform_client_id
  display_name   = "id-cs2-terraform"
}

resource "databricks_group_member" "terraform_admin" {
  group_id  = data.databricks_group.admins.id
  member_id = databricks_service_principal.terraform.id
}

# Creating a SQL warehouse needs allow_cluster_create. The workspace is serverless, so no classic cluster exists.
resource "databricks_service_principal" "deployer" {
  application_id        = local.azure.deployer_client_id
  display_name          = "id-cs2-prod-deployer"
  workspace_access      = true
  databricks_sql_access = true
  allow_cluster_create  = true
}

resource "databricks_service_principal" "planner" {
  application_id   = local.azure.planner_client_id
  display_name     = "id-cs2-prod-planner"
  workspace_access = true
}

resource "databricks_group" "operators" {
  display_name          = "cs2-operators"
  workspace_access      = true
  databricks_sql_access = true
}

resource "databricks_group" "readers" {
  display_name          = "cs2-readers"
  workspace_access      = true
  databricks_sql_access = true
}

resource "databricks_group_member" "operator" {
  group_id  = databricks_group.operators.id
  member_id = data.databricks_user.operator.id
}

resource "databricks_group_member" "planner" {
  group_id  = databricks_group.readers.id
  member_id = databricks_service_principal.planner.id
}
