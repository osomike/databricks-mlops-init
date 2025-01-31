# Databricks ML Use-Case Initialization Workflow, by Digital Power

This repository contains a GitHub Actions workflow that automates the setup of a new ML project for Databricks, completed with CI/CD and MLOps best practices. The workflow creates a new repository for the specified ML use case, configures necessary secrets, and initializes the Databricks bundle for the project. The goal is to standardize and simplify the process of starting new ML projects within the organization.

## Features

- **Automatic repository creation**: The workflow creates a new private repository in GitHub for the ML use case.
- **Databricks bundle initialization**: Sets up a Databricks MLOps project using [Databricks MLOps Stacks](https://github.com/databricks/mlops-stacks), configuring the environment for CI/CD.
- **Secrets management**: Automatically configures required secrets for both staging and production Databricks environments.
- **Feature Store (optional)**: Allows the inclusion of a feature store based on user input.
- **Git setup**: Initializes the local Git repository and pushes it to the newly created GitHub repo.

## Workflow Overview

### Trigger

The workflow can be triggered manually via the `workflow_dispatch` event. When triggered, it prompts the user to input specific parameters for the new ML project.

### Inputs

- **`project_name`** (required): The name of the new ML use case. This name will also be used to create the GitHub repository.
- **`github_contributor_group`** (required): The name of the GitHub team group with contributor rights. This group will be granted access to contribute to the newly created repository.
- **`github_admins_group`** (required): The name of the GitHub team group with administrator rights. This group will be granted access to administrate the newly created repository.
- **`model_schema`** (optional): The schema where models will be stored. This schema has to be present in staging and production before triggering the workflow. Default is `"all_my_ml_models"`.
- **`include_feature_store`** (optional): Whether to include a Databricks Feature Store in the project. Options: `yes` or `no` (default). If `no`, the `feature_engineering` folder and job are excluded, and preprocessing must be implemented manually in the training and inference jobs.

### Secrets

The workflow requires the following GitHub Secrets, in an environment named `default`:

- **`ADMIN_GITHUB_TOKEN`**: A GitHub token with admin access to the repositories, required for creating new repositories and setting secrets. For more details [check this section](#6-github-token-with-adequate-permissions)
- **`DATABRICKS_STAGING_CLIENT_SECRET`**: The service principal client secret for authenticating with the staging databricks workspace.
- **`DATABRICKS_PROD_CLIENT_SECRET`**: The service principal client secret for authenticating with the production databricks workspace.
- **`DATABRICKS_TOKEN`**: Used to initialize the databricks mlops bundle, can be any authentication token for any workspace.

### Environment Variables

The following environment variables must be set at the organization (`Organization Settings > Secrets and Variables > Variables`) or repository level, in an environment named `default`:

- **`DATABRICKS_CICD_PLATFORM`**: The platform used for CI/CD (e.g., `github_actions`).
- **`DATABRICKS_CLOUD`**: The cloud provider hosting Databricks (e.g., `azure`).
- **`DATABRICKS_PROD_CATALOG_NAME`**: The catalog name for the production environment. Note.- Keep in mind that this name will also be used in the bundle as the production target name, and associate it to the prd workspace and the prd service principal. Once the repository is initalized check the file `/<project_name>/databricks.yml`
- **`DATABRICKS_PROD_CLIENT_ID`**: The service principal client ID for authenticating with the Databricks production environment.
- **`DATABRICKS_PROD_TENANT_ID`**: The tenant ID where the service principal is registered, for authenticating with the Databricks production environment. Only needed if Azure service principals are used. Check variable `USE_DATABRICKS_SERVICE_PRINCIPAL`
- **`DATABRICKS_PROD_WORKSPACE_HOST`**: The Databricks workspace host URL for the production environment. (e.g., `https://adb-xxx.azuredatabricks.net`)
- **`DATABRICKS_STAGING_CATALOG_NAME`**: The catalog name for the staging environment. Note.- Keep in mind that this name will also be used in the bundle as the staging target name, and associate it to the staging workspace and the staging service principal. Once the repository is initalized check the file `/<project_name>/databricks.yml`
- **`DATABRICKS_STAGING_CLIENT_ID`**: The service principal client ID for authenticating with the Databricks staging environment.
- **`DATABRICKS_STAGING_TENANT_ID`**: The tenant ID where the service principal is registered, for authenticating with the Databricks staging environment. Only needed if Azure service principals are used. Check variable `USE_DATABRICKS_SERVICE_PRINCIPAL`
- **`DATABRICKS_STAGING_WORKSPACE_HOST`**: The Databricks workspace host URL for the staging environment. (e.g., `https://adb-xxx.azuredatabricks.net`)
- **`DATABRICKS_TEST_CATALOG_NAME`**: The catalog name for the test environment. Note.- Keep in mind that this name will also be used in the bundle as the test target name, and associate it to the staging workspace and the staging service principal. Once the repository is initalized check the file `/<project_name>/databricks.yml`
- **`DATABRICKS_UNITY_CATALOG_READ_USER_GROUP`**: The Unity Catalog read user group.
- **`DATABRICKS_READ_USER_GROUP`**: The user group inside databricks that should have read access to the databricks workflows for the ml project.
- **`USE_DATABRICKS_SERVICE_PRINCIPAL`**: Feature flag. If `true`, use Databricks service principals instead of cloud provider service principals.

## Workflow Steps

### 1. Repository Creation

The workflow starts by creating a new private GitHub repository with the name provided as `project_name`. It also grants access to specified admins and  collaborators and sets up the necessary secrets for Databricks integration.

### 2. Databricks Bundle Initialization

Using the `databricks bundle init` command, the workflow initializes a new MLops stack based on the provided configuration (`config.json`). This configuration includes details from inputs, secrets, and environment variables, such as:

- Workspace URLs (staging and production)
- Catalog and schema names
- Branch settings for CI/CD

### 3. Git Initialization and Setup

After initializing the Databricks bundle, the workflow sets up a local Git repository, adds the project files, and commits them.

### 4. Modification of the Bundle After Initialization

The workflow modifies the project files to run job clusters in single-user access mode for the service principal. This is necessary because [it is not yet supported by the bundle](https://github.com/databricks/mlops-stacks/issues/140), and [Databricks runtime ML is not supported in shared mode](https://docs.databricks.com/en/compute/access-mode-limitations.html#shared-access-mode-limitations-on-unity-catalog). To avoid having to fork the MLOps Stacks repository, this is implemented with [sed](https://www.gnu.org/software/sed/manual/html_node/The-_0022s_0022-Command.html#The-_0022s_0022-Command) and added as a separate commit.

### 5. (Optional) Service Principal Modifications

By default, the project uses service principals from the cloud provider. If the enviroment variable `USE_DATABRICKS_SERVICE_PRINCIPAL` is specified, the workflow modifies the project files to use [Databricks service principals](https://docs.databricks.com/en/dev-tools/auth/oauth-m2m.html#finish-configuring-oauth-m2m-authentication). This is only supported when `DATABRICKS_CLOUD` is `"azure"`. To avoid having to fork the MLOps Stacks repository, this is implemented with [sed](https://www.gnu.org/software/sed/manual/html_node/The-_0022s_0022-Command.html#The-_0022s_0022-Command) and added as a separate commit.

### 6. Push to Remote Repository

Finally, the workflow pushes the initialized project to the newly created repository on GitHub.

---

## Organizational Prerequisites for ML Project initialization deployment

Before deploying this workflow in your organization, ensure that the following requirements are met to enable seamless creation and management 

### 1. Multiple Workspaces
   - The organization must have **at least two Databricks workspaces**:
     - **Development/Staging Workspace**: Used for development and testing of ML models.
     - **Production Workspace**: Used for deploying models in production environments.

### 2. Unity Catalog Enabled Workspaces
   - Each Databricks workspace where this workflow will be used **must have Unity Catalog enabled**.
   - Unity Catalog is essential for managing data governance, sharing, and access controls across the ML projects.

### 3. Catalogs and Schemas in Workspaces
   - The required **catalogs and schemas must be deployed in each workspace**:
     - Catalogs should already exist to store and manage ML models.
     - Schemas should be available for storing ML inferences, which will be specified in the workflow (`inference_table`).
   - Ensure the following catalog names are set up:
     - Staging Catalog
     - Production Catalog
     - Test Catalog

### 4. User Groups for ML Bundles
   - The organization should have **Databricks or Entra ID (Azure Active Directory) user groups** created to manage permissions for ML bundles.
     - Example groups:
       - `databricks-read-user-group`: Grants read access to specified users for the created ML bundles.
       - `databricks-unity-catalog-read-user-group`: Manages access to Unity Catalog for specific projects.
   - Ensure proper assignment of user groups for security and role-based access.

### 5. Service Principal and Tokens in Workspaces
   - Each Databricks workspace must have an associated **service principal** to handle the automated operations of the ML projects.
     - Create a service principal in your Azure environment and configure **OAuth 2.0 tokens** for authentication between Databricks and external systems (like GitHub).
     - This service principal will handle operations such as deploying bundles, pushing configurations, and managing jobs.

### 6. GitHub Token with Adequate Permissions
   - A **GitHub Personal Access Token (PAT)** with the necessary permissions on the organization github space is required to automate the repository creation and configure access and secrets/variables:
     - Repository permissions needed:
       - **Administration**: `Read and write` to create new private repositories for each ML use case and assign collaborators with
       admin, write, or read permissions to the newly created repository.
       - **Commit statuses**: `Read and write` to add commits to the new repositories.
       - **Contents**: `Read and write` to access repository contents, commits, branches, downloads, releases, and merges.
       - **Environments**: `Read and write` to manage repository environments.
       - **Metadata**: `Read-only` to read metadata.
       - **Secrets**: `Read and write` to configure environment variables and secrets in the repository for secure authentication (e.g., Databricks workspace credentials).
       - **Workflows**: `Read and write` to update GitHub Action workflow files from the newly created repositories.

     - Organization permissions needed:
       - **Members**: `Read-Only` to be able to read the groups defined inside the organizations. To then be able to grant access to the newly created use-case repository to these groups.

## How to Use

1. Trigger the workflow manually via the `Actions` tab in the repository.
2. Provide the necessary inputs, such as the project name and optional parameters like model schema and inference table name.
3. The workflow will:
   - Create a new private repository in GitHub with the provided project name.
   - Initialize a Databricks MLops project.
   - Set up required secrets and configurations.
   - Push the initial project files to the new repository.
4. If you would like to run the default workflows make sure to validate the parameters passed to the different steps. This configuration is on `/<project_name>/resources/*.yml` files. You should at least update the `input_table_name` for the inference batch job, check file `/<project_name>/resources/batch-inference-workflow-resource.yml`

## Example

To create a new ML project named `awesome-ml-project` with a custom model schema and inference table, trigger the workflow with the following inputs:

- `project_name`: `awesome-ml-project`
- `github_contributor_group`: `github_ml_contributors`
- `github_admins_group`: `github_ml_admins`
- `model_schema`: `custom_schema`
- `include_feature_store`: `yes`

The workflow will then initialize the project with these configurations and set up the repository in GitHub.

## Additional Notes

- This workflow is designed to work with Databricks on Azure, but can be extended for use with other cloud providers by modifying the cloud-related steps.
- For more information on Databricks MLOps Stacks, visit the [official Databricks MLOps Stacks repository](https://github.com/databricks/mlops-stacks).
