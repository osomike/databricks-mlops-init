# Databricks ML Use-Case Initialization Workflow, by Digital Power

This repository contains a GitHub Actions workflow that automates the setup of a new ML project for Databricks, complete with CI/CD and MLOps best practices. The workflow creates a new repository for the specified ML use case, configures necessary secrets, and initializes the Databricks bundle for the project. The goal is to standardize and simplify the process of starting new ML projects within the organization.

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
- **`model_schema`** (optional): The schema where models will be stored. Default is `"meerlanden_schema"`.
- **`inference_table`** (optional): The name of the table where inferences will be stored. Default is `"meerlanden_inference_table"`.
- **`include_feature_store`** (optional): Whether to include a Databricks Feature Store in the project. Options: `"yes"` or `"no"`. Default is `"no"`.

### Secrets

The workflow requires the following GitHub Secrets:

- **`ADMIN_GITHUB_TOKEN`**: A GitHub token with admin access to the repositories, required for creating new repositories and setting secrets.
- **`DATABRICKS_STAGING_CLIENT_SECRET`**: The secret for authenticating with the Databricks staging workspace.
- **`DATABRICKS_PROD_CLIENT_SECRET`**: The secret for authenticating with the Databricks production workspace.

### Environment Variables

The following environment variables must be set at the organization or repository level (`Organization Settings > Secrets and Variables > Variables`):

- **`DATABRICKS_STAGING_WORKSPACE_HOST`**: The Databricks workspace host URL for the staging environment.
- **`DATABRICKS_PROD_WORKSPACE_HOST`**: The Databricks workspace host URL for the production environment.
- **`DATABRICKS_STAGING_CLIENT_ID`**: The client ID for authenticating with the Databricks staging environment.
- **`DATABRICKS_PROD_CLIENT_ID`**: The client ID for authenticating with the Databricks production environment.
- **`DATABRICKS_CLOUD`**: The cloud provider hosting Databricks (e.g., `"azure"`).
- **`DATABRICKS_CICD_PLATFORM`**: The platform used for CI/CD (e.g., `"github"`).
- **`DATABRICKS_READ_USER_GROUP`**: The user group that should have read access to the project.
- **`DATABRICKS_UNITY_CATALOG_READ_USER_GROUP`**: The Unity Catalog read user group.
- **`DATABRICKS_STAGING_CATALOG_NAME`**: The catalog name for the staging environment.
- **`DATABRICKS_PROD_CATALOG_NAME`**: The catalog name for the production environment.
- **`DATABRICKS_TEST_CATALOG_NAME`**: The catalog name for the testing environment.

## Workflow Steps

### 1. Repository Creation

The workflow starts by creating a new private GitHub repository with the name provided as `project_name`. It also grants admin access to specified collaborators and sets up the necessary secrets for Databricks integration.

### 2. Databricks CLI Setup

The workflow installs and configures the Databricks CLI to interact with your Databricks workspaces. The staging environment is used by default for initial setup.

### 3. Databricks Bundle Initialization

Using the `databricks bundle init` command, the workflow initializes a new MLops stack based on the provided configuration (`config.json`). This configuration includes details such as:

- Workspace URLs (staging and production)
- Catalog and schema names
- Branch settings for CI/CD

### 4. Git Initialization and Setup

After initializing the Databricks bundle, the workflow sets up a local Git repository, adds the project files, commits them, and pushes the new repository to GitHub.

### 5. Service Principal Modifications (Azure-specific)

If Azure is the cloud provider, the workflow modifies the service principal credentials in the project files to match Databricks requirements.

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
     - Test Catalog (if applicable)

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
   - A **GitHub Personal Access Token (PAT)** with the necessary permissions on the organization githuab space is required to automate repository creation and configure secrets/variables:
     - Permissions needed:
       - **Repository creation**: To create new private repositories for each ML use case.
       - **Manage secrets**: To configure environment variables and secrets in the repository for secure authentication (e.g., Databricks workspace credentials).
       - **Assign roles**: To assign collaborators with admin, write, or read permissions to the newly created repository.
     - The token should be stored as `ADMIN_GITHUB_TOKEN` in the GitHub Secrets.


## How to Use

1. Trigger the workflow manually via the `Actions` tab in the repository.
2. Provide the necessary inputs, such as the project name and optional parameters like model schema and inference table name.
3. The workflow will:
   - Create a new private repository in GitHub with the provided project name.
   - Initialize a Databricks MLops project.
   - Set up required secrets and configurations.
   - Push the initial project files to the new repository.

## Example

To create a new ML project named `awesome-ml-project` with a custom model schema and inference table, trigger the workflow with the following inputs:

- `project_name`: `awesome-ml-project`
- `model_schema`: `custom_schema`
- `inference_table`: `custom_inference_table`
- `include_feature_store`: `yes`

The workflow will then initialize the project with these configurations and set up the repository in GitHub.

## Additional Notes

- This workflow is designed to work with Databricks on Azure, but can be extended for use with other cloud providers by modifying the cloud-related steps.
- For more information on Databricks MLOps Stacks, visit the [official Databricks MLOps Stacks repository](https://github.com/databricks/mlops-stacks).
