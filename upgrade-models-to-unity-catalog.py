# Databricks notebook source
# MAGIC %md
# MAGIC # Overview
# MAGIC This notebook demonstrates how to use model registry APIs to upgrade existing models in the workspace Model Registry to Unity Catalog.
# MAGIC
# MAGIC ## Limitations
# MAGIC * The migration helpers in this notebook do not configure model permissions in Unity Catalog; upgraded models are owned by the user running the notebook.
# MAGIC * Model versions in Unity Catalog do not support tags; tags are not migrated.
# MAGIC * Model versions in Unity Catalog do not support fixed stages, as they will soon be deprecated in MLflow. Instead, stages are migrated to [aliases](https://mlflow.org/docs/latest/registry.html#using-registered-model-aliases). The "Champion" alias is assigned to the latest model version in the "Production" stage (if any), and the "Challenger" alias is assigned to the latest model version in the "Staging" stage.
# MAGIC * Model versions in Unity Catalog must have a [model signature](https://mlflow.org/docs/latest/models.html#model-signature), and model names in Unity Catalog must be valid Unity Catalog identifiers ([AWS](https://docs.databricks.com/sql/language-manual/sql-ref-identifiers.html)|[Azure](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-identifiers)|[GCP](https://docs.gcp.databricks.com/sql/language-manual/sql-ref-identifiers.html)). The APIs in this notebook print the names of all model and versions that cannot be upgraded.
# MAGIC * Model version upgrade is not idempotent; if you rerun the migration helpers with the same destination schema, the notebook recreates model versions that already exist. Consider selecting a new destination schema name rather than attempting to resume a failed migration.
# MAGIC
# MAGIC ### Requirements
# MAGIC * A cluster running Databricks Runtime 13.0 or above with access to Unity Catalog ([AWS](https://docs.databricks.com/data-governance/unity-catalog/get-started.html#create-a-cluster)|[Azure](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/get-started#create-a-cluster)|[GCP](https://docs.gcp.databricks.com/data-governance/unity-catalog/get-started.html#create-a-cluster)).
# MAGIC * Permissions to create models in at least one Unity Catalog schema. In particular, you need `USE CATALOG` permissions on the parent catalog, and both `USE SCHEMA` and `CREATE MODEL` permissions on the parent schema. If you get permissions errors while running the notebook, ask a catalog or schema owner or admin for access.

# COMMAND ----------

# MAGIC %pip install --upgrade "mlflow-skinny[databricks]"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## Configure catalog and schema
# MAGIC
# MAGIC To create a new catalog or schema, use `CREATE CATALOG` ([AWS](https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-create-catalog.html)|[Azure](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-catalog)|[GCP](https://docs.gcp.databricks.com/sql/language-manual/sql-ref-syntax-ddl-create-catalog.html)) or `CREATE SCHEMA` ([AWS](https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-create-schema.html)|[Azure](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-schema)|[GCP](https://docs.gcp.databricks.com/sql/language-manual/sql-ref-syntax-ddl-create-schema.html)), respectively.

# COMMAND ----------

DESTINATION_CATALOG = "main"
DESTINATION_SCHEMA = "migrated_models"

# COMMAND ----------

# Make sure the specified catalog and schema exist
spark.sql(f"USE CATALOG {DESTINATION_CATALOG}")
spark.sql(f"USE SCHEMA {DESTINATION_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC Define internal APIs for migration.

# COMMAND ----------

import mlflow
from mlflow.artifacts import download_artifacts
from mlflow.entities.model_registry.model_version_stages import (
    STAGE_PRODUCTION,
    STAGE_STAGING,
)
from mlflow.exceptions import MlflowException
from mlflow.protos.databricks_pb2 import RESOURCE_ALREADY_EXISTS, ErrorCode
from mlflow.tracking import MlflowClient


def _get_uc_model_name(catalog_name, schema_name, model_name):
    return f"{catalog_name}.{schema_name}.{model_name}"


def _convert_stage_to_alias(model_version):
    _STAGE_TO_ALIAS = {STAGE_PRODUCTION: "Champion", STAGE_STAGING: "Challenger"}
    latest_versions = MlflowClient().get_latest_versions(model_version.name)
    latest_version_by_stage = {mv.version: mv.current_stage for mv in latest_versions}
    stage = latest_version_by_stage.get(model_version.version, None)
    return _STAGE_TO_ALIAS.get(stage, None)


def _upgrade_model_version(model_name, version, dest_catalog, dest_schema, signature):
    """
    Upgrade an individual model version from the workspace model registry to Unity Catalog.
    The destination registered model is assumed to already exist with the specified name in Unity Catalog.
    """
    workspace_client = MlflowClient()
    uc_registry_client = MlflowClient(registry_uri="databricks-uc")
    orig_model_version = workspace_client.get_model_version(
        name=model_name, version=version
    )
    if signature is None:
        source = orig_model_version.source
    else:
        local_path = download_artifacts(f"models:/{model_name}/{version}")
        # Use the mlflow.models.set_signature API to add a signature to the model version
        mlflow.models.set_signature(local_path, signature)
        source = local_path
    uc_model_name = _get_uc_model_name(dest_catalog, dest_schema, model_name)
    uc_registry_client.create_model_version(
        name=uc_model_name,
        source=source,
        description=orig_model_version.description,
        run_id=orig_model_version.run_id,
    )


def _upgrade_registered_model(model_name, dest_catalog, dest_schema, upgrade_versions):
    workspace_client = MlflowClient()
    uc_registry_client = MlflowClient(registry_uri="databricks-uc")
    orig_registered_model = workspace_client.get_registered_model(name=model_name)
    # Create the registered model in UC
    uc_model_name = _get_uc_model_name(dest_catalog, dest_schema, model_name)
    try:
        # Note: tags are not yet supported for registered models in UC, so we don't migrate them upfront
        create_model_response = uc_registry_client.create_registered_model(
            name=uc_model_name, description=orig_registered_model.description
        )
    except MlflowException as e:
        if e.error_code == ErrorCode.Name(RESOURCE_ALREADY_EXISTS):
            pass
        else:
            raise e

    if upgrade_versions:
        page_token = None
        source_model_versions = []
        while True:
            search_res = workspace_client.search_model_versions(
                filter_string=f"name='{model_name}'", page_token=page_token
            )
            source_model_versions.extend(search_res)
            if search_res.token is None or len(search_res.token) == 0:
                break
            page_token = search_res.token
        for model_version in sorted(
            source_model_versions, key=lambda mv: int(mv.version)
        ):
            upgrade_model_version(
                model_name=model_name,
                version=model_version.version,
                dest_catalog=dest_catalog,
                dest_schema=dest_schema,
            )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Define migration APIs
# MAGIC You can call these APIs individually to migrate individual registered models or their constituent versions.

# COMMAND ----------


def upgrade_model_version(
    model_name,
    version,
    dest_catalog,
    dest_schema,
    signature: mlflow.models.ModelSignature = None,
):
    """
    Upgrade an individual model version from the workspace model registry to Unity Catalog.
    The destination registered model is assumed to already exist with the specified name in
    Unity Catalog.

    If model version upgrade fails due to lack of a model signature,
    you can manually specify one via the signature argument. See
    https://mlflow.org/docs/latest/models.html#how-to-log-models-with-signatures
    for details on how to specify a model signature.
    """
    try:
        _upgrade_model_version(model_name, version, dest_catalog, dest_schema, signature)
    except Exception as e:
        print(
            f"Failure upgrading version {version} of model '{model_name}' to destination schema '{dest_catalog}.{dest_schema}'. Got error {e}. Continuing..."
        )


def upgrade_registered_model(
    model_name, dest_catalog, dest_schema, upgrade_versions=True
):
    """
    Migrates a registered model and all its model versions from the workspace model
    registry to Unity Catalog
    :param model_name: Name of the model in the workspace model registry
    :param dest_catalog: Name of the catalog under which to create the model in Unity Catalog
    :param dest_schema: Name of the schema under which to create the model in Unity Catalog
    :param upgrade_versions: Whether to upgrade all versions under the registered model
    """
    try:
        _upgrade_registered_model(model_name, dest_catalog, dest_schema, upgrade_versions)
    except Exception as e:
        print(
            f"Failure upgrading model '{model_name}' to destination schema '{dest_catalog}.{dest_schema}'. Got error {e}. Continuing..."
        )


# COMMAND ----------

# MAGIC %md ### Call helpers to migrate models to UC
# MAGIC Call `upgrade_registered_model` on individual models to migrate them individually, or list models using `mlflow.search_registered_models` and iterate through to migrate. For example, you can tag registered models you'd like to migrate, use `search_registered_models` to find those models, and call `upgrade_registered_model` to migrate each of them.

# COMMAND ----------

# Specify a model name to migrate. If you do not specify a name here, the following command selects one to use.
MODEL_NAME = None

# COMMAND ----------

model_name = (
    MODEL_NAME
    or mlflow.search_registered_models(max_results=1, order_by=["name DESC"])[0].name
)

# COMMAND ----------

# Migrate the model
upgrade_registered_model(
    model_name, dest_catalog=DESTINATION_CATALOG, dest_schema=DESTINATION_SCHEMA
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Migrate model versions without signature
# MAGIC When you migrate registered models to Unity Catalog using the helpers defined above, some model versions might not migrate because they lack a model signature.
# MAGIC You can manually migrate model versions without a signature by passing `signature` to the
# MAGIC `upgrade_model_version` helper function. For more information about how to construct a model signature, see the [MLflow docs](https://mlflow.org/docs/latest/models.html#model-signature-and-input-example).

# COMMAND ----------

# MAGIC %md
# MAGIC First, construct a model signature.

# COMMAND ----------

from mlflow.models.signature import ModelSignature
from mlflow.types.schema import ColSpec, Schema

# COMMAND ----------


input_schema = Schema(
    [
        ColSpec("double", "sepal length (cm)"),
        ColSpec("double", "sepal width (cm)"),
        ColSpec("double", "petal length (cm)"),
        ColSpec("double", "petal width (cm)"),
        ColSpec("string", "class", optional=True),
    ]
)
output_schema = Schema([ColSpec("long")])
signature = ModelSignature(inputs=input_schema, outputs=output_schema)


# COMMAND ----------

# MAGIC %md
# MAGIC Then, call `upgrade_model_version`, passing the `signature` explicitly

# COMMAND ----------

# TODO: replace model_name and version with the name and version of the model version you'd like to upgrade
# upgrade_model_version(model_name=model_name, version=1, dest_catalog=DESTINATION_CATALOG, dest_schema=DESTINATION_SCHEMA, signature=signature)
