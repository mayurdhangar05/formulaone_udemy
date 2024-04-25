# Databricks notebook source
def mount_adls(storage_account_name,container_name):
    # Get secrets from key vault
    application_id = dbutils.secrets.get(scope="formula1-udemy",key="kv-formulaone-application-id")
    directory_id = dbutils.secrets.get(scope="formula1-udemy",key="kv-formulaone-directory-id")
    service_credential = dbutils.secrets.get(scope="formula1-udemy",key="kv-formulaone-service-credential")

    # set spark configuration
    configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": application_id,
           "fs.azure.account.oauth2.client.secret": service_credential,
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token"}
    
    # unmount the mount point if already exist
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    # Mount the storage account container
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)

    display(dbutils.fs.mounts())


# COMMAND ----------

mount_adls('bwtformula1project','formulaone')
