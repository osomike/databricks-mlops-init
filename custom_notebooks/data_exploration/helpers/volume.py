import os
import io
import uuid
import json
import sys
import pandas as pd

from pyspark.sql import SparkSession

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config


def get_databricks_profile() -> str:
    folder_path = os.path.join(
        sys.prefix.replace(os.path.basename(os.environ.get('VIRTUAL_ENV', '')), ''),
        ".databricks",
        "project.json"
    )
    
    if not os.path.exists(folder_path):
        folder_paths = [
            '../../../.databricks/project.json',
            '../../.databricks/project.json',
            '../.databricks/project.json',
            '.databricks/project.json'
        ]

        for path in folder_paths:
            if os.path.exists(path):
                folder_path = path
                break
        else:
            raise FileNotFoundError("Could not find '.databricks' folder. Make sure you have Databricks extension for Visual Studio Code enabled.")

    with open(folder_path) as file:
        config = json.load(file)

    if config.get('authType') != 'profile':
        raise ValueError("'authType' is missing or not set to 'profile'. Make sure to authenticate using a Databricks CLI profile.")

    return config['profile']

    
class VolumeOperator:

    def __init__(self, profile: str, catalog: str, schema: str, volume_name: str) -> None:
        """
        Initializes a new instance of the `VolumeOperator` class.

        Args:
            profile (str): The profile to use for the WorkspaceClient.
            catalog (str): The name of the UC catalog.
            schema (str): The name of the schema.
            volume_name (str): The name of the volume.

        Returns:
            None
        """
        self.client = WorkspaceClient(config=Config(profile=profile if profile else get_databricks_profile()))
        self.volume_path = f"/Volumes/{catalog}/{schema}/{volume_name}/"

    def list_files(self, sub_folder: str = "") -> pd.DataFrame:
        return pd.DataFrame(self.client.files.list_directory_contents(os.path.join(self.volume_path, sub_folder)))

    def download_file(self, source_path: str, target_path: str = ".") -> None:
        """
        Downloads a file from the specified Volume source path to the target path.

        Args:
            source_path (str): The path of the file to download in the volume.
            target_path (str, optional): The path to save the downloaded file. Defaults to ".".

        Returns:
            None
        """
        file = self.client.files.download(file_path=os.path.join(self.volume_path, source_path))
        if target_path != "" and not os.path.exists(target_path):
            os.makedirs(target_path)
        with open(os.path.join(target_path, os.path.basename(source_path)), 'wb') as f:
            f.write(file.contents.read())
        print("File downloaded and saved successfully!")

    def download_directory(self, source_path: str, target_path: str = ".") -> None:
        """
        Downloads a directory recursively from the specified Volume source path to the target path.

        Args:
            source_path (str): The path of the directory to download in the volume.
            target_path (str, optional): The path to save the downloaded directory. Defaults to ".".

        Returns:
            None
        """
        contents = self.client.files.list_directory_contents(os.path.join(self.volume_path, source_path))
        for item in contents:
            if not item.is_directory:
                self.download_file(source_path=item.path, target_path=os.path.join(target_path, item.path.replace(self.volume_path, '').replace(item.name, '').strip('/')))
            else:
                self.download_directory(source_path=item.path, target_path=target_path)

    def upload_file_from_directory(self, source_path: str, source_file_name: str, target_path: str = "") -> None:
        """
        Uploads a file from a specified directory to a target path in the volume.

        Args:
            source_path (str): The path of the local directory containing the file to be uploaded.
            source_file_name (str): The name of the file to be uploaded.
            target_path (str, optional): The target path in the volume where the file will be uploaded. Defaults to an empty string.

        Returns:
            None
        """
        with open(os.path.join(source_path, source_file_name), 'rb') as f:
            self.client.files.upload(file_path=os.path.join(self.volume_path, target_path, source_file_name), contents=f)
        print("File uploaded successfully!")

    def upload_directory(self, source_path: str, target_path: str = "") -> None:
        """
        Uploads a directory recursively from the specified source path to the target path in the volume.

        Args:
            source_path (str): The path of the directory to upload in the volume.
            target_path (str, optional): The target path in the volume where the directory will be uploaded. Defaults to an empty string.

        Returns:
            None
        """
        for root, dirs, files in os.walk(source_path):
            for file in files:
                self.upload_file_from_directory(source_path=root, source_file_name=file, target_path=os.path.join(target_path.strip('/'), root.replace(source_path, '').strip('/')))

    def upload_pandas_dataframe(self, df: pd.DataFrame, file_name: str, target_path: str = "") -> None:
        """
        Uploads a pandas DataFrame to the specified target path in the volume as a CSV file.

        Args:
            df (pd.DataFrame): The DataFrame to be uploaded.
            file_name (str): The name of the file to be uploaded.
            target_path (str, optional): The target path in the volume where the file will be uploaded. Defaults to an empty string.

        Returns:
            None
        """
        csv_buffer = io.BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        self.client.files.upload(file_path=os.path.join(self.volume_path, target_path, file_name), contents=csv_buffer)
        print("File uploaded successfully!")

    def delete_volume_file(self, file_path: str) -> None:
        """
        Deletes a file from the volume.

        Args:
            file_path (str): The path of the file to be deleted.

        Returns:
            None
        """
        self.client.files.delete(file_path=os.path.join(self.volume_path, file_path))
        print("File deleted successfully!")

    def __delete_volume_files_recursively(self, items) -> None:
        """
        Recursively deletes all files and directories in the specified items list.

        Args:
            items: A list of items to be deleted.

        Returns:
            None
        """
        for item in items:
            if not item.is_directory:
                self.client.files.delete(item.path)
            else:
                contents = self.client.files.list_directory_contents(item.path)
                if len(pd.DataFrame(contents)) == 0:
                    self.client.files.delete_directory(item.path)
                else:
                    self.__delete_volume_files_recursively(self.client.files.list_directory_contents(item.path))

    def delete_volume_folder(self, folder_name: str) -> None:
        """
        Deletes a folder and its contents from the volume.

        Args:
            folder_name (str): The name of the folder to be deleted.

        Returns:
            None
        """
        self.__delete_volume_files_recursively(self.client.files.list_directory_contents(os.path.join(self.volume_path, folder_name)))
        self.client.files.delete_directory(os.path.join(self.volume_path, folder_name))

    def download_uc_table(self, catalog: str, schema: str, table: str, target_path: str) -> None:
        """
        Downloads a Unity Catalog table to a target path using the provided spark session.

        Args:
            catalog (str): The catalog where the table is located.
            schema (str): The schema of the table.
            table (str): The name of the table.
            target_path (str): The path where the table will be downloaded.

        Returns:
            None
        """
        staging_folder = os.path.join(self.volume_path, f"tmp_{str(uuid.uuid4())}")
        spark_session = SparkSession.builder.getOrCreate()
        if os.environ["SPARK_CONNECT_MODE_ENABLED"] != "1":
            raise ValueError(f"'SPARK_CONNECT_MODE_ENABLED' got {os.environ['SPARK_CONNECT_MODE_ENABLED']}, but expected '1'. Make sure you have Databricks extension for Visual Studio Code enabled.")
        (
            spark_session
            .table(f"`{catalog}`.`{schema}`.`{table}`")
            .coalesce(1)
            .write
            .format("parquet")
            .mode("overwrite")
            .save(staging_folder)
        )

        files = self.list_files(sub_folder=staging_folder)
        pos = [os.path.splitext(item)[-1] for item in files["name"]].index(".parquet")
        path = files.iloc[pos].path
        self.download_file(source_path=path, target_path=target_path)
        self.delete_volume_folder(staging_folder)
