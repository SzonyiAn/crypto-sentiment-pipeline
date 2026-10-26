"""Saving files to the data layers, locally and (optionally) in Azure.

Design: the local data/ folder is always the working copy. With the "azure"
target, every file is ALSO uploaded to the Blob container of the same layer
(raw, staged or curated). This way the code is identical on your laptop and
on the Azure VM; only the target setting changes.
"""

import logging
import os
import shutil
from pathlib import Path

import config

log = logging.getLogger(__name__)


class Storage:
    def __init__(self, target: str = config.DEFAULT_TARGET):
        if target not in ("local", "azure"):
            raise ValueError(f"Unknown target '{target}', use 'local' or 'azure'")
        self.target = target
        self._blob_service = None

        if target == "azure":
            connection_string = os.getenv(config.AZURE_CONNECTION_STRING_ENV)
            if not connection_string:
                raise RuntimeError(
                    f"Target is 'azure' but {config.AZURE_CONNECTION_STRING_ENV} is not set"
                )
            # Imported here so the local mode works even without the Azure package.
            from azure.storage.blob import BlobServiceClient

            self._blob_service = BlobServiceClient.from_connection_string(connection_string)

    def local_path(self, layer: str, relative_path: str) -> Path:
        """data/<layer>/<relative_path>, e.g. data/raw/prices/2026/10/26/..."""
        if layer not in config.LAYERS:
            raise ValueError(f"Unknown layer '{layer}'")
        return config.DATA_DIR / layer / relative_path

    def save_bytes(self, layer: str, relative_path: str, content: bytes) -> Path:
        """Write content to the layer. Re-running with the same path REPLACES the
        file instead of creating a second copy, which keeps re-runs safe."""
        path = self.local_path(layer, relative_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        log.info("Saved %s/%s (%d bytes)", layer, relative_path, len(content))

        if self.target == "azure":
            blob = self._blob_service.get_blob_client(container=layer, blob=relative_path)
            blob.upload_blob(content, overwrite=True)
            log.info("Uploaded to Azure container '%s': %s", layer, relative_path)
        return path

    def clear_folder(self, layer: str, relative_dir: str) -> None:
        """Delete a folder (locally and in Azure) before writing a fresh version
        of it, so no file from an earlier run of the same day can stay behind."""
        folder = self.local_path(layer, relative_dir)
        if folder.exists():
            shutil.rmtree(folder)
        if self.target == "azure":
            container = self._blob_service.get_container_client(layer)
            prefix = relative_dir.rstrip("/") + "/"
            for blob in container.list_blobs(name_starts_with=prefix):
                container.delete_blob(blob.name)

    def publish_folder(self, layer: str, relative_dir: str) -> None:
        """Upload a folder that Spark wrote (e.g. a Parquet table) to Azure.

        Spark writes a table as a FOLDER of part-files, not a single file.
        Locally, Spark's overwrite mode already replaced the old folder. In
        Azure we do the same by hand: first delete every blob of the old
        version, then upload the new files. Without the delete step, a re-run
        could leave old part-files next to the new ones (= duplicated rows).
        """
        if self.target != "azure":
            return
        folder = self.local_path(layer, relative_dir)
        container = self._blob_service.get_container_client(layer)
        prefix = relative_dir.rstrip("/") + "/"

        old_blobs = [b.name for b in container.list_blobs(name_starts_with=prefix)]
        for name in old_blobs:
            container.delete_blob(name)

        uploaded = 0
        for file in sorted(folder.rglob("*")):
            # Skip Spark's checksum files (.crc); they are only useful locally.
            if file.is_file() and not file.name.endswith(".crc"):
                blob_name = prefix + file.relative_to(folder).as_posix()
                with file.open("rb") as stream:
                    container.upload_blob(blob_name, stream, overwrite=True)
                uploaded += 1
        log.info("Published %s/%s to Azure (%d old blobs removed, %d files uploaded)",
                 layer, relative_dir, len(old_blobs), uploaded)
