"""
Upload the UCI Online Retail CSV to the ADLS Gen2 raw zone.
Run once after converting the xlsx to csv.

Prerequisites:
    az login
    pip install azure-storage-file-datalake azure-identity
"""

import os
import time
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient


STORAGE_ACCOUNT = "retailpipelinedev"
CONTAINER = "raw"
REMOTE_PATH = "source/uci_retail/online_retail.csv"
LOCAL_PATH = "data/online_retail.csv"


def progress_hook(bytes_transferred: int, total_size: int) -> None:
    """Prints upload progress — called by the SDK after each chunk."""
    pct = (bytes_transferred / total_size) * 100
    mb_done = bytes_transferred / 1_000_000
    mb_total = total_size / 1_000_000
    print(f"  {pct:.1f}% — {mb_done:.1f} / {mb_total:.1f} MB", end="\r")


def upload_to_adls(
    storage_account: str,
    container: str,
    remote_path: str,
    local_path: str,
) -> None:
    """
    Upload a local file to ADLS Gen2 using DefaultAzureCredential.

    Args:
        storage_account: ADLS storage account name
        container: filesystem (container) name, e.g. 'raw'
        remote_path: destination path inside the container
        local_path: path to the local file to upload
    """
    account_url = f"https://{storage_account}.dfs.core.windows.net"
    credential = DefaultAzureCredential()

    service_client = DataLakeServiceClient(
        account_url,
        credential=credential,
        connection_timeout=30,   # TCP connection timeout in seconds
        read_timeout=300,        # socket read timeout per chunk in seconds (default is 60)
    )
    fs_client = service_client.get_file_system_client(container)
    file_client = fs_client.get_file_client(remote_path)

    file_size = os.path.getsize(local_path)
    print(f"Uploading {local_path} ({file_size / 1_000_000:.1f} MB) → {container}/{remote_path}")

    start = time.time()

    with open(local_path, "rb") as f:
        file_client.upload_data(
            f,
            overwrite=True,
            length=file_size,
            max_concurrency=1,
            chunk_size=4 * 1024 * 1024,  # 4MB per chunk — default 100MB causes write timeout on slow connections
            progress_hook=progress_hook,
        )

    elapsed = time.time() - start
    print(f"\nUpload complete in {elapsed:.1f}s → {container}/{remote_path}")


if __name__ == "__main__":
    upload_to_adls(STORAGE_ACCOUNT, CONTAINER, REMOTE_PATH, LOCAL_PATH)
