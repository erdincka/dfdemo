import os
import urllib.parse
import httpx

from config import logger

auth = ("mapr", "mapr")


def create_table(table_name: str):
    try:
        return (
            httpx.post(
                f"https://mapr:8443/rest/table/create?path=/demovol/{table_name}&tabletype=json&defaultreadperm=p",
                auth=auth,
                verify=False,
            )
            .raise_for_status()
            .json()
            .get("status", "NOK")
        )
    except Exception as error:
        logger.error(error)


def create_cf(table_name: str):
    try:
        return (
            httpx.post(
                f"https://mapr:8443/rest/table/cf/create?path=/demovol/{table_name}&cfname=democf&jsonpath=_id&force=1",
                auth=auth,
                verify=False,
            )
            .raise_for_status()
            .json()
            .get("status", "NOK")
        )
    except Exception as error:
        logger.error(error)


def set_datamask(table_name: str, column: str, datamask: str):
    try:
        return (
            httpx.post(
                f"https://mapr:8443/rest/table/cf/column/datamask/set?path=/demovol/{table_name}&cfname=default&name={column}&datamask={datamask}",
                auth=auth,
                verify=False,
            )
            .raise_for_status()
            .json()
            .get("status", "NOK")
        )
    except Exception as error:
        logger.error(error)


def list_datamasks():
    try:
        return (
            httpx.post(
                f"https://mapr:8443/rest/security/datamask/list",
                auth=auth,
                verify=False,
            )
            .raise_for_status()
            .json()
        )
    except Exception as error:
        logger.error(error)
        return {}


def get_documents(table_path: str, auth: tuple[str, str] = auth):
    # Return if table does not exist
    if not os.path.islink(f"/mapr/dfab.io/demovol/{table_path}"):
        return
    try:
        table_encoded = urllib.parse.quote_plus(f"/demovol/{table_path}")
        return (
            httpx.get(
                f"https://mapr:8243/api/v2/table/{table_encoded}",
                auth=auth,
                verify=False,
            )
            .raise_for_status()
            .json()
            .get("DocumentStream", [])
        )
    except Exception as error:
        logger.error(error)
        raise error


def add_documents(table_path: str, docs: list):
    try:
        table_encoded = urllib.parse.quote_plus(f"/demovol/{table_path}")
        logger.debug("Encoded tablename: %s", table_encoded)
        logger.debug("Writing records: %s", docs)
        return (
            httpx.post(
                f"https://mapr:8243/api/v2/table/{table_encoded}",
                json=docs,
                headers={"Content-Type": "application/json"},
                auth=auth,
                verify=False,
            )
            .raise_for_status()
            .text
        )
    except Exception as error:
        logger.error(error)
        raise error
