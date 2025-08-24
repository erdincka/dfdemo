import asyncio
from datetime import datetime
import json
import os
import urllib.parse
import httpx
import pandas as pd
import streamlit as st

import streams
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


# def create_cf(table_name: str):
#     try:
#         return (
#             httpx.post(
#                 f"https://mapr:8443/rest/table/cf/create?path=/demovol/{table_name}&cfname=democf&jsonpath=_id&force=1",
#                 auth=auth,
#                 verify=False,
#             )
#             .raise_for_status()
#             .json()
#             .get("status", "NOK")
#         )
#     except Exception as error:
#         logger.error(error)


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


def get_datamasks(table_name: str):
    try:
        return (
            httpx.get(
                f"https://mapr:8443/rest/table/cf/column/datamask/get?path=/demovol/{table_name}&cfname=default",
                auth=auth,
                verify=False,
            )
            .raise_for_status()
            .json()
            .get("data", {})
        )
    except Exception as error:
        logger.error(error)


@st.cache_data
def list_datamasks():
    try:
        return (
            httpx.get(
                f"https://mapr:8443/rest/security/datamask/list",
                auth=auth,
                verify=False,
            )
            .raise_for_status()
            .json()
            .get("data", [])
        )
    except Exception as error:
        logger.error(error)
        return {}


def get_documents(table_path: str, auth: tuple[str, str] = auth):
    # Return if table does not exist
    if not os.path.islink(f"/mapr/dfab.io/demovol/{table_path}"):
        return
    logger.info(f"Reading table {table_path} as user {auth[0]}")
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


def monitoring():
    enable_refresh = st.toggle("📊 Enable Monitoring")
    if enable_refresh:
        st.write("TODO!")


# ### Monitoring / OpenTSDB
# # --- Config ---
# OPENTSDB_URL = "http://mapr:4242"
# DEFAULT_TAGS = {}  # e.g., {"host": "server1"}


# # --- Fetch available metrics ---
# @st.cache_data(ttl=300)
# def fetch_metrics():
#     r = httpx.get(
#         f"{OPENTSDB_URL}/api/suggest",
#         params={"type": "metrics", "max": 1000},
#         timeout=10,
#     )
#     r.raise_for_status()
#     return r.json()


# # --- Query selected metric ---
# def query_metric(metric, start="15m-ago", aggregator="avg"):
#     payload = {
#         "start": start,
#         "queries": [{"aggregator": aggregator, "metric": metric}],
#     }
#     r = httpx.post(f"{OPENTSDB_URL}/api/query", json=payload, timeout=10)
#     r.raise_for_status()
#     result = r.json()
#     logger.debug(result)
#     try:
#         series = result[0]
#         df = pd.DataFrame(list(series["dps"].items()), columns=["timestamp", "value"])
#         df["timestamp"] = pd.to_datetime(pd.to_numeric(df["timestamp"]), unit="s")
#         return df
#     except Exception as e:
#         logger.error(f"Query failed: {e}")

#     return pd.DataFrame()


# @st.fragment(run_every="15s")
# def opentsdb_monitoring():
#     enable_refresh = st.toggle("📊 Enable Monitoring")
#     full_metrics = st.toggle("System metrics?")
#     # st.slider("Refresh interval (seconds)", 5, 60, 60, key="refresh_interval")
#     metrics = (
#         [m for m in fetch_metrics()]
#         if full_metrics
#         else [m for m in fetch_metrics() if "mapr.bucket" in m or "mapr.fs" in m]
#     )
#     selected_metric = st.selectbox("Choose a metric to display", metrics)

#     start_range = st.selectbox(
#         "Time range", ["15m-ago", "1h-ago", "6h-ago", "12h-ago", "1d-ago"]
#     )
#     aggregator = st.selectbox("Aggregator", ["avg", "sum", "min", "max", "none"])

#     if enable_refresh:
#         df = query_metric(selected_metric, start=start_range, aggregator=aggregator)
#         if not df.empty:
#             st.line_chart(df, x="timestamp", y="value")
#             # time.sleep(refresh_interval)
#         else:
#             logger.info("No data points found for that query.")


# async def topic_stats(stream_path: str, topic: str):
#     try:
#         URL = (
#             f"https://mapr:8443/rest/stream/topic/info?path={stream_path}&topic={topic}"
#         )

#         async with httpx.AsyncClient(
#             verify=False
#         ) as client:  # using async httpx instead of sync requests to avoid blocking the event loop
#             response = await client.get(URL, auth=auth, timeout=2.0)

#             if response is None or response.status_code != 200:
#                 # possibly not connected or topic not populated yet, just ignore
#                 logger.warning(f"Failed to get topic stats for {topic}")

#             else:
#                 metrics = response.json()
#                 if not metrics["status"] == "ERROR":
#                     logger.debug(metrics)
#                     df = pd.DataFrame(metrics["data"], columns=["timestamp", "value"])
#                     df["timestamp"] = pd.to_datetime(
#                         pd.to_numeric(metrics["timestamp"]), unit="ms"
#                     )
#                     st.line_chart(df, x="timestamp", y="value")
#                 else:
#                     # possibly topic is not created yet
#                     logger.warning("Topic stat query error %s", metrics["errors"])

#     except Exception as error:
#         logger.warning("Topic stat request error %s", error)
#         # delayed query if failed - possibly cluster is not accessible
