import json
import os
import subprocess
import socket
from urllib.parse import urlparse
import httpx
import pandas as pd
import streamlit as st
from streamlit.components.v1 import html
from faker import Faker

from config import logger
from constants import DEMO_STREAM
import s3
import streams

def run_command(command):
    logger.debug(f"Running: {command}")
    try:
        result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info(result.stdout.decode())
    except subprocess.CalledProcessError as e:
        logger.error(e.stderr.decode())


def set_service(service: dict, running: bool):
    logger.debug(f"Got {service} to {running}")
    if running:
        cmd_name = service["command"].split(" ").pop()
        killcmd = "kill -9 $(ps -ef | grep " + cmd_name + " | grep -v grep | awk '{ print $2 }')"
        run_command(killcmd)
    else:
        run_command(service['command'])

    

def query_nasa(search_term: str):
    params = { "media_type": "image", "q": search_term}
    try: 
        r = httpx.get("https://images-api.nasa.gov/search", params=params)
        if r.status_code == 200:
            data = r.json()
            # st.json(data, expanded=False)
            return parse_data(data)

    except Exception as e:
        logger.error(e)

    return pd.DataFrame()


def parse_data(data):
    """Parse the NASA API response data into a DataFrame."""
    logger.debug(f"Parsing NASA API response with {len(data['collection']['items'])} items.")
    try:
        df = pd.DataFrame(data["collection"]["items"])
        # df.set_index("href", inplace=True)
        df["title"] = df["data"].apply(lambda x: x[0]["title"])
        df["description"] = df["data"].apply(lambda x: x[0]["description"])
        df["keywords"] = df["data"].apply(lambda x: ', '.join((x[0]["keywords"] if "keywords" in x[0] else [])))
        df["preview"] = df["links"].apply(lambda x: [link["href"] for link in x if link["rel"] == "preview"])
        df.drop("data", axis=1, inplace=True)
        df.drop("links", axis=1, inplace=True)
        return df
    except Exception as error:
        logger.error(error)
        st.error(error)
        return pd.DataFrame()


def get_public_hostname():
    from streamlit_js_eval import streamlit_js_eval

    # This returns the result of `window.location.origin` from browser
    result = streamlit_js_eval(js_expressions="window.location.origin", key="get_url")

    if result:
        logger.debug(f"Detected public URL: {result}")
        return urlparse(result).hostname
    else:
        st.warning("Could not detect public URL yet.")
        return ""


def sample_to_incoming():
    for _ in range(10):
        msg = Faker().profile(fields=['name', 'address', 'job', 'sex'])
        if streams.produce(DEMO_STREAM, 'incoming', json.dumps(msg)):
            logger.info(f"Published {msg}")


# @st.dialog("Code", width='large')
# def code_viewer(code: str, extra_code: str = ""):
#     st.code(code)
#     if extra_code:
#         st.code(extra_code)


# def nav_to(url):
#     html(f'<script>window.open("{url}", "_blank").then(r => window.parent.location.href);</script>')


def is_port_open(port: int):
    """
    Check if a port is open on localhost.
    
    Args:
        port (int): The port number to check
        
    Returns:
        bool: True if port is open, False otherwise
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)  # Set timeout to avoid hanging
            result = s.connect_ex(('localhost', int(port)))
            logger.debug("Port %d connect result: %s", port, result)
            return result == 0
    except (socket.gaierror, ValueError):
        # Handle invalid port numbers or DNS resolution issues
        logger.debug("Invalid port or connection error: %d", port)
        return False
    except Exception as e:
        # Log unexpected errors
        logger.debug("Unexpected error checking port %d: %s", port, str(e))
        return False


def URLs(hostname: str):
    if hostname and is_port_open(8443):
        return [
            { 'name': "MCS", 'help': 'Management Console', 'url': f"https://{hostname}:8443/app/mcs" },
            { 'name': "DFUI", 'help': 'Consumption UI', 'url': f"https://{hostname}:8443/app/dfui" },
            { 'name': "S3", 'help': 'Object Storage Console', 'url': f"https://{hostname}:8443/app/mcs/opal/#/buckets/" },
        ]
    else:
        return []


def APPs(hostname: str):
    res = []
    if hostname:
        if is_port_open(8047): res.append( { 'name': 'Drill', 'help': 'NiFi', 'url': f'https://{hostname}:8047'} )
        if is_port_open(8780): res.append( { 'name': 'AF', 'help': 'Airflow', 'url': f'https://{hostname}:8780' } )
        if is_port_open(12443): res.append( { 'name': 'NF', 'help': 'NiFi', 'url': f"https://{hostname}:12443/nifi"} )

    return res


def set_bucket_list():
    st.session_state['bucket_content'] = s3.list_bucket(st.session_state['selected_bucket']) if st.session_state['selected_bucket'] else None


def get_app_folders():
    return os.listdir('/mapr/dfab.io/apps/')