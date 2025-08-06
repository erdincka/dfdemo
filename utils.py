import json
import subprocess
from urllib.parse import urlparse
import httpx
import pandas as pd
import streamlit as st
from faker import Faker

from config import logger
from constants import DEMO_STREAM
from streams import produce

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
        if produce(DEMO_STREAM, 'incoming', json.dumps(msg)):
            logger.info(f"Published {msg}")

@st.dialog("Code", width='large')
def code_viewer(code: str, extra_code: str = ""):
    st.code(code)
    if extra_code:
        st.code(extra_code)

