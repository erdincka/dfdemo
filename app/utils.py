import json
import subprocess
import socket
from urllib.parse import urlparse
from uuid import uuid4
import httpx
import mysql.connector
import pandas as pd
import streamlit as st
from faker import Faker


from config import logger
import constants, s3, streams, restcalls


def not_implemented():
    logger.debug("Calling a function which is not implemented yet")
    return "Not Implemented"


def run_command(command):
    logger.debug(f"Running: {command}")
    try:
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        logger.info(result.stdout.decode())
    except subprocess.CalledProcessError as e:
        logger.error(e.stderr.decode())


def run_command_with_output(command: str):
    logger.debug("[ CMD ] %s", command)
    try:
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        out = result.stdout.decode()
        if out:
            yield out
    except subprocess.CalledProcessError as e:
        yield f"[ ERROR ] {e.stderr.decode()}"


def set_service(service: dict, running: bool):
    logger.debug(f"Got {service} to {running}")
    if running:
        cmd_name = service["command"].split(" ").pop()
        killcmd = (
            "kill -9 $(ps -ef | grep "
            + cmd_name
            + " | grep -v grep | awk '{ print $2 }')"
        )
        run_command(killcmd)
    else:
        run_command(service["command"])


@st.cache_data
def query_nasa(search_term: str):
    params = {"media_type": "image", "q": search_term}
    try:
        r = httpx.get("https://images-api.nasa.gov/search", params=params)
        if r.status_code == 200:
            data = r.json()
            # st.json(data, expanded=False)
            return parse_data(data)

    except Exception as e:
        logger.error(e)

    return pd.DataFrame()


@st.cache_data
def parse_data(data):
    """Parse the NASA API response data into a DataFrame."""
    logger.debug(
        f"Parsing NASA API response with {len(data['collection']['items'])} items."
    )
    try:
        df = pd.DataFrame(data["collection"]["items"])
        # df.set_index("href", inplace=True)
        df["title"] = df["data"].apply(lambda x: x[0]["title"])
        df["description"] = df["data"].apply(lambda x: x[0]["description"])
        df["keywords"] = df["data"].apply(
            lambda x: ", ".join((x[0]["keywords"] if "keywords" in x[0] else []))
        )
        df["preview"] = df["links"].apply(
            lambda x: [link["href"] for link in x if link["rel"] == "preview"]
        )
        df.drop("data", axis=1, inplace=True)
        df.drop("links", axis=1, inplace=True)
        return df
    except Exception as error:
        logger.error(error)
        st.error(error)
        return pd.DataFrame()


def get_public_hostname():
    """
    Return the public hostname of the current machine.

    This function uses `streamlit_js_eval`, which is a widget command.
    Therefore it must not be cached.
    """
    from streamlit_js_eval import streamlit_js_eval

    # This returns the result of `window.location.origin` from browser
    result = streamlit_js_eval(js_expressions="window.location.origin", key="get_url")

    if result:
        logger.debug(f"Detected public URL: {result}")
        return urlparse(result).hostname
    else:
        logger.warning("Could not detect public URL yet.")
        return ""


def sample_to_incoming():
    for _ in range(10):
        msg = Faker().profile(fields=["name", "address", "job", "sex"])
        if streams.produce(constants.DEMO_STREAM, "incoming", json.dumps(msg)):
            logger.info(f"Published {msg}")


def sample_users(count: int = 10):
    res = []
    fake = Faker("en_GB")
    for _ in range(count):
        new_user = {
            "_id": uuid4().hex,
            "name": fake.name(),
            "address": fake.address(),
            "mobile": fake.phone_number(),
            "ssn": fake.ssn(),
            "creditcard": fake.credit_card_number(),
            "iban": fake.iban(),
        }
        res.append(new_user)

    return res


@st.cache_data
def get_users_from_url(count: int):
    URL = f"https://randomuser.me/api/?results={count}&format=json&dl&noinfo"
    res = httpx.get(URL)
    res.raise_for_status()
    users = res.json()
    return users["results"] if "results" in users else []


@st.cache_data
def user_records_for_mysql(users: list):

    sql = (
        "INSERT INTO users (title, first, last, street, city, state, postcode, country, gender, "
        "email, uuid, username, password, phone, cell, dob, registered, large, medium, thumbnail, nat) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )

    vals = []
    for user in users:
        # pprint(user)
        vals.append(
            (
                user["name"]["title"],
                user["name"]["first"],
                user["name"]["last"],
                str(user["location"]["street"]["number"])
                + " "
                + user["location"]["street"]["name"],
                user["location"]["city"],
                user["location"]["state"],
                user["location"]["postcode"],
                user["location"]["country"],
                user["gender"],
                user["email"],
                user["login"]["uuid"],
                user["login"]["username"],
                user["login"]["password"],
                user["phone"],
                user["cell"],
                user["dob"]["date"],
                user["registered"]["date"],
                # datetime.fromisoformat(user["dob"]["date"].replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S'),
                # datetime.fromisoformat(user["registered"]["date"].replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S'),
                user["picture"]["large"],
                user["picture"]["medium"],
                user["picture"]["thumbnail"],
                user["nat"],
            )
        )

    return (sql, vals)


def get_mysql_connection():
    try:
        conn = mysql.connector.connect(
            host="db",
            user="mysql",
            password="Admin123.",
            database="demodb",
            connection_timeout=5,
        )
        return conn
    except mysql.connector.Error as err:
        st.error(f"❌ MySQL connection failed: {err}")
    except Exception as error:
        st.error(f"Query error: {error}")


# @st.dialog("Code", width='large')
# def code_viewer(code: str, extra_code: str = ""):
#     st.code(code)
#     if extra_code:
#         st.code(extra_code)


# def nav_to(url):
#     html(f'<script>window.open("{url}", "_blank").then(r => window.parent.location.href);</script>')


def is_port_open(port: int):
    """
    Check if a port is open on host 'mapr'.

    Args:
        port (int): The port number to check

    Returns:
        bool: True if port is open, False otherwise
    """
    try:
        logger.debug("Checking port: %d", port)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)  # Set timeout to avoid hanging
            result = s.connect_ex(("mapr", int(port)))
            logger.debug("Port %d connect result: %s", port, result)
            return result == 0
    except (socket.gaierror, ValueError):
        # Handle invalid port numbers or DNS resolution issues
        logger.warning("Invalid port or connection error: %d", port)
        return False
    except Exception as e:
        # Log unexpected errors
        logger.warning("Unexpected error checking port %d: %s", port, str(e))
        return False


def URLs(hostname: str):
    logger.debug("Getting URLs for host: %s", hostname)
    if hostname and is_port_open(8443):
        return [
            {
                "name": "MCS",
                "help": "Management Console",
                "url": f"https://{hostname}:8443/app/mcs",
            },
            {
                "name": "DFUI",
                "help": "Consumption UI",
                "url": f"https://{hostname}:8443/app/dfui",
            },
            {
                "name": "S3",
                "help": "Object Storage Console",
                "url": f"https://{hostname}:8443/app/mcs/opal/#/buckets/",
            },
        ]
    else:
        return []


def APPs(hostname: str):
    logger.debug("Getting APP links for host: %s", hostname)
    res = []
    if hostname:
        if is_port_open(8047):
            res.append(
                {"name": "Drill", "help": "NiFi", "url": f"https://{hostname}:8047"}
            )
        if is_port_open(8780):
            res.append(
                {
                    "name": "Airflow",
                    "help": "Airflow",
                    "url": f"https://{hostname}:8780",
                }
            )
        if is_port_open(12443):
            res.append(
                {
                    "name": "NiFi",
                    "help": "NiFi",
                    "url": f"https://{hostname}:12443/nifi",
                }
            )

        if is_port_open(3000):
            res.append(
                {
                    "name": "Grafana",
                    "help": "Grafana Dashboards",
                    "url": f"https://{hostname}:3000",
                }
            )

    return res


def set_bucket_list():
    st.session_state["bucket_content"] = (
        s3.list_bucket(st.session_state["selected_bucket"])
        if st.session_state["selected_bucket"]
        else None
    )


def set_folder_list():
    st.session_state["folder_content"] = (
        get_folder_list(st.session_state["selected_folder"])
        if st.session_state["selected_folder"]
        else None
    )


def set_table_content(runas: str = ""):
    logger.debug("Get table content as user: %s", runas)
    try:
        st.session_state["table_content"] = (
            restcalls.get_documents(st.session_state["selected_table"])
            if not runas
            else (
                restcalls.get_documents(
                    st.session_state["selected_table"], auth=(runas, "mapr")
                )
                if st.session_state["selected_table"]
                else None
            )
        )
    except Exception as error:
        logger.error(error)
        raise error


def get_folder_list(folder: str):
    """
    Return a list of dictionaries that mirror the information you see with
    ``ls -l`` - permissions, link-count, owner, group, size, modification
    time, name and, for symlinks, the target string.

    This uses ``Path.lstat`` (so we never follow a symlink) and the
    :mod:`stat` helpers to format the mode string in the same way as
    ``ls`` prints it.  Any exception for a particular entry is logged
    and that entry is simply skipped - this keeps the function robust
    when it encounters non-regular files such as device nodes or
    special MapR links.
    """
    import os
    import stat
    from datetime import datetime
    from pathlib import Path
    import pwd, grp

    content = []

    if folder not in constants.DEMO_FOLDERS:
        return content

    base_path = Path(f"/mapr/dfab.io/{folder}")

    try:
        for entry in base_path.iterdir():
            lstat = entry.lstat()  # do NOT follow symlinks
            mode_str = stat.filemode(lstat.st_mode)

            # Owner / group names – fall back to UID/GID if lookup fails
            try:
                owner = pwd.getpwuid(lstat.st_uid).pw_name
            except KeyError:
                owner = str(lstat.st_uid)

            try:
                group = grp.getgrgid(lstat.st_gid).gr_name
            except KeyError:
                group = str(lstat.st_gid)

            # Human‑readable modification time (like ls – one‑hour precision)
            mtime = datetime.fromtimestamp(lstat.st_mtime).strftime("%b %d %H:%M")

            # Target for symlinks (empty string if not a symlink)
            target = os.readlink(entry) if entry.is_symlink() else ""

            content.append(
                {
                    "mode": mode_str,  # e.g. “lrwxrwxrwx”
                    "nlink": lstat.st_nlink,  # number of hard links
                    "owner": owner,
                    "group": group,
                    "size": lstat.st_size,
                    "mtime": mtime,
                    "name": entry.name,
                    "target": target,
                }
            )
    except Exception as e:
        # If we can't stat a file we log and skip it
        logger.error(e)
        st.error(e)

    return content


@st.dialog("DDM predefined types", width="large")
def show_ddm_types():
    st.table([(d["name"], d["description"]) for d in restcalls.list_datamasks()])


def file_content(path: str):
    res = ""
    with open(path, "r") as f:
        res = f.read()

    return res  # .encode("utf-8")


def remount_tenant():
    tenant = st.session_state.selected_tenant
    if tenant:
        logger.info("Remount running for %s", tenant)
        user = "user11" if tenant == "Tenant1" else "user21"
        mount_point = "/t1" if tenant == "Tenant1" else "/t2"
        export_path = "/tenant1" if tenant == "Tenant1" else "/tenant2"
        user_path = "/t1/user11" if tenant == "Tenant1" else "/t2/user21"

        for out in run_command_with_output(
            f"""
            sed -i 's|^fuse.ticketfile.location=.*|fuse.ticketfile.location=/home/mapr/tenant_{user}_ticket.txt|' /opt/mapr/conf/fuse.conf
            sed -i 's|^fuse.mount.point=.*|fuse.mount.point={mount_point}|' /opt/mapr/conf/fuse.conf
            sed -i 's|^.*fuse.export=.*|fuse.export=/dfab.io/{export_path}/|' /opt/mapr/conf/fuse.conf
            # echo "Restarting Posix client to remount tenant volume"
            service mapr-posix-client-basic restart 2>&1 > /dev/null
            # service mapr-posix-client-basic status 2> /dev/null
            while [ ! -d {user_path} ]; do sleep 2; done # ensure mount is completed
            echo "{mount_point} mounted with {user} ticket!"
        """
        ):
            logger.info(out)

    else:
        logger.info("No tenant selected!")
