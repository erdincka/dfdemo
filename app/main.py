import logging
import streamlit as st
import pandas as pd

# Set the logging level for the inotify_buffer to WARNING or higher
logging.getLogger("watchdog.observers.inotify_buffer").setLevel(logging.WARNING)

st.session_state.setdefault("logs", "")
st.session_state.setdefault("source_dataframe", pd.DataFrame())

import s3, utils, demos, constants, restcalls
from config import logger


def sidebar():
    # Session settings & vars
    hostname = utils.get_public_hostname()
    urls = utils.URLs(hostname)
    apps = utils.APPs(hostname)

    sb = st.sidebar
    sb.toggle("Enable AI Model", key="use_ai")

    # List URLs
    sb.write("Services:")
    cols = sb.columns(3)
    for idx, lnk in enumerate(urls + apps):
        col = cols[idx % 3]
        col.link_button(
            lnk["name"], lnk["url"], help=lnk["url"], use_container_width=True
        )

    sb.selectbox(
        "Buckets",
        options=s3.list_buckets(),
        on_change=utils.set_bucket_list,
        key="selected_bucket",
        index=None,
    )

    sb.selectbox(
        "Folders",
        options=constants.DEMO_FOLDERS,
        on_change=utils.set_folder_list,
        key="selected_folder",
        index=None,
    )

    links_in_demovol = [
        n["name"]
        for n in utils.get_folder_list("/demovol")
        if n["target"].startswith("mapr::table::")
    ]

    logger.debug(links_in_demovol)
    table_name = sb.selectbox(
        "Tables",
        options=links_in_demovol,
        on_change=utils.set_table_content,
        key="selected_table",
        index=None,
        accept_new_options=True,
    )

    if table_name and table_name not in links_in_demovol:
        if sb.button(f"Create table '{table_name}'"):
            sb.write(f"Create table: {restcalls.create_table(table_name)}")
            # sb.write(f"Create column family: {restcalls.create_cf(table_name)}")
            # sb.write(
            #     f'Create DDM on creditcard: {restcalls.set_datamask(table_name, "creditcard", "mrddm_last4")}'
            # )

    sb.markdown(
        """
        Learn more about [HPE Data Fabric](https://docs.ezmeral.hpe.com/datafabric-customer-managed/710/MapROverview/c_overview_intro.html)
                """
    )


def main():
    st.set_page_config(page_title="HPE Data Fabric Demo", layout="wide")
    sidebar()

    demo_list = list(demos.DEMO_LIST.keys())

    tabs = st.tabs(demo_list)
    for name, demo in demos.DEMO_LIST.items():
        with tabs[demo_list.index(name)].container():
            title, info, keywords = st.columns([6, 1, 5], vertical_alignment="bottom")
            title.write(f"#### {demo['title']}")
            keywords.pills(
                "Keywords",
                demo["keywords"],
                disabled=True,
                default=None,
                label_visibility="collapsed",
            )
            info.button(
                "",
                type="tertiary",
                icon=":material/info:",
                help=demo.get("flow", ""),
                key=name,
                disabled=True,
            )
            demo["function"]()

    # List S3 bucket content
    if st.session_state.get("bucket_content", None):
        st.write(f"Bucket {st.session_state['selected_bucket']} objects:")
        st.table(st.session_state["bucket_content"])

    # List folder content
    if st.session_state.get("folder_content", None):
        st.write(f"Folder {st.session_state['selected_folder']} content:")
        st.table(st.session_state["folder_content"])

    # Enable AI chat
    if st.session_state.get("use_ai", False):
        st.chat_input("Fire away")

    # Log Output
    with st.expander("Logs"):
        st.code(st.session_state.get("logs", ""), language="text", height=140)


if __name__ == "__main__":
    main()
