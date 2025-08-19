import os
from pathlib import Path
from uuid import uuid4
import streamlit as st
import pandas as pd
import json
import inspect

import fragments
from config import logger
import utils
import constants
import streams
import s3
import restcalls


def inout():

    if st.button(
        "Publish sample data to *incoming* topic",
        help='To use "Stream" source, publish some messages into it.',
    ):
        utils.sample_to_incoming()

    cols = st.columns(2, border=True)
    # Source selection
    with cols[0]:
        st.title("Source")
        source = st.segmented_control(
            "Source", key="source", options=constants.sources, label_visibility="hidden"
        )
        match source:
            case "File":
                input_file = st.file_uploader("File", type=["csv", "json"])
                if input_file is not None:
                    st.session_state["filename"] = input_file.name
                    if Path(input_file.name).suffix == ".json":
                        st.session_state["source_dataframe"] = pd.read_json(input_file)
                    elif Path(input_file.name).suffix == ".csv":
                        st.session_state["source_dataframe"] = pd.read_csv(input_file)
                    else:
                        st.error(f"Unknown file extension: {input_file.name}")
                else:
                    st.session_state["source_dataframe"] = pd.DataFrame()

            case "NASA API":
                search_terms = [
                    "missile",
                    "earthquake",
                    "tsunami",
                    "oil",
                    "flood",
                    "iraq",
                    "syria",
                ]
                search_term = st.segmented_control(
                    "Search NASA Image API for:", search_terms, key="search_term"
                )
                if search_term:
                    st.session_state["source_dataframe"] = utils.query_nasa(search_term)
                else:
                    st.session_state["source_dataframe"] = pd.DataFrame()

            case "Sales DB":
                st.write("Will read data from rdbms")
                st.session_state["source_dataframe"] = pd.DataFrame()

            case "Stream":
                if st.button("Consume from 'incoming'"):
                    messages = []
                    for msg in streams.consume(constants.DEMO_STREAM, "incoming"):
                        logger.info("Incoming %s", msg)
                        messages.append(json.loads(msg))
                    st.session_state["source_dataframe"] = pd.DataFrame(messages)
                    if len(messages) == 0:
                        st.warning("No messages returned from consumer!")

            case _:
                logger.debug("Source not selected!")
                st.info("Select a data source!")
                st.session_state["source_dataframe"] = pd.DataFrame()

    input_record_count = len(st.session_state["source_dataframe"])

    # Destination selection
    with cols[1]:
        if input_record_count:
            st.title("Destination")
            st.segmented_control(
                "Target",
                options=constants.targets,
                format_func=str.capitalize,
                key="target",
            )
            st.segmented_control(
                "Format",
                options=constants.write_as,
                format_func=str.capitalize,
                key="format",
            )
            st.text_input(
                "Name",
                placeholder="myfile",
                icon=(
                    "📁"
                    if st.session_state.target == "posix"
                    else ":material/data_object:"
                ),
                help="File or object name - no extension",
                key="destination_name",
            )
        # S3 bucket selection
        if st.session_state.get("target", "") == "s3":
            buckets = s3.list_buckets()
            st.selectbox(
                "Bucket",
                options=buckets,
                accept_new_options=True,
                index=None,
                key="save_to_bucket",
                help="Select or create a new bucket",
            )

    # Show source data
    if input_record_count:
        with st.expander(f"Source with {input_record_count} records."):
            st.dataframe(st.session_state.source_dataframe, height=200)

    # Data Transformation
    st.write("Apply transformation (ie, ETL processing) on the incoming data")
    if len(st.session_state.source_dataframe):
        with st.expander("Data Transformation"):
            fragments.data_transformation()
            fragments.show_refined_data()

    # Save to destination
    if st.session_state.get("target", "") == "s3":
        if (
            st.session_state.save_to_bucket
            and st.session_state.format
            and st.session_state.destination_name
        ):
            df = (
                st.session_state.refined_data
                if len(st.session_state.refined_data)
                else st.session_state["source_dataframe"]
            )
            # Determine content type based on format
            content_type = "text/csv"
            if st.session_state.get("format", None) == "json":
                content_type = "application/json"
            elif st.session_state.get("format", None) == "parquet":
                content_type = "application/octet-stream"

            if st.button("Put", type="primary"):
                filename = f"{st.session_state.destination_name}.{'csv' if content_type == 'text/csv' else 'json' if content_type == 'application/json' else 'parquet'}"
                if s3.put(
                    df=df,
                    bucket_name=st.session_state.save_to_bucket,
                    file_key=filename,
                    content_type=content_type,
                ):
                    st.success(
                        f"{filename} uploaded to bucket {st.session_state.save_to_bucket}"
                    )

    elif st.session_state.get("target", "") == "posix":
        # line = st.columns(2, vertical_alignment="bottom")
        # destination = line[0].selectbox(
        #     "Folder",
        #     options=[f["name"] for f in utils.get_folder_list("/demovol")],
        #     index=None,
        #     accept_new_options=True,
        # )
        if (
            # destination
            st.session_state["format"]
            and st.session_state["destination_name"]
            and st.button(
                "Save", help="Save to folder", key="btn_save_to_folder", type="primary"
            )
        ):
            # os.makedirs(f"/mapr/dfab.io/demovol/{destination}", exist_ok=True)

            filename = (
                f"{st.session_state.destination_name}.{st.session_state['format']}"
            )
            try:
                df = (
                    st.session_state.refined_data
                    if len(st.session_state.refined_data)
                    else st.session_state["source_dataframe"]
                )
                # Create the full path for saving
                save_path = Path(f"/mapr/dfab.io/demovol/{filename}")
                # Ensure directory exists
                # save_path.parent.mkdir(parents=True, exist_ok=True)
                # Save based on format
                logger.info(st.session_state["format"])
                if st.session_state["format"] == "csv":
                    df.to_csv(save_path.with_suffix(".csv"), index=False)
                    st.success(f"Data saved as {save_path.with_suffix('.csv')}")
                elif st.session_state["format"] == "json":
                    df.to_json(save_path.with_suffix(".json"), orient="records")
                    st.success(f"Data saved as {save_path.with_suffix('.json')}")
                elif st.session_state["format"] == "parquet":
                    df.to_parquet(save_path.with_suffix(".parquet"), index=False)
                    st.success(f"Data saved as {save_path.with_suffix('.parquet')}")

            except Exception as e:
                st.error(f"Error saving file: {str(e)}")
                logger.error(e)

    # Code viewers
    with st.expander("Code"):
        with st.expander("S3 Client"):
            st.code(inspect.getsource(s3.get_client))
        with st.expander("S3 Bucket List"):
            st.code(inspect.getsource(s3.list_bucket))
        with st.expander("S3 Put"):
            st.code(inspect.getsource(s3.put))
        with st.expander("Stream Producer"):
            st.code(inspect.getsource(streams.produce))
        with st.expander("Stream Consumer"):
            st.code(inspect.getsource(streams.consume))


def multi_tenancy():
    st.markdown(
        """
        This section allows you to select a tenant, which will mount that tenant's volume with its admin user (`user11` or `user21`), then you can run list and read and write commands against that tenant mount points as different users.

        `user11@tenant1` and `user21@tenant2` have *read/write* ACE for their respective tenant volumes, and `user12` has *read* ACE on its own tenant volume (mounted at `/t1`)
        
    """
    )

    tenant = st.segmented_control(
        "Select Tenant:",
        ["Tenant1", "Tenant2"],
        on_change=utils.remount_tenant,
        key="selected_tenant",
    )

    if tenant:

        line = st.columns([3, 9])

        runas = line[0].segmented_control(
            "Run as", options=["user11", "user12", "user21"]
        )

        # tempfilename = uuid4().hex[:8]
        cmd = line[1].segmented_control(
            "Command",
            options=[
                "ls -la /t1",
                "ls -la /t2",
                "ls -la /t1/user11",
                "ls -la /t1/user12",
                "ls -la /t2/user21",
                "cat /t1/user11/*",
                "cat /t1/user12/*",
                "cat /t2/user21/*",
                "date >> /t1/user11/testfile.1",
                "date >> /t1/user12/testfile.1",
                "date >> /t2/user21/testfile.1",
            ],
        )

        st.write(f"Running `sudo -u {runas} bash -c '{cmd}'`")

        if runas and cmd:
            for out in utils.run_command_with_output(
                f"sudo -u {runas} bash -c '{cmd}'"
            ):
                st.code(out, language="shell")

    with st.expander("Code"):
        with st.expander("Create tenant users & volumes"):
            st.code(
                """
                # Create users for multi-tenant demo
                getent group tenant1 || groupadd -g 10000 tenant1
                getent group tenant2 || groupadd -g 20000 tenant2
                id user11 || useradd -m -d /home/user11 -g 10000 -s /bin/bash -u 10001 user11
                id user12 || useradd -m -d /home/user12 -g 10000 -s /bin/bash -u 10002 user12
                id user21 || useradd -m -d /home/user21 -g 20000 -s /bin/bash -u 20002 user21
                echo user11:mapr | chpasswd
                echo user12:mapr | chpasswd
                echo user21:mapr | chpasswd
                mkdir /t1
                mkdir /t2
                
                # Create volumes and tenant tickets
                /opt/mapr/bin/maprcli volume create -name tenant1Vol -path /tenant1 -tenantuser user11 -readAce 'g:tenant1' -writeAce 'u:user11' -replication 1 -minreplication 1 -nsreplication 1 -nsminreplication 1 -dare false -tieringenable false 
                /opt/mapr/bin/maprcli volume create -name tenant2Vol -path /tenant2 -tenantuser user21 -readAce 'g:tenant2' -writeAce 'u:user21' -replication 1 -minreplication 1 -nsreplication 1 -nsminreplication 1 -dare false -tieringenable false 
                echo mapr | maprlogin generateticket -type tenant -user user11 -out /home/mapr/tenant_user11_ticket.txt
                echo mapr | maprlogin generateticket -type tenant -user user21 -out /home/mapr/tenant_user21_ticket.txt
                """,
                language="shell",
            )


def datamasking():
    st.write(
        "Dynamically mask columns for users/groups, detailed information is on the [DDM docs](https://docs.ezmeral.hpe.com/datafabric-customer-managed/710/SecurityGuide/DDM.html)."
    )
    st.write(
        "Create/re-use table, add records, set DDM and retrieve records as different users."
    )
    st.write("*mapr* has `unmaskedread` permission.")
    st.write("*public* has `read` permission.")

    table_name = st.session_state["selected_table"]

    if table_name:

        # Show/set masks on table
        st.write(f"### Masks on {table_name}:")
        st.table(restcalls.get_datamasks(table_name=table_name))
        # Set DDM
        masks, fields, _, actions = st.columns(
            [3, 3, 4, 2], vertical_alignment="bottom"
        )
        action1, action2 = actions.columns(2, vertical_alignment="bottom")
        if action2.button("🔎", key="seeDDMtypes", help="Show DDM predefined types"):
            utils.show_ddm_types()

        selected_mask = masks.selectbox(
            "Mask Types",
            options=[d["name"] for d in restcalls.list_datamasks()],
            index=None,
            key="selected_mask",
        )
        selected_field = fields.selectbox(
            "Field",
            options=["name", "creditcard", "address", "mobile", "ssn", "iban"],
            index=None,
            key="selected_field",
        )
        if (
            selected_field
            and selected_mask
            and action1.button("Set DDM", help="Set DDM for field")
        ):
            restcalls.set_datamask(table_name, selected_field, selected_mask)

        # List table content
        read_as_command, _, actions = st.columns([3, 7, 2], vertical_alignment="bottom")
        action1, action2 = actions.columns(2, vertical_alignment="bottom")

        runas_user = read_as_command.segmented_control(
            "Read table as",
            options=["mapr", "user11"],
        )
        if runas_user:
            utils.set_table_content(runas=runas_user if runas_user else "")

        if action1.button("🔄", help="Refresh"):
            utils.set_table_content(runas=runas_user if runas_user else "")
        if action2.button(
            # ":material/queue:",
            "🆕",
            help="Add new json documents",
        ):
            try:
                docs = utils.sample_creditcards(2)
                st.write(f"#### Sending records to {table_name}")
                st.table(docs)
                st.write(restcalls.add_documents(table_name, docs))
            except Exception as error:
                st.error(error)

        if st.session_state.get("table_content", None):
            st.write(f"### Records in {st.session_state['selected_table']}:")
            st.table(st.session_state["table_content"])

    else:
        st.markdown("### 👈 Select a table to start!")


def cdc():
    st.markdown(
        """
        - Check MySQL connectivity
        - Check MySQL settings to verify CDC is enabled
        - Check MySQL table users is populated
        - Check NiFi service running
        - Check NiFi flow file is available - provide a link to download if so
        - Check dashboard availability
        - Provide button to update users table - create new users!
        - Embed dashboard for realtime updates within st UI
    """
    )


def mesh():
    # st.link_button("Mesh", "http://docker.kayalab.uk:3005/mesh/")
    st.write("Use https://github.com/erdincka/catchx")


DEMO_LIST = {
    "⛲ Read & Write": {
        "function": inout,
        "title": "Data ingestion and multi-format data storage",
        "keywords": [
            "ingestion",
            "kafka",
            "stream",
            "DB",
            "json",
            "parquet",
            "s3",
        ],
        "flow": """
        Pick a data source: Upload a json/csv file, query NASA image search API, or read from a message stream.

        Select the destination location, S3 bucket or filesystem, and select the format (csv/json/parquet).

        Pick the destionation bucket or folder from the sidebar to view their content.

        > Open logs to see messages.
        """,
    },
    "👥 Multi-Tenancy": {
        "function": multi_tenancy,
        "title": "Tenant Isolation",
        "keywords": [
            "tenant",
            "security",
            "permission",
            "ACE",
            "access control expression",
        ],
        "flow": """
        Data Fabric is configured with 2 tenants, `tenant1` and `tenant2`
        
        Tenant are assigned to users:

            user11 & user12 belongs to tenant1
        
            user21 belongs to tenant2

        `user11` in `tenant1` is configured with read-write access, and `user12` in `tenant1` is configured with read-only access, using these commands at volume creation:

            /opt/mapr/bin/maprcli volume create -name tenant1Vol -path /tenant1 -tenantuser user11 -readAce 'g:tenant1' -writeAce 'u:user11' -replication 1 -minreplication 1 -nsreplication 1 -nsminreplication 1 -dare false -tieringenable false
            /opt/mapr/bin/maprcli volume create -name tenant2Vol -path /tenant2 -tenantuser user21 -readAce 'g:tenant2' -writeAce 'u:user21' -replication 1 -minreplication 1 -nsreplication 1 -nsminreplication 1 -dare false -tieringenable false

        """,
    },
    "🛰️ CDC": {
        "function": cdc,
        "title": "Capture Data Changes in Real-time",
        "flow": f"""
        We've set up a MySQL DB with `binlog` enabled for `demodb`.

        Provided NiFi flow contains processors to consume these transaction logs and then in turn writes them to S3 endpoint in parquet format. 

        Follow the instructions for end to end demo flow: https://github.com/erdincka/df-cdc 
        """,
        "keywords": [
            "Change data capture",
            "stream",
            "RDBMS",
        ],
    },
    "🕸️ Data Mesh": {
        "function": mesh,
        "title": "Build a Data Mesh",
        "keywords": [
            "RDBMS",
            "real-time",
            "stream",
        ],
    },
    "🕶️ DDM": {
        "function": datamasking,
        "title": "Dynamic Data Masking on Tables",
        "keywords": ["security", "confidential", "data masking"],
    },
}
