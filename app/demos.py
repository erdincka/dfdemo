import os
from pathlib import Path
import streamlit as st
import pandas as pd
import json
import inspect

from config import logger
import utils
import constants
import streams
import s3
import restcalls


def inout():
    if st.button("Publish sample data to 'incoming' topic"):
        utils.sample_to_incoming()

    cols = st.columns(2, border=True)
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

    # Select the destination
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

            if st.session_state["target"] == "s3":
                buckets = s3.list_buckets()
                # st.write(f"Writing to {st.session_state['target']} at {st.session_state['destination']} using {st.session_state['format']}")
                # if st.session_state.get('target', None) and st.session_state.get('format', None) and st.session_state.get('destination', None):
                bucket = st.selectbox("Select Bucket", options=buckets)
                new_bucket = st.text_input("Or Create New Bucket", placeholder="demobk")
                destination = new_bucket if new_bucket else bucket

                if bucket and st.session_state["format"]:
                    df = st.session_state["source_dataframe"]
                    # Determine content type based on format
                    content_type = "text/csv"
                    if st.session_state.get("format", None) == "json":
                        content_type = "application/json"
                    elif st.session_state.get("format", None) == "parquet":
                        content_type = "application/octet-stream"

                    putobject = st.button("Put")
                    if putobject and destination:
                        filename = f"demofile.{'csv' if content_type == 'text/csv' else 'json' if content_type == 'application/json' else 'parquet'}"
                        if s3.put(
                            df=df,
                            bucket_name=destination,
                            file_key=filename,
                            content_type=content_type,
                        ):
                            st.success(f"{filename} uploaded to bucket {destination}")
            elif st.session_state["target"] == "posix":
                destination = st.selectbox(
                    "Folder",
                    options=[f["name"] for f in utils.get_folder_list("/demovol")],
                    index=None,
                    accept_new_options=True,
                )
                if (
                    destination
                    and st.session_state["format"]
                    and st.button(
                        "Save", help="Save to folder", key="btn_save_to_folder"
                    )
                ):
                    os.makedirs(f"/mapr/dfab.io/demovol/{destination}", exist_ok=True)

                    filename = f"demofile.{st.session_state['format']}"
                    try:
                        df = st.session_state["source_dataframe"]
                        # Create the full path for saving
                        save_path = Path(f"/app/{filename}")
                        # Ensure directory exists
                        save_path.parent.mkdir(parents=True, exist_ok=True)
                        # Save based on format
                        if format == "csv":
                            df.to_csv(save_path.with_suffix(".csv"), index=False)
                            st.success(f"Data saved as {save_path.with_suffix('.csv')}")
                        elif format == "json":
                            df.to_json(save_path.with_suffix(".json"), orient="records")
                            st.success(
                                f"Data saved as {save_path.with_suffix('.json')}"
                            )
                        elif format == "parquet":
                            df.to_parquet(
                                save_path.with_suffix(".parquet"), index=False
                            )
                            st.success(
                                f"Data saved as {save_path.with_suffix('.parquet')}"
                            )

                    except Exception as e:
                        st.error(f"Error saving file: {str(e)}")

    # Show source data
    if input_record_count:
        st.write(f"Source with {input_record_count} records.")
        st.dataframe(st.session_state["source_dataframe"], height=200)

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
        ### Multi Tenant Filesystem Access with Data Fabric
        
        Data Fabric is configured with 2 tenants, `tenant1` and `tenant2`
        
        Tenant are assigned to users:

            user11 & user12 belongs to tenant1
        
            user21 belongs to tenant2
        
        `user11` in `tenant1` is configured with read-write access, and `user12` in `tenant1` is configured with read-only access, using these commands at volume creation:

            /opt/mapr/bin/maprcli volume create -name tenant1Vol -path /tenant1 -tenantuser user11 -readAce 'g:tenant1' -writeAce 'u:user11' -replication 1 -minreplication 1 -nsreplication 1 -nsminreplication 1 -dare false -tieringenable false
            /opt/mapr/bin/maprcli volume create -name tenant2Vol -path /tenant2 -tenantuser user21 -readAce 'g:tenant2' -writeAce 'u:user21' -replication 1 -minreplication 1 -nsreplication 1 -nsminreplication 1 -dare false -tieringenable false


        By running these commands, we can see the results of various read and write operations for these users, against the mounted `tenant1` filesystem at /t1 mountpoint.
        
    """
    )

    if st.button(
        "Run commands for create/read/write operations with different users - commands are listed in the code section"
    ):
        for out in utils.run_command_with_output(
            """
            echo "Update service with user11's ticket"
            sed -i 's|^fuse.ticketfile.location=.*|fuse.ticketfile.location=/home/mapr/tenant_user11_ticket.txt|' /opt/mapr/conf/fuse.conf
            sed -i 's|^fuse.mount.point=.*|fuse.mount.point=/t1|' /opt/mapr/conf/fuse.conf
            sed -i 's|^.*fuse.export=.*|fuse.export=/dfab.io/tenant1/|' /opt/mapr/conf/fuse.conf
            echo "Restarting Posix client to remount tenant volume"
            service mapr-posix-client-basic restart 2>&1 > /dev/null
            service mapr-posix-client-basic status 2> /dev/null
            while [ ! -d /t1/user11 ]; do sleep 2; done # ensure mount is completed
            echo "/t1 mounted!"
            echo "List /t1/ as user11"; sudo -u user11 ls -la /t1/
            echo "List /t1/ as user12"; sudo -u user12 ls -la /t1/
            echo "Tenant2 user user21 cannot access /t1/, running ls /t1 should return nothing"; sudo -u user21 ls -la /t1/ || echo "ls /t1/ failed!"
            echo "user11 has read/write ACE, and user12 has only read ACE on /t1"
            echo "Write file as user11, should return file"; fname=$(mktemp | cut -d'/' -f3); sudo -u user11 touch /t1/user11/$fname; sudo -u user11 ls -l /t1/user11/$fname
            echo "Write file as user12, should fail with permission error even for their own dir"; fname=$(mktemp | cut -d'/' -f3); sudo -u user12 touch /t1/user12/$fname || echo "create /t1/user12/$fname failed"; sudo -u user12 ls -l $fname || echo "ls /t1/user12/$fname failed"
            echo "file/dir ACLs are also respected, user11 write to user12 owned folder, should fail with permission error"; fname=$(mktemp | cut -d'/' -f3); sudo -u user11 touch /t1/user12/$fname || echo "create /t1/user12/$fname failed for user11"
            """
        ):
            st.code(out, language="log")

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
                /opt/mapr/bin/maprcli acl set -type cluster -user root:fc mapr:fc user11:login user12:login user21:login

                # Create volumes for multi-tenant demo
                /opt/mapr/bin/maprcli volume create -name tenant1Vol -path /tenant1 -tenantuser user11
                /opt/mapr/bin/maprcli volume create -name tenant2Vol -path /tenant2 -tenantuser user21
                echo mapr | maprlogin generateticket -type tenant -user user11 -out /home/mapr/tenant_user11_ticket.txt
                echo mapr | maprlogin generateticket -type tenant -user user21 -out /home/mapr/tenant_user21_ticket.txt
                chown mapr:mapr /home/mapr/tenant_user11_ticket.txt /home/mapr/tenant_user21_ticket.txt
                """,
                language="shell",
            )
        with st.expander("Multi-tenancy demo steps"):
            st.code(inspect.getsource(multi_tenancy))


def datamasking():
    st.markdown(
        """### Dynamic Data Masking
            
    Works on JSON tables, with column families and across tables.

    Create/re-use table, add records, set DDM and retrieve records as different users.

    user11 has `unmaskedread` permission.

    user12 has `read` permission.
            
    """
    )

    cols = st.columns(4, vertical_alignment="bottom")
    # table_name = cols[0].text_input("Table name", placeholder="mytable")
    table_name = st.session_state["selected_table"]

    # Allow adding records
    if table_name:
        if cols[0].button(
            "✨",
            help=f"Add random records to the table {table_name}",
        ):
            try:
                docs = utils.sample_creditcards(1)
                st.write(f"Sending records to {table_name}")
                st.table(docs)
                st.write(restcalls.add_documents(table_name, docs))
            except Exception as error:
                st.error(error)
        user = cols[2].selectbox("User", options=["user11", "user12"])
        # List table content
        if st.session_state.get("table_content", None):
            cols = st.columns([9, 1])
            cols[0].write(f"Records in table: {st.session_state['selected_table']}")
            if cols[1].button("🔄"):
                utils.set_table_content(runas=user if user else "")
            st.table(st.session_state["table_content"])

    # cols[3].selectbox(
    #     "Data Masks",
    #     options=[d["name"] for d in restcalls.list_datamasks().get("data", [])],
    #     index=None,
    #     key="selected_ddm",
    # )


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
    st.link_button("Mesh", "http://docker.kayalab.uk:3005/")


DEMO_LIST = {
    "Stream & Batch": inout,
    "Multi-Tenancy": multi_tenancy,
    "CDC": cdc,
    "Data Mesh": mesh,
    "DDM": datamasking,
}

logger.debug("Demos Loaded!")
