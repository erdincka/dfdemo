import os
from pathlib import Path
from uuid import uuid4
import streamlit as st
import pandas as pd
import json
import inspect

from mysql.connector import MySQLConnection

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
        help='To use "Stream" source, publish sample messages into it.',
    ):
        utils.sample_to_incoming()

    source = st.columns(1, border=True)
    # Source selection
    with source[0]:
        st.subheader("Source", help="Multi-format data ingestion", divider=True)
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

            case "REST API":
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

    # Show source data
    if input_record_count:
        with st.expander(f"Source with {input_record_count} records."):
            st.dataframe(st.session_state.source_dataframe, height=200)

    # Data Transformation
    etl = st.columns(1, border=True)
    with etl[0]:
        st.subheader(
            "ETL Processing",
            help="Apply transformation on the incoming data",
            divider=True,
        )
        if len(st.session_state.source_dataframe):
            fragments.data_transformation()
            fragments.show_refined_data()

    # Destination selection
    destination = st.columns(1, border=True)
    with destination[0]:
        st.subheader("Destination", help="Multi-format data storage", divider=True)
        if input_record_count:
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

                filename = f"{st.session_state.destination_name}.{'csv' if content_type == 'text/csv' else 'json' if content_type == 'application/json' else 'parquet'}"

                if st.button(
                    f"Put {filename} in {st.session_state.save_to_bucket}",
                    type="primary",
                ):
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
            filename = (
                f"{st.session_state.destination_name}.{st.session_state['format']}"
            )
            if (
                st.session_state["format"]
                and st.session_state["destination_name"]
                and st.button(
                    f"Save to {filename}",
                    help="Save to folder",
                    key="btn_save_to_folder",
                    type="primary",
                )
            ):
                try:
                    df = (
                        st.session_state.refined_data
                        if len(st.session_state.refined_data)
                        else st.session_state["source_dataframe"]
                    )
                    # Create the full path for saving
                    save_path = Path(
                        f"/mapr/{constants.CLUSTER_NAME}/{constants.DEMO_VOLUME}/{filename}"
                    )
                    logger.debug(st.session_state["format"])
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

        if st.button(
            "Insert sample record(s) into the table",
            help="Add new json document(s)",
            # type="primary",
        ):
            try:
                count = 1
                docs = utils.sample_users(count)
                st.write(f"#### Sending {count} record(s) to {table_name}")
                st.table(docs)
                st.write(restcalls.add_documents(table_name, docs))
            except Exception as error:
                st.error(error)

        # Show/set masks on table
        st.write(f"### Masks on {table_name}:")
        st.table(restcalls.get_datamasks(table_name=table_name))
        # Set DDM
        masks, fields, _, action = st.columns([3, 3, 4, 2], vertical_alignment="bottom")
        action1, action2 = action.columns(2, vertical_alignment="bottom")
        if action2.button("🔎", key="seeDDMtypes", help="Show DDM predefined types"):
            utils.show_ddm_types()

        selected_field = fields.selectbox(
            "Field",
            options=["name", "creditcard", "address", "mobile", "ssn", "iban"],
            index=None,
            key="selected_field",
        )
        selected_mask = masks.selectbox(
            "Mask Types",
            options=[d["name"] for d in restcalls.list_datamasks()],
            index=None,
            key="selected_mask",
        )
        if (
            selected_field
            and selected_mask
            and action1.button("Set DDM", help="Set DDM for field")
        ):
            restcalls.set_datamask(table_name, selected_field, selected_mask)

        # List table content
        read_as_command, _, action = st.columns([3, 8, 1], vertical_alignment="bottom")

        runas_user = read_as_command.segmented_control(
            "Read table as",
            options=["mapr", "user11"],
        )
        if runas_user:
            utils.set_table_content(runas=runas_user if runas_user else "")

        if action.button("🔄", help="Refresh"):
            utils.set_table_content(runas=runas_user if runas_user else "")

        if st.session_state.get("table_content", None):
            st.write(f"### Records in {st.session_state['selected_table']}:")
            st.table(st.session_state["table_content"])

    else:
        st.markdown("### 👈 Select a table to start!")


def cdc():
    # --- New: Verify MySQL connection to 'demodb' on host 'db' ---
    conn: MySQLConnection = (
        utils.get_mysql_connection()
    )  # pyright: ignore[reportAssignmentType]

    line = st.columns(3)
    if conn.is_connected():
        line[0].write("DB connected: ✅")
    else:
        return

    cursor = conn.cursor()
    try:
        # Check CDC
        sql = """
        SELECT @@global.log_bin;
        SELECT @@global.binlog_format;
        """
        results = cursor.execute(sql, multi=True)
        if results:
            for i, cur in enumerate(results):
                if cur.with_rows:
                    rows = cur.fetchall()
                    result = rows[0][0]
                    if result:
                        if i == 0:  # result of first select
                            line[1].write("Binlog ✅")
                        elif i == 1:  # result of second select
                            line[2].write("CDC: ✅")
    except Exception as error:
        logger.error(error)
        st.error(error)

    st.image("./images/CDC Demo.png", caption="Demo Flow", use_container_width=True)

    hostname = os.environ["PUBLIC_HOSTNAME"]
    nifi_url = f"https://{hostname}:12443/nifi"
    if st.button(
        "Setup NiFi",
        help="Run this only once!!! Or go to NiFi UI, and delete the Process Group and Template.",
    ):
        if restcalls.setup_nifi_flow(hostname):
            st.write('Open NiFi UI, double click to the "Process Group".')
            st.write(
                'Double-click on "CaptureChangeMySQL processor", click "Stop & Configure", go to "Properties" tab and enter `Admin123.` in the "Password" field. Click "Apply" to close. Finally right click and "Start".'
            )
            st.image(
                "./images/NiFi_CaptureChangeMySQL.png", caption="Change MySQL Password"
            )
            st.image("./images/NiFi_MySQLPassword.png", caption="Set MySQL password")
            st.write(
                "Click on 'NiFi Flow' at the bottom left corner, right click on empty space in the canvas, and select 'Run'."
            )

    st.subheader("Open NiFi UI to configure and monitor", divider=True)
    st.link_button(
        "Open NiFi",
        url=nifi_url,
        help=nifi_url,
    )

    st.write("Login with credentials: `admin`/`Admin123.Admin123.`")

    st.subheader("Add users to DB and monitor NiFi flow", divider=True)
    row = st.columns([1, 5], vertical_alignment="bottom")
    myinsert, myselect = row[0].columns(2)
    if myinsert.button(
        "",
        icon=":material/add_circle_outline:",
        help="Insert new users to the database",
        key="mysql_insert",
    ):
        users = utils.get_users_from_url(5)
        sql, vals = utils.user_records_for_mysql(users)
        cursor.executemany(sql, vals)
        conn.commit()
        st.success(f"{cursor.rowcount} row(s) added.")
        st.info("Refresh button to see new records updated in both bucket and volume")

    if myselect.button(
        "",
        icon=":material/refresh:",
        help="Refresh users from database",
        key="mysql_select",
    ):
        cursor.execute("SELECT * FROM users;")
        rows = cursor.fetchall()
        with st.expander(f"Fetched {cursor.rowcount} records from `users`"):
            if cursor.description:
                col_names = [desc[0] for desc in cursor.description]  # column headers
                df = pd.DataFrame(rows, columns=col_names)
                if "id" in col_names:
                    df = df.set_index("id")
                st.dataframe(df)

    st.dataframe(
        s3.summarize_s3_folder("demobucket", "users/")
    )  # use prefix with trailing slash otherwise files starting with 'users', such as 'users.csv' will be included
    st.dataframe(
        utils.dir_stats(f"{constants.MOUNT_PATH}/{constants.DEMO_VOLUME}/users")
    )

    # st.write(
    #     "Drag 'Process Group' icon from top of the page onto canvas, click on the browse icon to upload the flow file"
    # )
    # st.image("./images/NiFi_UploadFlow.png", caption="upload flow file")

    # st.write(
    #     'Select the "Process Group", and select the `Configuration` icon for "HPE Data Fabric Demo"'
    # )
    # st.image(
    #     "./images/NiFi_ControllerSettings.png",
    #     caption="Update controller configuration",
    # )
    # st.write(
    #     'In the "Controller Services" tab, enable all services by clicking the lightning icon and then selecting "Enable".'
    # )
    # st.image("./images/NiFi_ControllerServices.png", caption="Enable services")

    # st.write(
    #     'Click on empty space on the canvas, and select "Play" button to start all processors.'
    # )

    # st.link_button("Grafana", url=f"https://{hostname}:3000")
    st.write("Open Grafana dashboard for detailed monitoring.")

    cursor.close()
    conn.close()


def mesh():
    st.write("##### Data Mesh Architecture")
    st.markdown(
        "Defining hub-and-spoke layout with every 'box' representing a separate Data Fabric cluster, while being part of the Global Namespace.",
        help="A 'data mesh hub and spoke' is a federated data governance model within a data mesh architecture, where a central 'hub' team defines global standards, tools, and infrastructure, while decentralized 'spoke' teams (data domains) independently develop and publish domain-specific data products, ensuring both consistency and flexibility. This approach balances centralized control for essential functions like governance and security with the agility and ownership of decentralized data product development by business domains.",
    )
    st.write(
        "Data Fabric Global Namespace allows using a central (hub) cluster that hosts shared services and organisational policies, definitions as well as a 'data marketplace' that serves as an 'Enterprise Data Catalogue'."
    )
    st.image("./images/hubspoke.png", caption="Data Mesh in Hub-Spoke")

    st.write("##### Data Product - Fraud")
    st.write(
        "Focusing on a single department or project as the whole owner of a 'Data Product', which allows them autonomy and independence on the orchestration of all data ingestion, transformation and business outcomes, while keeping organisational policies and controls in place."
    )
    st.write(
        "Taking the 'Fraud' department as an example, the department will have full control on how data is ingested, processed and stored. Eventually, they will have the final 'data product' that can be shared with the rest of the organisation by publishing their 'data product' to the 'Data Marketplace' with their defined 'data contracts'."
    )
    st.image(
        "./images/frauddomain.png", caption="Fraud department as Data Product owner"
    )
    st.markdown(
        "In this domain, we are using the *medallion architecture*. Using separate volumes for each layer allows different data placement, protection, cost & performance levels.",
        help="The Medallion architecture is a data design pattern used in a lakehouse to organize data into three layers—Bronze, Silver, and Gold—each representing incrementally higher quality and refinement. The Bronze layer stores raw, untransformed data, while the Silver layer cleans, validates, and standardizes it. The Gold layer then transforms the data into enriched, business-ready datasets for analytics, machine learning, and reporting. This multi-layered approach ensures data quality, provides a single source of truth, and supports various use cases and workloads.",
    )

    st.link_button(
        "Open Mesh demo", f"http://{os.environ['PUBLIC_HOSTNAME']}:3005/mesh/"
    )
    st.write("If this is not working:")
    st.write("Use https://github.com/erdincka/catchx")


def gns():
    st.write(
        "*Ability to attach to external data sources, and making them part of the global namespace, so users and apps can access/manipulate the data on these sources without knowing their original location.*"
    )

    st.markdown("### 👈 Open DFUI to add external S3 endpoint")

    st.write("TODO: Configure sandbox to use NFSv4!")

    # st.write("Looking at the global mount point - */mapr* ")

    # with st.echo():
    #     for out in utils.run_command_with_output("ls -la /mapr"):
    #         st.code(out)

    # if st.button(
    #     "Mount NFSv4 volume",
    #     key="btn_nfs_mount",
    #     help="GNS facilitates access to external NFSv4 endpoints as if they are local to the cluster",
    # ):
    #     for out in utils.run_command_with_output("echo 'TO BE IMPLEMENTED'"):
    #         st.code(out, language="shell")

    # if st.button(
    #     "Import S3 Endpoint",
    #     key="btn_s3_import",
    #     help="Data Fabric Object Store can facilitate access to external S3-compatible endpoints as if they're local buckets",
    # ):
    #     for out in utils.run_command_with_output("echo 'TO BE IMPLEMENTED'"):
    #         st.code(out, language="shell")


def cross_protocol():
    st.write(
        "*Access the same data via different APIs, enabling apps and users reading and writing data in their preferred protocol*"
    )

    st.markdown("### Write files using HDFS")
    st.write(
        f"The following will run a command that will create an empty file with a random name and *.tmp* extension in **{constants.DEMO_VOLUME}**."
    )
    if st.button("Write HDFS"):
        with st.echo():
            filename = uuid4().hex
            for out in utils.run_command_with_output(
                f"hadoop fs -touchz /demovol/{filename}.tmp"
            ):
                st.code(out)

    st.markdown("### Read files as S3 objects")
    st.write(
        f"The following will run a command that will list all objects in the filesystem **{constants.DEMO_VOLUME}**, you should see another random filename with *.tmp* extension added."
    )
    if st.button("List with S3"):
        with st.echo():
            for out in utils.run_command_with_output(
                "/opt/mapr/bin/mc ls df/filestore/demovol/"
            ):
                st.code(out)


DEMO_LIST = {
    "⛲ Multi-Modal": {
        "function": inout,
        "title": "ETL for multi-modal data",
        "keywords": [
            "ingestion",
            "kafka",
            "stream",
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
    "👥 Multi-Tenant": {
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
    "🛰️ Realtime": {
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
        "title": "Build a Data Mesh with Data Products",
        "flow": """
        We aim to show the feature-rich data pipeline capabilities of HPE Data Fabric. 
        
        ## Fraud Detection pipeline demo with Ezmeral Data Fabric

        This is not an accurate representation of a real fraud detection process, but rather an end-to-end demonstration of how a pipeline can be built using some of Ezmeral Data Fabric capabilities for a real-life scenario. We aim to highlight the flexibility and openness of Ezmeral Data Fabric as a converged data platform for various data types and choices of open-source ecosystem tools/frameworks.

        The tools and frameworks used in the demo are selected with simplicity of their implementation in mind, but they are not meant to limit user's choice when it comes to real life implementation. Users are free to choose included or third-party tools, as Data Fabric supports various industry-standard protocols to read, process and store data.

        The app shows the ingestion of transaction data (json) via Event Streams and batch customer data (csv files) into the fabric, and then storing and processing them through their lifecycle inside the Fabric, using technologies such as NoSQL Document DBs or Iceberg tables. Then at the final stage we both simulate a fraud detection ML model inferencing on incoming messages as well as providing consolidated information as a Data Product that can be shared within the organisation either for Business Intelligence & Analytics or for other consumption methods through query APIs.

        We have already created volumes for the data product **fraud** as a representation of Madallion Architecture. We have *Bronz*, *Silver* and *Gold* volumes to represent data flow through its lifecycle.
        """,
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
    "🔭 GNS": {
        "function": gns,
        "title": "Global Namespace",
        "flow": "Access data on external sources as if they're local to the cluster",
        "keywords": ["s3", "nfsv4", "unified"],
    },
    "✖️ XProto": {
        "function": cross_protocol,
        "title": "Cross Protocol Data Access",
        "flow": "Read and write data using S3 and Posix",
        "keywords": ["s3", "posix", "hdfs", "data access"],
    },
}
