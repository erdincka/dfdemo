import inspect
import logging
from pathlib import Path
import streamlit as st
import pandas as pd

import constants
from streams import consume, produce
from utils import code_viewer, get_public_hostname, query_nasa, sample_to_incoming
from config import logger
import s3

# Set the logging level for the inotify_buffer to WARNING or higher
logging.getLogger("watchdog.observers.inotify_buffer").setLevel(logging.WARNING) 

st.session_state.setdefault('logs', "")
st.session_state.setdefault('source_dataframe', pd.DataFrame())


def main():
    st.set_page_config(page_title='HPE Data Fabric Demo', layout='wide')

    cols = st.columns(3, border=True)

    with cols[1]:
        st.title('Source')
        source = st.segmented_control('Source', key='source', options=constants.sources, label_visibility='hidden')
        match source:
            case 'File':
                input_file = st.file_uploader('File', type=['csv', 'json'])
                if input_file is not None:
                    st.session_state['filename'] = input_file.name
                    if Path(input_file.name).suffix == ".json":
                        st.session_state['source_dataframe'] = pd.read_json(input_file)
                    elif Path(input_file.name).suffix == ".csv":
                        st.session_state['source_dataframe'] = pd.read_csv(input_file)
                    else:
                        st.error(f"Unknown file extension: {input_file.name}")
                else: st.session_state['source_dataframe'] = pd.DataFrame()

            case 'NASA API':
                search_terms = ["missile", "earthquake", "tsunami", "oil", "flood", "iraq", "syria"]
                search_term = st.segmented_control('Search NASA Image API for:', search_terms, key='search_term')
                if search_term:
                    st.session_state['source_dataframe'] = query_nasa(search_term)
                else: st.session_state['source_dataframe'] = pd.DataFrame()

            case 'Sales DB':
                st.write("Will read data from rdbms")
                st.session_state['source_dataframe'] = pd.DataFrame()

            case 'Stream':
                grid = st.columns((3,1))
                with grid[0]:
                    if st.button("Consume from 'incoming'"):
                        messages = []
                        for msg in consume(constants.DEMO_STREAM, "incoming"):
                            logger.info("Incoming %s", msg)
                            messages.append(msg)
                        st.session_state['source_dataframe'] = pd.DataFrame(messages)
                        if len(messages) == 0: st.warning('No messages returned from consumer!')
                with grid[1]:
                    if st.button("💻", key='view-code-consume'):
                        code_viewer(inspect.getsource(consume))

            case _:
                logger.debug("Source not selected!")
                st.info("Select a data source!")
                st.session_state['source_dataframe'] = pd.DataFrame()

    # Show source data
    input_record_count = len(st.session_state['source_dataframe'])
    if input_record_count:
        st.write(f"Source with {input_record_count} records.")
        st.dataframe(st.session_state['source_dataframe'], height=200)


    # Select the destination
    with cols[2]:
        if input_record_count:
            st.title("Destination")
            st.segmented_control('Target', options=constants.targets, format_func=str.capitalize, key='target')
            st.segmented_control('Format', options=constants.write_as, format_func=str.capitalize, key='format')
    
            if st.session_state['target'] == 's3':
                buckets = s3.list_buckets()
                # st.write(f"Writing to {st.session_state['target']} at {st.session_state['destination']} using {st.session_state['format']}")
            # if st.session_state.get('target', None) and st.session_state.get('format', None) and st.session_state.get('destination', None):
                bucket = st.selectbox('Bucket', options=buckets)
                # new_bucket = st.text_input("Bucket", placeholder='demobk')
                # destination = new_bucket if new_bucket else selected_bucket

                if bucket and st.session_state['format']:
                    df = st.session_state['source_dataframe']
                    # Determine content type based on format
                    content_type = "text/csv"
                    if st.session_state.get('format', None) == "json":
                        content_type = "application/json"
                    elif st.session_state.get('format', None) == "parquet":
                        content_type = "application/octet-stream"

                    grid = st.columns((3,1))
                    with grid[0]:
                        putobject = st.button("Put")
                        if putobject and bucket: 
                            filename = f"demofile.{'csv' if content_type == 'text/csv' else 'json' if content_type == 'application/json' else 'parquet'}"
                            if s3.put(df=df, bucket_name=bucket, file_key=filename, content_type=content_type):
                                st.success(f"{filename} uploaded to bucket {bucket}")
                    with grid[1]:
                        if st.button("💻", key='view-code-s3-put'):
                            code_viewer(inspect.getsource(s3.put))

    # Session settings & vars
    with cols[0]:
        st.title("Settings")
        use_ai = st.toggle("Enable AI Model")
        # setup as target file/table name
        # st.session_state['scheme_name'] = st.session_state['search_term'] if st.session_state['source'] == 'api' else st.session_state.get('filename', 'nofile') if st.session_state['source'] == 'file' else ''
        # st.write(f"Source Schema: {st.session_state['scheme_name']}")
        # st.link_button("MCS", url=get_public_url().split(":")[1])
        hostname = get_public_hostname()
        grid = st.columns((3, 1))
        with grid[0]:
            st.link_button("MCS", help='Management Console', url=f"https://{hostname}:8443/app/mcs" if hostname else "")
            st.button("Publish to 'incoming'", help="Publish sample messages to incoming topic for stream ingestion", on_click=sample_to_incoming)
            if st.button("List Bucket", help='List content of the Bucket'):
                st.table(s3.list_objects('demobk'))
        with grid[1]:
            st.link_button("S3", help='Object Storage Console', url=f"https://{hostname}:8443/app/mcs/opal/#/buckets/demobk" if hostname else "")
            if st.button("💻", key='view-code-produce'):
                code_viewer(inspect.getsource(produce))
            if st.button("💻", key='view-code-listobjects'):
                code_viewer(inspect.getsource(s3.get_client), inspect.getsource(s3.list_objects))

    if use_ai:
        st.chat_input("Fire away")

    # Log Output
    st.code(st.session_state.get('logs', ''), language='text', height=140)

if __name__ == '__main__':
    main()
