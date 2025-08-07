import inspect
import json
import logging
from pathlib import Path
import streamlit as st
import pandas as pd
from streamlit_extras.row import row

import constants
import streams
from config import logger
import s3
import utils

# Set the logging level for the inotify_buffer to WARNING or higher
logging.getLogger("watchdog.observers.inotify_buffer").setLevel(logging.WARNING) 

st.session_state.setdefault('logs', "")
st.session_state.setdefault('source_dataframe', pd.DataFrame())


def main():
    st.set_page_config(page_title='HPE Data Fabric Demo', layout='wide')

    # Session settings & vars
    hostname = utils.get_public_hostname()
    urls = utils.URLs(hostname)
    apps = utils.APPs(hostname)

    sb = st.sidebar
    sb.toggle("Enable AI Model", key='use_ai')
    for lnk in urls + apps:
        c1,c2 = sb.columns([1, 3])
        c1.write(lnk['name'])
        c2.write(lnk['url'])

    sb.selectbox("Buckets", options=s3.list_buckets(), on_change=utils.list_bucket_table, key='selected_bucket')
    sb.button('List Bucket', key='btn_list_bucket', help='List objects in bucket')
    # if bckt and st.session_state['btn_list_bucket']:
    #     st.write(f"Objects in {bckt}")
    #     st.table(s3.list_bucket(bckt))

    sb.button('Code for list_objects', key='code_list_bucket', help='Code for list_bucket')
    if st.session_state['code_list_bucket']:
        utils.code_viewer(inspect.getsource(s3.get_client), inspect.getsource(s3.list_bucket))

    cols = st.columns(2, border=True)
    with cols[0]:
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
                    st.session_state['source_dataframe'] = utils.query_nasa(search_term)
                else: st.session_state['source_dataframe'] = pd.DataFrame()

            case 'Sales DB':
                st.write("Will read data from rdbms")
                st.session_state['source_dataframe'] = pd.DataFrame()

            case 'Stream':
                grid = st.columns((3,1))
                with grid[0]:
                    if st.button("Consume from 'incoming'"):
                        messages = []
                        for msg in streams.consume(constants.DEMO_STREAM, "incoming"):
                            logger.info("Incoming %s", msg)
                            messages.append(json.loads(msg))
                        st.session_state['source_dataframe'] = pd.DataFrame(messages)
                        if len(messages) == 0: st.warning('No messages returned from consumer!')
                with grid[1]:
                    if st.button("< />", key='view-code-consume'):
                        utils.code_viewer(inspect.getsource(streams.consume))

            case _:
                logger.debug("Source not selected!")
                st.info("Select a data source!")
                st.session_state['source_dataframe'] = pd.DataFrame()

    input_record_count = len(st.session_state['source_dataframe'])

    # Select the destination
    with cols[1]:
        if input_record_count:
            st.title("Destination")
            st.segmented_control('Target', options=constants.targets, format_func=str.capitalize, key='target')
            st.segmented_control('Format', options=constants.write_as, format_func=str.capitalize, key='format')
    
            if st.session_state['target'] == 's3':
                buckets = s3.list_buckets()
                # st.write(f"Writing to {st.session_state['target']} at {st.session_state['destination']} using {st.session_state['format']}")
            # if st.session_state.get('target', None) and st.session_state.get('format', None) and st.session_state.get('destination', None):
                bucket = st.selectbox('Bucket', options=buckets)
                new_bucket = st.text_input("Bucket", placeholder='demobk')
                destination = new_bucket if new_bucket else bucket

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
                        if putobject and destination: 
                            filename = f"demofile.{'csv' if content_type == 'text/csv' else 'json' if content_type == 'application/json' else 'parquet'}"
                            if s3.put(df=df, bucket_name=destination, file_key=filename, content_type=content_type):
                                st.success(f"{filename} uploaded to bucket {destination}")
                    with grid[1]:
                        if st.button("💻", key='view-code-s3-put'):
                            utils.code_viewer(inspect.getsource(s3.put))
            elif st.session_state['target'] == 'posix':
                st.write('Write to filesystem')

    # Show source data
    if input_record_count:
        st.write(f"Source with {input_record_count} records.")
        st.dataframe(st.session_state['source_dataframe'], height=200)

    if st.session_state['use_ai']:
        st.chat_input("Fire away")

    # Log Output
    with st.expander("Logs"):
        st.code(st.session_state.get('logs', ''), language='text', height=140)

if __name__ == '__main__':
    main()
