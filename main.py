from pathlib import Path
import streamlit as st
import pandas as pd

from constants import sources, targets, write_as
from utils import query_nasa
from config import logger
import s3

st.session_state.setdefault('logs', "")
st.session_state.setdefault('source_dataframe', pd.DataFrame())


def main():
    st.set_page_config(page_title='HPE Data Fabric Demo', layout='wide')

    cols = st.columns(3, border=True)

    with cols[1]:
        st.title('Source')
        source = st.segmented_control('Source', key='source', options=sources, format_func=str.capitalize, label_visibility='hidden')
        match source:
            case 'file':
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

            case 'api':
                search_terms = ["missile", "earthquake", "tsunami", "oil", "flood", "iraq", "syria"]
                search_term = st.segmented_control('Search NASA Image API for:', search_terms, key='search_term')
                if search_term:
                    st.session_state['source_dataframe'] = query_nasa(search_term)
                else: st.session_state['source_dataframe'] = pd.DataFrame()

            case 'sales':
                st.write("Will read data from rdbms")
                st.session_state['source_dataframe'] = pd.DataFrame()

            case 'kafkatopic':
                st.write("Will read data from topic")
                st.session_state['source_dataframe'] = pd.DataFrame()

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
            st.segmented_control('Target', options=targets, format_func=str.capitalize, key='target')
            st.segmented_control('Format', options=write_as, format_func=str.capitalize, key='format')
    
            if st.session_state['target'] == 's3':
                buckets = s3.list_buckets()
                # st.write(f"Writing to {st.session_state['target']} at {st.session_state['destination']} using {st.session_state['format']}")
            # if st.session_state.get('target', None) and st.session_state.get('format', None) and st.session_state.get('destination', None):
                selected_bucket = st.selectbox('Bucket', options=buckets)
                new_bucket = st.text_input("Bucket", placeholder='demobk')
                destination = new_bucket if new_bucket else selected_bucket

                if destination and st.session_state['format']:
                    df = st.session_state['source_dataframe']
                    # Determine content type based on format
                    content_type = "text/csv"
                    if st.session_state.get('format', None) == "json":
                        content_type = "application/json"
                    elif st.session_state.get('format', None) == "parquet":
                        content_type = "application/octet-stream"

                    putobject = st.button("Put")
                    if putobject and destination: 
                        filename = f"demofile.{'csv' if content_type == 'text/csv' else 'json' if content_type == 'application/json' else 'parquet'}"
                        if s3.put(df=df, bucket_name=destination, file_key=filename, content_type=content_type):
                            st.success(f"{filename} uploaded to bucket {destination}")

    # Session settings & vars
    with cols[0]:
        st.title("Settings")
        use_ai = st.toggle("Enable AI Model")
        # setup as target file/table name
        st.session_state['scheme_name'] = st.session_state['search_term'] if st.session_state['source'] == 'api' else st.session_state.get('filename', 'nofile') if st.session_state['source'] == 'file' else ''
        # st.write(f"Source Schema: {st.session_state['scheme_name']}")

    if use_ai:
        st.chat_input("Fire away")

    # Log Output
    # st.code(st.session_state.get('logs', ''), language='text', height=100)

if __name__ == '__main__':
    main()
