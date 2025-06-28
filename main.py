from pathlib import Path
import streamlit as st
import pandas as pd

from constants import services, sources, targets, write_as
from utils import not_implemented, query_nasa, select_source, select_target, set_service
from config import logger


st.session_state.setdefault('logs', "")
st.session_state.setdefault('source_dataframe', pd.DataFrame())


def main():
    st.set_page_config(page_title='HPE Data Fabric Demo', layout='wide')

    cols = st.columns(len(services))
    for svc in services:
        with cols[services.index(svc)]:
            st.toggle(svc['name'], key=svc['name'], on_change=lambda s=svc, state=st.session_state.get(svc['name'], False): set_service(s, state))


    source = st.segmented_control('Source', key='source', options=sources, format_func=str.capitalize, on_change=select_source)
    match source:
        case 'file':
            input_file = st.file_uploader('File', type=['csv', 'json'])
            if input_file is not None:
                if Path(input_file.name).suffix == ".json":
                    st.session_state['source_dataframe'] = pd.read_json(input_file)
                elif Path(input_file.name).suffix == ".csv":
                    st.session_state['source_dataframe'] = pd.read_csv(input_file)
                else:
                    st.error(f"Unknown file extension: {input_file.name}")
            else: st.session_state['source_dataframe'] = pd.DataFrame()

        case 'api':
            search_terms = ["missile", "earthquake", "tsunami", "oil", "flood", "iraq", "syria"]
            search_term = st.segmented_control('Search NASA Image API for:', search_terms)
            if search_term:
                st.session_state['source_dataframe'] = query_nasa(search_term)
            else: st.session_state['source_dataframe'] = pd.DataFrame()

        case 'sales':
            st.write("Will read data from rdbms")
            st.session_state['source_dataframe'] = pd.DataFrame()

        case _:
            logger.info("Select source data to use!")
            st.session_state['source_dataframe'] = pd.DataFrame()

    # Show source data
    input_record_count = len(st.session_state['source_dataframe'])
    if input_record_count:
        st.write(f"Source with {input_record_count} records.")
        st.write(st.session_state['source_dataframe'])


    # Select the destination
    if input_record_count:
        target = st.segmented_control("Target", options=targets, format_func=str.capitalize, on_change=select_target)
        logger.info(f'Selected target {target}')
        target_format = st.segmented_control('Format', options=write_as, format_func=str.capitalize, on_change=not_implemented)
        logger.info(f"Will write as {target_format}")

    # Log Output
    st.markdown('### System Logs')
    st.code(st.session_state.get('logs', ''), language='text', height=300)

if __name__ == '__main__':
    main()
