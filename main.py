# Standard libraries
import logging

# Third-party libraries
import streamlit as st
from streamlit.logger import get_logger

from settings import services
from utils import run_command

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = get_logger(__name__)

def add_to_logs(msg):
    st.session_state.setdefault('logs', '')
    st.session_state['logs'] += str(msg) + '\n'

def main():
    st.set_page_config(page_title='HPE Data Fabric Demo', layout='wide')
    st.title('Try this!')

    st.markdown('### Services')
    for svc in services:
        name = svc['name'].capitalize()
        output = run_command(svc['command'])
        st.success(f'Installing {name}: {output}')
    # Log Output
    st.markdown('### System Logs')
    st.code(st.session_state.get('logs', ''), language='text', height=300)

if '__streamlitmagic__' not in locals():
    import streamlit.web.bootstrap
    streamlit.web.bootstrap.run(__file__, False, [], {})
else:
    main()
