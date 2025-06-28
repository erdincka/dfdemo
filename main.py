# Standard libraries
import logging

# Third-party libraries
import streamlit as st
from streamlit.logger import get_logger
import subprocess

class StreamlitLogHandler(logging.Handler):
    def __init__(self, widget_update_func):
        super().__init__()
        self.widget_update_func = widget_update_func

    def emit(self, record):
        msg = self.format(record)
        self.widget_update_func(msg)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = get_logger(__name__)

def add_to_logs(msg):
    st.session_state.setdefault('logs', '')
    st.session_state['logs'] += str(msg) + '\n'

def run_command(command):
    try:
        result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return result.stdout.decode()
    except subprocess.CalledProcessError as e:
        return f'Error: {e.stderr.decode()}'

packages = [
    { 'name': 'nifi', 'package': 'mapr-nifi'},
    { 'name': 'airflow', 'package': 'mapr-airflow'},
    { 'name': 'spark', 'package': 'mapr-spark'},
    { 'name': 'hue', 'package': 'mapr-hue'},
]

def main():
    st.set_page_config(page_title='HPE Data Fabric Demo', layout='wide')
    st.title('Try this!')

    # Initialize checkbox states
    for package in packages:
        if package not in st.session_state:
            st.session_state[package['name']] = False

    # Checkbox Section
    st.markdown('### packages Control')
    for package in packages:
        name = package['name'].capitalize()
        prev_state = st.session_state.get(f'{package["name"]}_prev', None)
        current_state = st.checkbox(name, value=st.session_state[package['name']], key=f'{package["name"]}_checkbox')

        # Check if state changed
        if prev_state is None or prev_state != current_state:
            st.session_state[f'{package["name"]}_prev'] = current_state

            if current_state:
                output = run_command(f'apt install -yq {package["package"]}')
                st.success(f'Installing {name}: {output}')
            else:
                output = run_command(f'apt remove -yq {package["package"]}')
                st.success(f'Removing {name}: {output}')

    # Log Output
    st.markdown('### System Logs')
    st.code(st.session_state.get('logs', ''), language='text', height=300)

if '__streamlitmagic__' not in locals():
    import streamlit.web.bootstrap
    streamlit.web.bootstrap.run(__file__, False, [], {})
else:
    main()
