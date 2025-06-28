import logging
import streamlit as st
from streamlit.logger import get_logger

class StreamlitLogHandler(logging.Handler):
    def __init__(self, widget_update_func):
        super().__init__()
        self.widget_update_func = widget_update_func

    def emit(self, record):
        msg = self.format(record)
        self.widget_update_func(msg)

st.set_page_config(page_title="IoT Devices", layout="wide")
logging.basicConfig(level=logging.INFO)
logger = get_logger(__name__)
def add_to_logs(msg):
    st.session_state["logs"] += str(msg) + "\n"


def main():
    st.title("Hello from dfdemo!")

    streamlit_log_handler = StreamlitLogHandler(add_to_logs)
    streamlit_log_handler.setLevel(logging.INFO)
    logger.addHandler(streamlit_log_handler)
    st.code(st.session_state.get("logs", ""), language="text", height=300)


if __name__ == "__main__":
    main()
