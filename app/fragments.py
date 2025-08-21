from datetime import datetime
from time import sleep
import pandas as pd
import streamlit as st

import restcalls
from utils import not_implemented


@st.fragment
def data_transformation():
    keys = st.session_state.source_dataframe.columns
    cols = st.columns(4, gap="medium", vertical_alignment="center")
    with cols[0]:
        st.selectbox(
            "Index",
            options=keys,
            index=None,
            placeholder="Select column for index",
            key="index_column",
        )
        st.write(f"Index: {st.session_state.index_column}")
    with cols[1]:
        st.multiselect(
            "Remove",
            [k for k in keys if k != st.session_state.index_column],
            placeholder="Columns to remove",
            key="remove_columns",
        )
        st.write(f"Remove: {st.session_state.remove_columns}")
    with cols[2]:
        st.selectbox(
            "Mask",
            [
                k
                for k in keys
                if k not in st.session_state.remove_columns
                and k != st.session_state.index_column
            ],
            index=None,
            placeholder="Column to mask",
            key="mask_column",
        )
        st.write(f"Mask: {st.session_state.mask_column}")
    with cols[3]:
        st.selectbox(
            "Label",
            [
                k
                for k in keys
                if k not in st.session_state.remove_columns
                and k != st.session_state.index_column
            ],
            index=None,
            placeholder="Apply category using AI",
            key="label_column",
        )
        st.write(f"Label: {st.session_state.label_column}")


@st.fragment
def show_refined_data():
    # Data transformation
    df: pd.DataFrame = st.session_state.source_dataframe.copy()
    if st.session_state.index_column:
        df.set_index(st.session_state.index_column, inplace=True)
        # if st.session_state.index_column.lower() == "timestamp":
        #     df.index = pd.to_datetime(st.session_state.topic_data['timestamp'], unit="s")
    if st.session_state.remove_columns:
        df.drop(columns=st.session_state.remove_columns, inplace=True)
    if st.session_state.mask_column:
        df[st.session_state.mask_column] = df[st.session_state.mask_column].apply(
            lambda x: str(x)[:2] + "*****"
        )
    if st.session_state.label_column:
        df["category_type"] = df[st.session_state.label_column].apply(not_implemented)
    st.dataframe(df, height=300)
    # st.code(df.head())
    st.session_state["refined_data"] = df
