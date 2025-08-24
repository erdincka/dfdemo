import pandas as pd
import streamlit as st

from utils import not_implemented


@st.fragment
def data_transformation():
    keys = st.session_state.source_dataframe.columns
    cols = st.columns(5, gap="medium", vertical_alignment="center")
    with cols[0]:
        st.selectbox(
            "Index",
            options=keys,
            index=None,
            help="Assign column as index",
            key="index_column",
        )
        st.write(f"Index: {st.session_state.index_column}")
    with cols[1]:
        st.multiselect(
            "Remove",
            [k for k in keys if k != st.session_state.index_column],
            help="Discard column in the output",
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
            help="Column to mask",
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
            help="Apply category using AI (mocked)",
            key="label_column",
        )
        st.write(f"Label: {st.session_state.label_column}")

    with cols[4]:
        if st.button(
            "Apply",
            help="Creates in-memory copy of the input dataset with selected transformation. Output will be written with these ETL processing applied.",
            type="primary",
        ):
            st.rerun()


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
    st.dataframe(df, height=200)
    # st.code(df.head())
    st.session_state["refined_data"] = df
