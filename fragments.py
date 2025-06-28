import streamlit as st

def data_transformation():
        keys = st.session_state.topic_data.columns
        cols = st.columns(4, gap="medium", vertical_alignment="center")
        with cols[0]:
            st.selectbox("Index", 
                options=keys, 
                index=None, 
                placeholder="Select column for index",
                key="index_column")
            st.write(f"Index: {st.session_state.index_column}")
        with cols[1]:
            st.multiselect(
                "Remove", [k for k in keys if k != st.session_state.index_column], placeholder="Columns to remove", key="remove_columns"
            )
            st.write(f"Remove: {st.session_state.remove_columns}")
        with cols[2]:
            st.selectbox(
                "Mask",
                [k for k in keys if k not in st.session_state.remove_columns and k != st.session_state.index_column],
                index=None,
                placeholder="Column to mask",
                key="mask_column",
            )
            st.write(f"Mask: {st.session_state.mask_column}")
        with cols[3]:
            st.selectbox(
                "Label",
                [k for k in keys if k not in st.session_state.remove_columns and k != st.session_state.index_column],
                index=None,
                placeholder="Apply category using AI",
                key="label_column",
            )
            st.write(f"Label: {st.session_state.label_column}")
