import streamlit as st
import pandas as pd

# --- Initialize dataframe and session state ---
# This ensures the dataframe persists across reruns.
# The `if` statement makes sure it's only created once.
if "df" not in st.session_state:
    st.session_state.df = pd.DataFrame({
        'col_1': [1, 2, 3],
        'col_2': ['A', 'B', 'C']
    })

def remove_row():
    """Removes the top row from the dataframe in session state."""
    if not st.session_state.df.empty:
        st.session_state.df = st.session_state.df.iloc[1:]

st.title("Simple Dataframe App")

# --- Display the dataframe ---
st.subheader("Current Data")
st.dataframe(st.session_state.df)

# --- Button to remove the top row ---
# Now using a callback function for a guaranteed single-click update.
st.button("Remove Top Row", on_click=remove_row)

# --- Button to reload/reset the dataframe ---
# This will restore the dataframe to its initial state
if st.button("Reset Data"):
    st.session_state.df = pd.DataFrame({
        'col_1': [1, 2, 3],
        'col_2': ['A', 'B', 'C']
    })
    st.success("Dataframe reloaded!")