import streamlit as st
import pandas as pd
from rapidfuzz import process, fuzz
import re

def strip_suffix(name):
    """Remove Jr., Sr., II, III from player names for consistent matching"""
    if pd.isna(name):
        return ""
    return re.sub(r'\s+(Jr\.|Sr\.|II|III)$', '', name).strip()

# def fuzzy_match_names(name, choices, limit=1, score_cutoff=80):
#     """Return best fuzzy match from a list of choices"""
#     results = process.extract(name, choices, scorer=fuzz.token_sort_ratio, limit=limit, score_cutoff=score_cutoff)
#     return results[0][0] if results else None


# --- Page config ---
st.set_page_config(page_title="Fantasy Football Tool", layout="wide")

# --- Custom CSS for smaller font in dataframe ---
st.markdown(
    """
    <style>
    /* Make dataframe text smaller and compact */
    .stDataFrame div[data-baseweb="table"] td {
        font-size: 12px !important;
        padding: 4px !important;
    }
    .stDataFrame div[data-baseweb="table"] th {
        font-size: 12px !important;
        padding: 4px !important;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# --- Load and clean dataframe ---
try:
    df = pd.read_csv("output.csv")
    cols_to_keep = [
        'RK', 'TIERS', 'PLAYER NAME', 'TEAM', 'POS', 'BYE WEEK',
        'SOS SEASON', 'ECR VS. ADP', 'handcuff', 'is_rookie',
        'is_lottery_ticket', 'is_fantasypros_sleeper'
    ]
    df = df[cols_to_keep]

    rename_dict = {
        'is_rookie':'R',
        'is_lottery_ticket': 'LT',
        'is_fantasypros_sleeper': 'SLPR'
    }
    df = df.rename(columns=rename_dict)
except FileNotFoundError:
    st.error("`output.csv` not found. Please ensure the file is in the same directory.")
    st.stop()
except Exception as e:
    st.error(f"An error occurred while loading the dataframe: {e}")
    st.stop()


# --- Initialize session state ---
if "df_filtered" not in st.session_state:
    st.session_state.df_filtered = df.copy()
if "removed_stack" not in st.session_state:
    st.session_state.removed_stack = []

# --- Callback function to handle player removal ---
def remove_players_callback():
    """Parses user input and removes players from the dataframe using normalized names."""
    user_input = st.session_state.user_input_key
    names_to_remove = []
    lines = user_input.splitlines()
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        # ESPN format: take text before first "/"
        if "/" in line:
            names_to_remove.append(line.split("/")[0].strip())
        # elif ',' in line:
        #     names_to_remove.extend(name.strip() for name in line.split(","))
        # # else:
        # #     names_to_remove.append(line)
    
    # Strip suffixes from user input
    names_to_remove = [strip_suffix(n) for n in names_to_remove]

    print(names_to_remove)
    
    # Strip suffixes from dataframe names
    df_names_normalized = st.session_state.df_filtered["PLAYER NAME"].apply(strip_suffix)
    
    # Do removal
    mask = df_names_normalized.str.lower().isin([n.lower() for n in names_to_remove])
    removed_rows = st.session_state.df_filtered[mask]
    
    if not removed_rows.empty:
        st.session_state.removed_stack.append(removed_rows)
        st.session_state.df_filtered = st.session_state.df_filtered[~mask]
        st.toast(f"Removed: {', '.join([n.title() for n in names_to_remove])}")
    else:
        st.toast("No matching players found.")

    st.session_state.user_input_key = ""

def undo_removal_callback():
    """Restores the last set of removed players."""
    if st.session_state.removed_stack:
        last_removed = st.session_state.removed_stack.pop()
        st.session_state.df_filtered = pd.concat(
            [st.session_state.df_filtered, last_removed]
        ).sort_index()
        st.toast("Undo successful!")
    else:
        st.toast("Nothing to undo.")


# --- Display dataframe ---
st.title("Fantasy Football Tool")
st.subheader("Rankings Table")
st.dataframe(st.session_state.df_filtered, width="content", height=600)

# --- One-click form for removing players with callback ---
st.subheader("Remove Players")
with st.form(key="remove_form"):
    user_input = st.text_area(
        "Paste ESPN Draft History or enter player names",
        height=150,
        key="user_input_key" # Added a key to access the value in the callback
    )
    submit_button = st.form_submit_button(
        label="Remove Players",
        on_click=remove_players_callback
    )

# --- Undo button ---
# --- Undo callback function ---


# --- Undo button ---
st.subheader("Undo Removal")
st.button("Undo", on_click=undo_removal_callback)

