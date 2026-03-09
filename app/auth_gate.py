from __future__ import annotations

import hmac
import os

import streamlit as st


def enforce_basic_auth() -> None:
    """Require a basic login when auth env vars are configured."""
    expected_username = os.getenv("APP_BASIC_AUTH_USERNAME", "")
    expected_password = os.getenv("APP_BASIC_AUTH_PASSWORD", "")

    has_username = bool(expected_username)
    has_password = bool(expected_password)

    # Local-dev convenience: auth disabled when both credentials are unset.
    if not has_username and not has_password:
        return

    if has_username != has_password:
        st.error(
            "Auth configuration is incomplete. Set both APP_BASIC_AUTH_USERNAME "
            "and APP_BASIC_AUTH_PASSWORD, or unset both to disable auth."
        )
        st.stop()

    if st.session_state.get("app_basic_auth_authenticated", False):
        return

    st.title("AI Lead Generation Agents")
    st.caption("Sign in to continue.")

    with st.form("app_basic_auth_form", clear_on_submit=False):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Sign In", use_container_width=True)

    if submitted:
        username_match = hmac.compare_digest(username, expected_username)
        password_match = hmac.compare_digest(password, expected_password)
        if username_match and password_match:
            st.session_state["app_basic_auth_authenticated"] = True
            st.rerun()
        else:
            st.error("Invalid username or password.")

    st.stop()
