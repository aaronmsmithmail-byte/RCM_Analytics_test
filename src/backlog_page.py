"""Feature Backlog page — simplified Jira-style board for tracking feature requests.

Items are persisted in the ``feature_backlog`` DuckDB table so they survive
application restarts.  Users can create new items, and the development team
can update status via an inline dropdown.
"""

from datetime import datetime

import pandas as pd
import streamlit as st

from src.database import get_connection

# ── Priority / status colours for visual distinction ─────────────────

_PRIORITY_ICONS = {
    "Critical": "🔴",
    "High": "🟠",
    "Medium": "🟡",
    "Low": "🟢",
}

_STATUS_OPTIONS = ["Not Started", "In Progress", "Waiting for Review", "Completed"]
_PRIORITY_OPTIONS = ["Critical", "High", "Medium", "Low"]

_STATUS_ICONS = {
    "Not Started": "⬜",
    "In Progress": "🔵",
    "Waiting for Review": "🟣",
    "Completed": "✅",
}

# ── DB helpers ────────────────────────────────────────────────────────


def _load_backlog() -> pd.DataFrame:
    """Load all backlog items from DuckDB, ordered by priority then date."""
    priority_order = "CASE priority WHEN 'Critical' THEN 1 WHEN 'High' THEN 2 WHEN 'Medium' THEN 3 WHEN 'Low' THEN 4 ELSE 5 END"
    try:
        conn = get_connection(read_only=False)
        df = conn.execute(
            f"SELECT * FROM feature_backlog ORDER BY {priority_order}, created_at DESC"
        ).fetchdf()
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


def _insert_item(title, description, priority, acceptance_criteria, benefits):
    """Insert a new backlog item."""
    conn = get_connection(read_only=False)
    conn.execute(
        "INSERT INTO feature_backlog "
        "(title, description, priority, acceptance_criteria, benefits, status) "
        "VALUES (?, ?, ?, ?, ?, 'Not Started')",
        (title, description, priority, acceptance_criteria, benefits),
    )
    conn.close()


def _update_status(item_id: int, new_status: str):
    """Update the status of a backlog item."""
    conn = get_connection(read_only=False)
    conn.execute(
        "UPDATE feature_backlog SET status = ?, updated_at = ? WHERE id = ?",
        (new_status, datetime.now(), item_id),
    )
    conn.close()


def _delete_item(item_id: int):
    """Delete a backlog item."""
    conn = get_connection(read_only=False)
    conn.execute("DELETE FROM feature_backlog WHERE id = ?", (item_id,))
    conn.close()


# ── Main render function ──────────────────────────────────────────────


def render_feature_backlog():
    """Render the feature backlog page with board view and new-item form."""
    st.title("Feature Backlog")
    st.caption(
        "Track feature requests and enhancements. "
        "Submit new ideas, set priorities, and update status as work progresses."
    )

    # Load current items
    df = _load_backlog()

    # ── Summary scorecards ────────────────────────────────────────────
    total = len(df)
    if total > 0:
        status_counts = df["status"].value_counts().to_dict()
    else:
        status_counts = {}

    cols = st.columns(5)
    with cols[0]:
        st.metric("Total Items", total)
    for i, status in enumerate(_STATUS_OPTIONS):
        with cols[i + 1]:
            count = status_counts.get(status, 0)
            st.metric(f"{_STATUS_ICONS[status]} {status}", count)

    st.divider()

    # ── Board view ────────────────────────────────────────────────────
    st.subheader("Backlog Items")

    if df.empty:
        st.info("No backlog items yet. Use the form below to add your first feature request.")
    else:
        for _, row in df.iterrows():
            priority_icon = _PRIORITY_ICONS.get(row["priority"], "⚪")
            status_icon = _STATUS_ICONS.get(row["status"], "⬜")

            with st.expander(
                f"{priority_icon} {row['title']}  —  {status_icon} {row['status']}",
                expanded=False,
            ):
                # Status update row
                col_status, col_delete = st.columns([3, 1])
                with col_status:
                    current_idx = (
                        _STATUS_OPTIONS.index(row["status"])
                        if row["status"] in _STATUS_OPTIONS
                        else 0
                    )
                    new_status = st.selectbox(
                        "Status",
                        _STATUS_OPTIONS,
                        index=current_idx,
                        key=f"status_{row['id']}",
                    )
                    if new_status != row["status"]:
                        _update_status(row["id"], new_status)
                        st.rerun()
                with col_delete:
                    st.markdown("")  # spacer
                    if st.button("🗑️ Delete", key=f"del_{row['id']}", type="secondary"):
                        _delete_item(row["id"])
                        st.rerun()

                # Detail fields
                st.markdown(f"**Priority:** {priority_icon} {row['priority']}")
                st.markdown(f"**Description:**  \n{row['description']}")

                if row.get("acceptance_criteria"):
                    st.markdown("**Acceptance Criteria:**")
                    # Display numbered criteria as a list
                    criteria = str(row["acceptance_criteria"]).replace("\\n", "\n")
                    st.markdown(criteria)

                if row.get("benefits"):
                    st.markdown(f"**Benefits:**  \n{row['benefits']}")

                # Timestamps
                created = row.get("created_at", "")
                updated = row.get("updated_at", "")
                if created:
                    ts = pd.Timestamp(created)
                    st.caption(
                        f"Created: {ts.strftime('%b %d, %Y %I:%M %p')}"
                        + (
                            f" · Updated: {pd.Timestamp(updated).strftime('%b %d, %Y %I:%M %p')}"
                            if updated and str(updated) != str(created)
                            else ""
                        )
                    )

    st.divider()

    # ── New item form ─────────────────────────────────────────────────
    st.subheader("➕ Submit New Feature Request")

    with st.form("new_backlog_item", clear_on_submit=True):
        title = st.text_input("Title *", placeholder="e.g. Automated denial appeal workflow")
        description = st.text_area(
            "Description *",
            placeholder="Describe the feature or enhancement in detail...",
            height=100,
        )
        col_p, col_spacer = st.columns([1, 2])
        with col_p:
            priority = st.selectbox("Priority *", _PRIORITY_OPTIONS, index=2)

        acceptance_criteria = st.text_area(
            "Acceptance Criteria",
            placeholder="1. The system should...\n2. Users can...\n3. Data is...",
            height=100,
        )
        benefits = st.text_area(
            "Benefits",
            placeholder="Explain how this feature will improve the RCM workflow...",
            height=80,
        )

        submitted = st.form_submit_button("Submit Feature Request", type="primary")
        if submitted:
            if not title.strip() or not description.strip():
                st.error("Title and Description are required.")
            else:
                _insert_item(
                    title.strip(),
                    description.strip(),
                    priority,
                    acceptance_criteria.strip() or None,
                    benefits.strip() or None,
                )
                st.success(f"✅ Feature request \"{title.strip()}\" submitted!")
                st.rerun()
