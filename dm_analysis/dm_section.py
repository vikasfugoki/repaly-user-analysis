import streamlit as st
import boto3
import pandas as pd
from boto3.dynamodb.conditions import Key
from decimal import Decimal
import streamlit.components.v1 as components
import plotly.express as px


# ── table / index names ────────────────────────────────────────────────────────

instagram_messages_table         = "instagram_messages"
instagram_messages_details_table = "instagram_messages_details"
instagram_messages_details_gsi   = "business_user_id-index"
instagram_messages_gsi           = "account_id-timestamp-index"


# ── utilities ──────────────────────────────────────────────────────────────────

def _dec_to_native(obj):
    if isinstance(obj, Decimal):
        v = float(obj)
        return int(v) if v == int(v) else v
    if isinstance(obj, dict):
        return {k: _dec_to_native(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_dec_to_native(i) for i in obj]
    return obj


def _csv_download(df: pd.DataFrame, filename: str, label: str = "⬇ Download CSV") -> None:
    """Render a Streamlit download button for a DataFrame as CSV."""
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label=label,
        data=csv_bytes,
        file_name=filename,
        mime="text/csv",
        key=f"dl_{filename}",
    )


# ── data fetching ──────────────────────────────────────────────────────────────

def get_dm_details(business_user_id: str, dynamodb) -> list:
    """
    Query instagram_messages_details via GSI business_user_id-index.
    Sort key: latest_msg_timestamp (String) — newest first.
    """
    table  = dynamodb.Table(instagram_messages_details_table)
    items  = []
    kwargs = {
        "IndexName": instagram_messages_details_gsi,
        "KeyConditionExpression": Key("business_user_id").eq(business_user_id),
        "ScanIndexForward": False,  # newest first by latest_msg_timestamp
    }
    while True:
        response = table.query(**kwargs)
        items.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
        kwargs["ExclusiveStartKey"] = last_key
    return items


def get_full_conversation(conv_id: str, dynamodb) -> dict:
    """Fetch one row from instagram_messages by primary key 'id'."""
    table    = dynamodb.Table(instagram_messages_table)
    response = table.get_item(Key={"id": conv_id})
    return response.get("Item") or {}


def get_conversations_for_timeseries(account_id: str, dynamodb) -> list:
    """
    Query instagram_messages via GSI account_id-timestamp-index.
    Returns only id + creation_timestamp — lightweight for timeseries.
    """
    table  = dynamodb.Table(instagram_messages_table)
    items  = []
    kwargs = {
        "IndexName": instagram_messages_gsi,
        "KeyConditionExpression": Key("account_id").eq(account_id),
        "ProjectionExpression": "id, creation_timestamp",
        "ScanIndexForward": True,   # oldest → newest so chart is naturally ordered
    }
    while True:
        response = table.query(**kwargs)
        items.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
        kwargs["ExclusiveStartKey"] = last_key
    return items


def _plot_conversation_timeseries(account_id: str, dynamodb) -> None:
    """Fetch creation_timestamps and render a 30-min bucket line chart."""
    with st.spinner("Loading conversation timeseries…"):
        rows = get_conversations_for_timeseries(account_id, dynamodb)

    if not rows:
        st.caption("No timeseries data available.")
        return

    ts_series = pd.to_datetime(
        [r["creation_timestamp"] for r in rows if r.get("creation_timestamp")],
        utc=True,
        errors="coerce",
    ).dropna()

    if ts_series.empty:
        st.caption("No valid timestamps found.")
        return

    agg = (
        ts_series.to_frame(name="ts")
        .assign(bucket=lambda d: d["ts"].dt.floor("30min"))
        .groupby("bucket")
        .size()
        .reset_index(name="New Conversations")
        .sort_values("bucket")
    )

    fig = px.line(
        agg, x="bucket", y="New Conversations",
        markers=True,
        labels={"bucket": "Time (30-min buckets)", "New Conversations": "New Conversations"},
        title="New DM Conversations — 30-minute buckets",
    )
    fig.update_traces(
        line=dict(color="#4361ee", width=2.5),
        marker=dict(color="#4361ee", size=6),
        fill="tozeroy",
        fillcolor="rgba(67,97,238,0.08)",
    )
    fig.update_layout(
        height=320,
        margin=dict(t=40, b=80, l=0, r=0),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(tickformat="%H:%M\n%b %d", tickfont=dict(size=10), gridcolor="#f0f0f0"),
        yaxis=dict(rangemode="tozero", gridcolor="#f0f0f0"),
        font=dict(family="DM Sans, sans-serif", size=12),
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True)


# ── table builder ──────────────────────────────────────────────────────────────

def _build_dm_table(details: list) -> pd.DataFrame:
    rows = []
    for d in details:
        profile = _dec_to_native(d.get("follower_profile") or {})
        rows.append({
            "conv_id":          d.get("id", ""),
            "follower_user_id": d.get("follower_user_id", ""),
            "username":         profile.get("username", "—"),
            "name":             profile.get("name", "—"),
            "follower_count":   str(profile.get("follower_count", "—")),
            "is_following_you": bool(profile.get("is_user_follow_business", False)),
            "category":         d.get("category", "—"),
            "is_category_ai":   bool(d.get("is_category_AI", False)),
            "latest_msg_ts":    d.get("latest_msg_timestamp", ""),
            "unreplied":        bool(d.get("unreplied", False)),
            "profile_pic":      profile.get("profile_pic", ""),
            "_detail":          d,
        })
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ── conversation modal ─────────────────────────────────────────────────────────

@st.dialog("💬 Conversation", width="large")
def _show_dm_modal(detail: dict, dynamodb) -> None:
    conv_id     = detail.get("id", "")
    business_id = detail.get("business_user_id", "")
    profile     = _dec_to_native(detail.get("follower_profile") or {})
    username    = profile.get("username", "—")
    name        = profile.get("name", "—")
    pic_url     = profile.get("profile_pic", "")
    category    = detail.get("category", "—")
    unreplied   = detail.get("unreplied", False)

    # ── header: avatar + name + badges
    pic_html = (
        f'<img src="{pic_url}" style="width:44px;height:44px;border-radius:50%;'
        f'object-fit:cover;border:2px solid #4361ee;flex-shrink:0;" '
        f'onerror="this.style.display=\'none\'">'
        if pic_url else
        '<div style="width:44px;height:44px;border-radius:50%;background:#4361ee22;'
        'display:flex;align-items:center;justify-content:center;font-size:1.2rem;flex-shrink:0;">👤</div>'
    )
    CAT_COLOURS = {
        "Inquiry": "#f77f00", "Positive": "#2dc653", "Negative": "#e63946",
        "Potential Buyer": "#0096c7",
    }
    cat_colour = CAT_COLOURS.get(category, "#6c757d")
    unreplied_badge = (
        '<span style="background:#e6394614;color:#e63946;border:1px solid #e6394666;'
        'border-radius:10px;padding:1px 8px;font-size:0.72rem;font-weight:600;">⚠ Unreplied</span>'
        if unreplied else ""
    )
    st.markdown(
        f'<div style="display:flex;align-items:center;gap:12px;font-family:sans-serif;">'
        f'  {pic_html}'
        f'  <div style="flex:1;">'
        f'    <div style="font-weight:700;font-size:1rem;color:#1a1a2e;">@{username}'
        f'      <span style="font-weight:400;font-size:0.82rem;color:#6c757d;margin-left:6px;">{name}</span>'
        f'    </div>'
        f'    <div style="margin-top:4px;display:flex;gap:6px;align-items:center;flex-wrap:wrap;">'
        f'      <span style="background:{cat_colour}14;color:{cat_colour};border:1px solid {cat_colour}55;'
        f'             border-radius:10px;padding:1px 9px;font-size:0.72rem;font-weight:600;">{category}</span>'
        f'      {unreplied_badge}'
        f'    </div>'
        f'  </div>'
        f'</div>',
        unsafe_allow_html=True,
    )
    st.divider()

    # ── fetch full conversation on demand
    with st.spinner("Loading messages…"):
        conv        = _dec_to_native(get_full_conversation(conv_id, dynamodb))
    messages        = conv.get("messages", [])
    messages_sorted = sorted(messages, key=lambda m: m.get("timestamp", ""))

    if not messages_sorted:
        st.info("No messages found for this conversation.")
        return

    st.caption(f"{len(messages_sorted)} messages")

    # ── chat bubbles — plain text, chronological
    bubble_html = '<div style="display:flex;flex-direction:column;gap:8px;padding:4px 0;">'
    for msg in messages_sorted:
        sender_id  = msg.get("sender_id", "")
        is_you     = sender_id == business_id
        msg_type   = msg.get("msg_type", "text")
        content    = msg.get("content", "")
        timestamp  = msg.get("timestamp", "")
        is_deleted = msg.get("is_deleted", False)

        if is_deleted:
            text = "🗑️ Message deleted"
        elif msg_type == "text" and isinstance(content, str):
            text = content.strip()
        else:
            text = f"[{msg_type}]"

        ts_fmt   = timestamp[11:16] + "  " + timestamp[:10] if len(timestamp) >= 16 else timestamp
        align    = "flex-end"            if is_you else "flex-start"
        bg       = "#4361ee"             if is_you else "#f1f3f5"
        fg       = "#ffffff"             if is_you else "#1a1a2e"
        radius   = "16px 4px 16px 16px" if is_you else "4px 16px 16px 16px"
        ts_align = "right"               if is_you else "left"
        safe_text = (
            text.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\n", "<br>")
        )

        bubble_html += (
            f'<div style="display:flex;justify-content:{align};">'
            f'  <div style="max-width:70%;">'
            f'    <div style="background:{bg};color:{fg};border-radius:{radius};'
            f'                padding:9px 13px;font-size:0.88rem;line-height:1.5;'
            f'                word-break:break-word;box-shadow:0 1px 2px rgba(0,0,0,0.07);">'
            f'      {safe_text}'
            f'    </div>'
            f'    <div style="font-size:0.67rem;color:#adb5bd;margin-top:2px;text-align:{ts_align};">'
            f'      {ts_fmt}'
            f'    </div>'
            f'  </div>'
            f'</div>'
        )
    bubble_html += "</div>"

    components.html(
        '<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;600&display=swap" rel="stylesheet">'
        f'<div style="font-family:\'DM Sans\',sans-serif;overflow-y:auto;max-height:500px;padding:4px 2px;">'
        f'{bubble_html}</div>',
        height=540,
        scrolling=True,
    )


# ── main renderer ──────────────────────────────────────────────────────────────

def render_dm_analysis(business_user_id: str, dynamodb, account_id: str = None) -> None:
    """
    Entry point. Call this from your main app:
        from dm_analysis import render_dm_analysis
        render_dm_analysis(business_user_id, dynamodb, account_id=account_id)

    account_id is required for the timeseries plot (instagram_messages GSI).
    If omitted, the timeseries section is skipped.
    """
    with st.spinner("Loading DM conversations…"):
        details = get_dm_details(business_user_id, dynamodb)

    if not details:
        st.info("No DM conversations found for this account.")
        return

    df = _build_dm_table(details)

    # ── summary metrics
    total_convs  = len(df)
    unreplied_ct = int(df["unreplied"].sum())
    ai_cat_ct    = int(df["is_category_ai"].sum())
    following_ct = int(df["is_following_you"].sum())
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Conversations", f"{total_convs:,}")
    c2.metric("Unreplied",           f"{unreplied_ct:,}")
    c3.metric("AI Categorised",      f"{ai_cat_ct:,}")
    c4.metric("Following You",       f"{following_ct:,}")
    st.divider()

    # ── timeseries plot
    if account_id:
        st.markdown("#### 📈 Conversation volume over time")
        _plot_conversation_timeseries(account_id, dynamodb)
        st.divider()

    # ── filters
    fa, fb, fc, fd = st.columns([3, 2, 1, 1])
    search_term    = fa.text_input(
        "🔍 Search username, name or category",
        placeholder="Type to filter…", key="dm_search",
    )
    sort_col       = fb.selectbox(
        "Sort by", ["latest_msg_ts", "username", "category"], key="dm_sort",
    )
    show_unreplied = fc.checkbox("Unreplied only", value=False, key="dm_unreplied")
    all_cats       = ["All"] + sorted(df["category"].dropna().unique().tolist())
    cat_filter     = fd.selectbox("Category", all_cats, key="dm_cat_filter")

    view = df.copy()
    if show_unreplied:
        view = view[view["unreplied"] == True]
    if cat_filter != "All":
        view = view[view["category"] == cat_filter]
    if search_term:
        mask = (
            view["username"].str.contains(search_term, case=False, na=False)
            | view["name"].str.contains(search_term, case=False, na=False)
            | view["category"].str.contains(search_term, case=False, na=False)
            | view["follower_user_id"].str.contains(search_term, case=False, na=False)
        )
        view = view[mask]
    view = view.sort_values(sort_col, ascending=(sort_col == "username")).reset_index(drop=True)

    if view.empty:
        st.warning("No conversations match the current filter.")
        return

    dl_col, cap_col = st.columns([1, 4])
    export_df = view[[
        "follower_user_id", "username", "name", "category",
        "latest_msg_ts", "follower_count", "is_following_you", "unreplied",
    ]].copy()
    with dl_col:
        _csv_download(export_df, "dm_conversations.csv")
    with cap_col:
        st.caption(f"Showing **{len(view)}** of {total_convs} conversations")

    # ── HTML table
    CAT_COLOURS = {
        "Inquiry": "#f77f00", "Positive": "#2dc653", "Negative": "#e63946",
        "Potential Buyer": "#0096c7", "Others": "#6c757d",
    }
    header_cells = (
        "<th>#</th>"
        "<th>Username</th>"
        "<th>Name</th>"
        "<th style='min-width:110px'>Category</th>"
        "<th style='min-width:130px'>Latest Msg Time</th>"
        "<th style='min-width:70px;text-align:center'>Followers</th>"
        "<th style='min-width:70px;text-align:center'>Following</th>"
        "<th style='min-width:70px;text-align:center'>Unreplied</th>"
        "<th style='min-width:70px;text-align:center'>Open</th>"
    )
    rows_html = ""
    for rank, (_, row) in enumerate(view.iterrows(), 1):
        latest_ts     = row["latest_msg_ts"][:16].replace("T", " ") if row["latest_msg_ts"] else "—"
        stripe        = "background:#f8f9fa" if rank % 2 == 0 else ""
        mouseout      = "#f8f9fa" if rank % 2 == 0 else "transparent"
        cat           = str(row["category"])
        cat_col       = CAT_COLOURS.get(cat, "#6c757d")
        cat_badge     = (
            f'<span style="background:{cat_col}14;color:{cat_col};border:1px solid {cat_col}55;'
            f'border-radius:10px;padding:1px 8px;font-size:0.75rem;font-weight:600">{cat}</span>'
        )
        unreplied_cell = (
            '<span style="color:#e63946;font-weight:700">Yes</span>'
            if row["unreplied"] else '<span style="color:#adb5bd">—</span>'
        )
        following_cell = (
            '<span style="color:#2dc653;font-weight:700">✓</span>'
            if row["is_following_you"] else '<span style="color:#adb5bd">—</span>'
        )
        username_safe = str(row["username"]).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        name_safe     = str(row["name"]).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

        rows_html += (
            f'<tr style="{stripe};transition:background .15s" '
            f'onmouseover="this.style.background=\'#eef2ff\'" '
            f'onmouseout="this.style.background=\'{mouseout}\'">'
            f'<td style="color:#adb5bd;text-align:center;font-size:0.8rem">{rank}</td>'
            f'<td style="font-weight:600;color:#4361ee;font-size:0.85rem">@{username_safe}</td>'
            f'<td style="font-size:0.85rem;color:#495057">{name_safe}</td>'
            f'<td>{cat_badge}</td>'
            f'<td style="font-size:0.82rem;color:#495057;white-space:nowrap">{latest_ts}</td>'
            f'<td style="text-align:center;font-size:0.82rem;color:#6c757d">{row["follower_count"]}</td>'
            f'<td style="text-align:center">{following_cell}</td>'
            f'<td style="text-align:center">{unreplied_cell}</td>'
            f'<td style="text-align:center">—</td>'
            f'</tr>'
        )

    table_html = (
        """
<style>
  .dm-scroll{overflow-x:auto;overflow-y:auto;max-height:480px;border-radius:10px;border:1px solid #dee2e6;}
  .dm-table{border-collapse:collapse;width:100%;font-size:0.88rem;font-family:'DM Sans',sans-serif;}
  .dm-table th{background:#1a1a2e;color:#e0e0ff;padding:10px 14px;text-align:left;font-weight:600;
    letter-spacing:.04em;font-size:0.78rem;position:sticky;top:0;z-index:2;border-bottom:2px solid #4361ee;}
  .dm-table td{padding:9px 14px;vertical-align:middle;border-bottom:1px solid #f0f0f0;white-space:nowrap;}
  .dm-table tr:last-child td{border-bottom:none;}
</style>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;600&display=swap" rel="stylesheet">
<div class="dm-scroll">
  <table class="dm-table">
    <thead><tr>"""
        + header_cells
        + """</tr></thead>
    <tbody>"""
        + rows_html
        + """</tbody>
  </table>
</div>"""
    )
    components.html(table_html, height=520, scrolling=False)

    # ── selectbox + button to open conversation modal
    st.divider()
    left, right = st.columns([5, 1], vertical_alignment="bottom")
    options = [
        f"#{i+1} · @{row['username']} — {row['category']} · {row['latest_msg_ts'][:10] if row['latest_msg_ts'] else '—'}"
        for i, (_, row) in enumerate(view.iterrows())
    ]
    detail_map = {label: row["_detail"] for label, (_, row) in zip(options, view.iterrows())}
    selected_label = left.selectbox(
        "🔍 Open conversation",
        options,
        index=None,
        placeholder="Select a conversation to view…",
        key="dm_inspect_select",
    )
    open_clicked = right.button(
        "Open ↗", key="dm_inspect_btn", type="primary",
        use_container_width=True, disabled=selected_label is None,
    )
    if open_clicked and selected_label:
        _show_dm_modal(detail_map[selected_label], dynamodb)

    # ── message summary section (lazy — user must opt in) ──────────────────────
    if account_id:
        st.divider()
        st.markdown("#### 🗂️ First & Latest Message per Conversation")
        st.caption("Fetches full message data from `instagram_messages`. Click below to load.")

        if st.button("📥 Analyse Messages", key="load_msg_summary", type="secondary"):
            st.session_state["dm_msg_summary_loaded"] = True

        if st.session_state.get("dm_msg_summary_loaded"):
            _render_message_summary(account_id, dynamodb)

# ── message summary helpers ────────────────────────────────────────────────────

def _get_conversations_for_summary(account_id: str, dynamodb) -> list:
    """Query instagram_messages via account_id-timestamp-index, pull full messages."""
    table  = dynamodb.Table(instagram_messages_table)
    items  = []
    kwargs = {
        "IndexName": instagram_messages_gsi,
        "KeyConditionExpression": Key("account_id").eq(account_id),
        "ScanIndexForward": True,
    }
    while True:
        response = table.query(**kwargs)
        items.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
        kwargs["ExclusiveStartKey"] = last_key
    return items


def _msg_text(msg: dict) -> str:
    """Extract plain text preview from a single message dict."""
    if msg.get("is_deleted"):
        return "[deleted]"
    content  = msg.get("content", "")
    msg_type = msg.get("msg_type", "text")
    if msg_type == "text" and isinstance(content, str) and content.strip():
        t = content.strip()
        return t[:80] + ("…" if len(t) > 80 else "")
    return f"[{msg_type}]"


def _msg_ts_fmt(msg: dict) -> str:
    t = msg.get("timestamp", "")
    return t[:16].replace("T", " ") if len(t) >= 16 else t


def _build_summary_df(conversations: list) -> pd.DataFrame:
    rows = []
    for conv in conversations:
        msgs = sorted(
            _dec_to_native(conv.get("messages", [])),
            key=lambda m: m.get("timestamp", ""),
        )
        if not msgs:
            continue
        first, last = msgs[0], msgs[-1]
        rows.append({
            "follower_user_id": conv.get("follower_user_id", "—"),
            "first_msg":        _msg_text(first),
            "first_ts":         _msg_ts_fmt(first),
            "last_msg":         _msg_text(last),
            "last_ts":          _msg_ts_fmt(last),
            # raw ISO for sorting
            "_first_ts_raw":    first.get("timestamp", ""),
            "_last_ts_raw":     last.get("timestamp", ""),
        })
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _render_message_summary(account_id: str, dynamodb) -> None:
    """Render first/last message table with sort controls."""
    with st.spinner("Loading message summary…"):
        conversations = _get_conversations_for_summary(account_id, dynamodb)

    if not conversations:
        st.info("No conversations found.")
        return

    df = _build_summary_df(conversations)
    if df.empty:
        st.info("No messages found in any conversation.")
        return

    # ── sort controls
    SORT_OPTIONS = {
        "Latest msg — newest first": ("_last_ts_raw",  False),
        "Latest msg — oldest first": ("_last_ts_raw",  True),
        "First msg — newest first":  ("_first_ts_raw", False),
        "First msg — oldest first":  ("_first_ts_raw", True),
        "Follower ID (A→Z)":         ("follower_user_id", True),
    }
    sa, sb = st.columns([3, 1])
    sort_choice = sa.selectbox(
        "Sort by", list(SORT_OPTIONS.keys()),
        key="msg_summary_sort",
    )
    search = sb.text_input("🔍 Filter", placeholder="Follower ID or message…", key="msg_summary_search")

    sort_col, ascending = SORT_OPTIONS[sort_choice]
    view = df.copy()
    if search:
        mask = (
            view["follower_user_id"].str.contains(search, case=False, na=False)
            | view["first_msg"].str.contains(search, case=False, na=False)
            | view["last_msg"].str.contains(search, case=False, na=False)
        )
        view = view[mask]
    view = view.sort_values(sort_col, ascending=ascending).reset_index(drop=True)

    dl_col2, cap_col2 = st.columns([1, 4])
    export_summary = view[[
        "follower_user_id", "first_msg", "first_ts", "last_msg", "last_ts",
    ]].copy()
    with dl_col2:
        _csv_download(export_summary, "dm_message_summary.csv")
    with cap_col2:
        st.caption(f"{len(view)} conversations")

    # ── HTML table
    header_cells = (
        "<th>#</th>"
        "<th>Follower ID</th>"
        "<th style='min-width:280px'>First Message</th>"
        "<th style='min-width:120px'>First Msg Time</th>"
        "<th style='min-width:280px'>Latest Message</th>"
        "<th style='min-width:120px'>Latest Msg Time</th>"
    )
    rows_html = ""
    for rank, (_, row) in enumerate(view.iterrows(), 1):
        stripe   = "background:#f8f9fa" if rank % 2 == 0 else ""
        mouseout = "#f8f9fa" if rank % 2 == 0 else "transparent"

        def _safe(s):
            return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

        first_is_type = row["first_msg"].startswith("[") and row["first_msg"].endswith("]")
        last_is_type  = row["last_msg"].startswith("[")  and row["last_msg"].endswith("]")
        first_col = "#adb5bd" if first_is_type else "#495057"
        last_col  = "#adb5bd" if last_is_type  else "#1a1a2e"

        rows_html += (
            f'<tr style="{stripe};transition:background .15s" '
            f'onmouseover="this.style.background=\'#eef2ff\'" '
            f'onmouseout="this.style.background=\'{mouseout}\'">'
            f'<td style="color:#adb5bd;text-align:center;font-size:0.8rem">{rank}</td>'
            f'<td style="font-family:monospace;font-size:0.8rem;color:#4361ee;font-weight:600">'
            f'  {_safe(row["follower_user_id"])}</td>'
            f'<td style="font-size:0.84rem;color:{first_col};max-width:280px;'
            f'  overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{_safe(row["first_msg"])}</td>'
            f'<td style="font-size:0.8rem;color:#6c757d;white-space:nowrap">{row["first_ts"]}</td>'
            f'<td style="font-size:0.84rem;color:{last_col};font-weight:500;max-width:280px;'
            f'  overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{_safe(row["last_msg"])}</td>'
            f'<td style="font-size:0.8rem;color:#495057;white-space:nowrap">{row["last_ts"]}</td>'
            f'</tr>'
        )

    table_html = (
        """
<style>
  .ms-scroll{overflow-x:auto;overflow-y:auto;max-height:480px;border-radius:10px;border:1px solid #dee2e6;}
  .ms-table{border-collapse:collapse;width:100%;font-size:0.88rem;font-family:'DM Sans',sans-serif;}
  .ms-table th{background:#1a1a2e;color:#e0e0ff;padding:10px 14px;text-align:left;font-weight:600;
    letter-spacing:.04em;font-size:0.78rem;position:sticky;top:0;z-index:2;border-bottom:2px solid #4361ee;}
  .ms-table td{padding:9px 14px;vertical-align:middle;border-bottom:1px solid #f0f0f0;}
  .ms-table tr:last-child td{border-bottom:none;}
</style>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;600&display=swap" rel="stylesheet">
<div class="ms-scroll">
  <table class="ms-table">
    <thead><tr>"""
        + header_cells
        + """</tr></thead>
    <tbody>"""
        + rows_html
        + """</tbody>
  </table>
</div>"""
    )
    components.html(table_html, height=520, scrolling=False)