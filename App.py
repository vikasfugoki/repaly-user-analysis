import streamlit as st
import boto3
import pandas as pd
from collections import defaultdict
from boto3.dynamodb.conditions import Attr
import plotly.express as px
from decimal import Decimal
import streamlit.components.v1 as components

st.set_page_config(
    page_title="Repaly Analytics",
    page_icon="📊",
    layout="wide",
)

# ── password gate ──────────────────────────────────────────────────────────────

def check_password():
    if st.session_state.get("authenticated"):
        return True
    st.title("🔐 Repaly Analytics")
    pwd = st.text_input("Password", type="password", key="pwd_input")
    if st.button("Login", type="primary"):
        if pwd == st.secrets["APP_PASSWORD"]:
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("Wrong password")
    st.stop()

check_password()

# ── AWS setup (from secrets) ───────────────────────────────────────────────────

session = boto3.Session(
    aws_access_key_id     = st.secrets["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key = st.secrets["AWS_SECRET_ACCESS_KEY"],
    region_name           = st.secrets["AWS_REGION"],
)
dynamodb = session.resource("dynamodb")

instagram_account_table          = "instagram_account_repository"
instagram_media_analytics_table  = "instagram_analytics_repository"
instagram_media_table            = "instagram_media_repository"

# ── data functions ─────────────────────────────────────────────────────────────

def get_item_by_pk(table_name: str, pk_name: str, pk_value) -> dict | None:
    table    = dynamodb.Table(table_name)
    response = table.get_item(Key={pk_name: pk_value})
    return response.get("Item")


def get_items_by_sk(table_name: str, sk_name: str, sk_value) -> list:
    table       = dynamodb.Table(table_name)
    items       = []
    scan_kwargs = {"FilterExpression": Attr(sk_name).eq(sk_value)}
    while True:
        response = table.scan(**scan_kwargs)
        items.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
        scan_kwargs["ExclusiveStartKey"] = last_key
    return items


def get_post_comment_totals(media_analytics: list, media_details: list) -> pd.DataFrame:
    records = []
    for item, detail in zip(media_analytics, media_details):
        post_id        = item.get("id")
        comment_counts = item.get("comment_counts", {})
        total           = sum(int(v) for v in comment_counts.values())
        inquiry         = sum(int(v) for k, v in comment_counts.items() if "inquiry"        in k)
        negative        = sum(int(v) for k, v in comment_counts.items() if "negative"       in k)
        positive        = sum(int(v) for k, v in comment_counts.items() if "positive"       in k)
        others          = sum(int(v) for k, v in comment_counts.items() if "others"         in k)
        tagged          = sum(int(v) for k, v in comment_counts.items() if "tagged"         in k)
        potential_buyer = sum(int(v) for k, v in comment_counts.items() if "potential_buyer" in k)
        records.append({
            "id":             post_id,
            "post_link":      detail.get("permalink"),
            "total":          total,
            "inquiry":        inquiry,
            "negative":       negative,
            "positive":       positive,
            "others":         others,
            "tagged":         tagged,
            "potential_buyer": potential_buyer,
            "_comment_counts": comment_counts,
        })
    return pd.DataFrame(records).sort_values("total", ascending=False).reset_index(drop=True)


def get_category_totals(items: list) -> dict:
    category_totals = defaultdict(int)
    for item in items:
        for category, count in item.get("comment_counts", {}).items():
            category_totals[category] += int(count)
    return dict(category_totals)


def get_per_media_analytics(post_id: str) -> tuple:
    item         = get_item_by_pk(instagram_media_analytics_table, "id", post_id) or {}
    item_details = get_item_by_pk(instagram_media_table, "id", post_id) or {}
    return item, item_details.get("tag_and_value_pair"), item_details.get("ai_enabled")

# ── chart helpers ──────────────────────────────────────────────────────────────

def plot_category_data(data: dict, title: str = "Comment Category Distribution"):
    if not data:
        st.warning("No data to display.")
        return
    df = (
        pd.DataFrame(list(data.items()), columns=["Category", "Count"])
        .sort_values("Count", ascending=False)
        .reset_index(drop=True)
    )
    df["Label"] = df["Category"].str.replace("_", " ").str.title()
    total        = df["Count"].sum()
    top          = df.iloc[0]

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Events",  f"{total:,}")
    col2.metric("Categories",    len(df))
    col3.metric("Top Category",  top["Label"], f"{top['Count']:,} events")
    st.divider()

    fig = px.bar(
        df, x="Label", y="Count", text="Count", color="Count",
        color_continuous_scale="Blues",
        labels={"Label": "Category", "Count": "Count"},
        title=title,
    )
    fig.update_traces(texttemplate="%{text:,}", textposition="outside")
    fig.update_layout(
        coloraxis_showscale=False,
        xaxis_tickangle=-35,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="sans-serif", size=13),
        margin=dict(t=60, b=120),
        height=480,
    )
    st.plotly_chart(fig, use_container_width=True)

# ── constants ──────────────────────────────────────────────────────────────────

def _dec_to_native(obj):
    if isinstance(obj, Decimal):
        v = float(obj)
        return int(v) if v == int(v) else v
    if isinstance(obj, dict):
        return {k: _dec_to_native(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_dec_to_native(i) for i in obj]
    return obj


def _bar_html(value: int, max_value: int, colour: str) -> str:
    pct = (value / max_value * 100) if max_value else 0
    return (
        '<div style="display:flex;align-items:center;gap:6px">'
        '<div style="flex:1;background:#e9ecef;border-radius:4px;height:8px;">'
        f'<div style="width:{pct:.1f}%;background:{colour};border-radius:4px;height:8px;"></div>'
        '</div>'
        f'<span style="font-variant-numeric:tabular-nums;min-width:36px;text-align:right">{value}</span>'
        '</div>'
    )


COL_COLOURS = {
    "total":           "#4361ee",
    "inquiry":         "#f77f00",
    "negative":        "#e63946",
    "positive":        "#2dc653",
    "tagged":          "#7b2d8b",
    "others":          "#6c757d",
    "potential_buyer": "#0096c7",
}
DISPLAY_COLS = ["total", "inquiry", "negative", "positive", "tagged", "others", "potential_buyer"]

TYPE_BADGE_COLOURS = {
    "others":                  "#6c757d",
    "positive":                "#2dc653",
    "positive no automation":  "#74c69d",
    "negative":                "#e63946",
    "negative no automation":  "#ff6b6b",
    "inquiry":                 "#f77f00",
    "inquiry dm":              "#f4a261",
    "inquiry no automation":   "#e9c46a",
    "tagged comment":          "#7b2d8b",
    "tagged comment dm":       "#9d4edd",
    "potential buyers":        "#0096c7",
}

# ── modal ──────────────────────────────────────────────────────────────────────

@st.dialog("📬 Post Detail", width="large")
def _show_modal(post_detail: dict, tag_and_value_pair: list = None, ai_enabled: dict = None) -> None:
    post_id          = post_detail.get("id", "—")
    counts           = _dec_to_native(post_detail.get("comment_counts", {}))
    comments_by_type = _dec_to_native(post_detail.get("comments_by_type", {}))

    st.markdown(f"**Post ID:** `{post_id}`")
    st.divider()

    # 1 ── count badges
    st.markdown("##### Comment counts")
    badges = ""
    for cat, cnt in sorted(counts.items(), key=lambda x: -x[1]):
        label  = cat.replace("_", " ").title()
        colour = TYPE_BADGE_COLOURS.get(label.lower(), "#888")
        badges += (
            f'<span style="display:inline-block;margin:3px 4px;padding:4px 12px;'
            f'border-radius:20px;background:{colour}22;border:1px solid {colour};'
            f'color:{colour};font-size:0.82rem;font-weight:700">'
            f'{label}&nbsp;&nbsp;{cnt}</span>'
        )
    st.markdown(badges, unsafe_allow_html=True)
    st.divider()

    # 2 ── individual comments
    st.markdown("##### Individual comments")
    rows = []
    for cat, entries in comments_by_type.items():
        for e in entries:
            rows.append({
                "Category":   cat.replace("_", " ").title(),
                "Username":   e[1] if len(e) > 1 else "",
                "Comment":    e[2] if len(e) > 2 else "",
                "Response":   e[3] if len(e) > 3 else "",
                "comment_ts": e[0] if len(e) > 0 else None,
                "Processed TS": e[4] if len(e) > 4 else "",
            })

    if rows:
        cdf = pd.DataFrame(rows)

        ts_valid = cdf[cdf["comment_ts"].notna()].copy()
        if not ts_valid.empty:
            ts_valid["datetime"] = pd.to_datetime(ts_valid["comment_ts"], unit="s", utc=True)
            ts_valid["bucket"]   = ts_valid["datetime"].dt.floor("10min")
            agg = (
                ts_valid.groupby(["bucket", "Category"])
                .size().reset_index(name="Comments")
                .sort_values("bucket").reset_index(drop=True)
            )
            colour_map = {k.replace("_", " ").title(): v for k, v in TYPE_BADGE_COLOURS.items()}
            fig = px.line(
                agg, x="bucket", y="Comments", color="Category",
                color_discrete_map=colour_map,
                labels={"bucket": "Time (10-min buckets)", "Comments": "# Comments"},
                title="Comment volume — 10-minute buckets", markers=True,
            )
            fig.update_traces(line=dict(width=2), marker=dict(size=6))
            fig.update_layout(
                height=300, margin=dict(t=36, b=80, l=0, r=0),
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                legend=dict(orientation="h", yanchor="top", y=-0.25, xanchor="center", x=0.5),
                yaxis=dict(rangemode="tozero"), font=dict(size=11),
            )
            fig.update_xaxes(tickformat="%H:%M\n%b %d", tickfont=dict(size=10))
            st.plotly_chart(fig, use_container_width=True)

        display_df = cdf.copy()
        display_df["Time"] = pd.to_datetime(
            display_df["comment_ts"], unit="s", utc=True, errors="coerce"
        ).dt.strftime("%Y-%m-%d %H:%M UTC")
        display_df = display_df.drop(columns=["comment_ts"])
        col_order  = ["Category", "Username", "Comment", "Response", "Time", "Processed TS"]
        display_df = display_df[[c for c in col_order if c in display_df.columns]]

        all_cats = sorted(display_df["Category"].unique().tolist())
        fa, fb   = st.columns([4, 1])
        selected_cats = fa.multiselect(
            "Filter by category", options=all_cats, default=all_cats,
            key="modal_cat_filter", placeholder="Select categories…",
        )
        fb.metric("Showing", f"{len(display_df[display_df['Category'].isin(selected_cats)])} / {len(display_df)}")
        filtered_df = display_df[display_df["Category"].isin(selected_cats)] if selected_cats else display_df
        st.dataframe(
            filtered_df, use_container_width=True, hide_index=True,
            height=min(320, 44 + 36 * len(filtered_df)),
            column_config={
                "Response": st.column_config.TextColumn(width="small"),
                "Time":     st.column_config.TextColumn(width="medium"),
                "Processed TS": st.column_config.NumberColumn(format="%d", width="small"),
            },
        )
    else:
        st.caption("No individual comments found.")

    if ai_enabled:
        st.divider()
        st.markdown("##### ⚙️ AI automation config")
        ai = _dec_to_native(ai_enabled)
        SECTION_ICONS = {"negative_comments": "🚫", "positive_comments": "✅",
                         "inquiries": "💬", "potential_buyers": "🛒"}
        cols = st.columns(len(ai))
        for col, (section_key, cfg) in zip(cols, ai.items()):
            icon  = SECTION_ICONS.get(section_key, "•")
            label = section_key.replace("_", " ").title()
            mode  = cfg.get("mode", "—")
            with col:
                st.markdown(
                    f'<div style="border:1px solid #dee2e6;border-radius:8px;padding:10px 12px;">'
                    f'<div style="font-size:0.75rem;color:#6c757d;margin-bottom:2px">{icon} {label}</div>'
                    f'<div style="font-weight:700;font-size:0.9rem">{mode}</div>',
                    unsafe_allow_html=True,
                )
                for k, v in cfg.items():
                    if k != "mode" and v not in ("", None, {}, []):
                        st.caption(f"{k}: `{v}`")
                st.markdown("</div>", unsafe_allow_html=True)

    if tag_and_value_pair:
        st.divider()
        st.markdown("##### 🏷️ Tag & value pairs")
        for i, pair in enumerate(_dec_to_native(tag_and_value_pair)):
            with st.expander(f"Rule {i+1} — tags: {pair.get('tags', []) or 'none'}", expanded=i == 0):
                l, r = st.columns(2)
                l.markdown(f"**Mode active:** `{pair.get('mode', '—')}`")
                l.markdown(f"**Require follow:** `{pair.get('requireFollow', '—')}`")
                l.markdown(f"**Send permalink:** `{pair.get('sendPermaLink', '—')}`")
                if pair.get("permaLink"):
                    l.markdown(f"**Permalink:** [{pair['permaLink']}]({pair['permaLink']})")
                tags = pair.get("tags") or []
                if tags:
                    r.markdown(" ".join(
                        f'<span style="display:inline-block;margin:2px;padding:2px 8px;border-radius:12px;'
                        f'background:#4361ee22;border:1px solid #4361ee;color:#4361ee;font-size:0.78rem">{t}</span>'
                        for t in tags), unsafe_allow_html=True)
                if pair.get("responseDM"):        r.markdown(f"**DM response:** {pair['responseDM']}")
                if pair.get("responseComment"):   r.markdown(f"**Comment response:** {pair['responseComment']}")
                if pair.get("followPromptMessage"): r.markdown(f"**Follow prompt:** {pair['followPromptMessage']}")

# ── account-level helpers ──────────────────────────────────────────────────────

def _render_account_ai_enabled(ai_enabled: dict) -> None:
    if not ai_enabled:
        return
    ai    = _dec_to_native(ai_enabled)
    ICONS = {"negative_comments": "🚫", "positive_comments": "✅",
             "inquiries": "💬", "potential_buyers": "🛒", "other_comments": "📝"}
    cols  = st.columns(len(ai))
    for col, (key, cfg) in zip(cols, ai.items()):
        label  = key.replace("_", " ").title()
        mode   = cfg.get("mode", "—")
        colour = "#2dc653" if "ai" in mode else "#f77f00" if "leave" in mode else "#6c757d"
        with col:
            st.markdown(
                f'<div style="border:1px solid {colour}55;border-radius:10px;padding:12px 14px;background:{colour}0d;">'
                f'<div style="font-size:0.72rem;color:#888;margin-bottom:4px">{ICONS.get(key,"•")} {label}</div>'
                f'<div style="font-weight:700;color:{colour};font-size:0.9rem">{mode}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
            for k, v in cfg.items():
                if k != "mode" and v not in ("", None, {}, []):
                    if k == "tags" and isinstance(v, list):
                        st.caption("tags: " + ", ".join(f"`{t}`" for t in v))
                    else:
                        st.caption(f"{k}: `{v}`")


def _render_account_tag_and_value(tag_and_value_pair: list) -> None:
    if not tag_and_value_pair:
        return
    for i, pair in enumerate(_dec_to_native(tag_and_value_pair)):
        tags  = pair.get("tags") or []
        with st.expander(f"Rule {i+1} — tags: {tags if tags else 'none'}", expanded=(i == 0)):
            l, r = st.columns(2)
            l.markdown(f"**Mode:** `{pair.get('mode', '—')}`")
            l.markdown(f"**Require follow:** `{pair.get('requireFollow', '—')}`")
            l.markdown(f"**Send permalink:** `{pair.get('sendPermaLink', '—')}`")
            if pair.get("permaLink"):
                l.markdown(f"**Link:** [{pair['permaLink']}]({pair['permaLink']})")
            if tags:
                r.markdown(" ".join(
                    f'<span style="display:inline-block;margin:2px;padding:2px 8px;border-radius:12px;'
                    f'background:#4361ee22;border:1px solid #4361ee;color:#4361ee;font-size:0.78rem">{t}</span>'
                    for t in tags), unsafe_allow_html=True)
            if pair.get("responseDM"):          r.markdown(f"**DM response:** {pair['responseDM']}")
            if pair.get("responseComment"):     r.markdown(f"**Comment response:** {pair['responseComment']}")
            if pair.get("followPromptMessage"): r.markdown(f"**Follow prompt:** {pair['followPromptMessage']}")


def _build_flat_comments(media_analytics: list) -> pd.DataFrame:
    rows = []
    for item in media_analytics:
        post_id          = str(item.get("id", ""))
        comments_by_type = _dec_to_native(item.get("comments_by_type", {}))
        for cat, entries in comments_by_type.items():
            for e in entries:
                rows.append({
                    "post_id":    post_id,
                    "Category":   cat.replace("_", " ").title(),
                    "Username":   e[1] if len(e) > 1 else "",
                    "Comment":    e[2] if len(e) > 2 else "",
                    "Response":   e[3] if len(e) > 3 else "",
                    "comment_ts": e[0] if len(e) > 0 else None,
                })
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _render_account_timeseries(media_analytics: list) -> None:
    flat = _build_flat_comments(media_analytics)
    if flat.empty or flat["comment_ts"].isna().all():
        st.caption("No timestamp data available for timeseries.")
        return
    ts_valid = flat[flat["comment_ts"].notna()].copy()
    ts_valid["datetime"] = pd.to_datetime(ts_valid["comment_ts"], unit="s", utc=True)
    ts_valid["bucket"]   = ts_valid["datetime"].dt.floor("30min")
    agg = (
        ts_valid.groupby(["bucket", "Category"])
        .size().reset_index(name="Comments")
        .sort_values("bucket").reset_index(drop=True)
    )
    colour_map = {k.replace("_", " ").title(): v for k, v in TYPE_BADGE_COLOURS.items()}
    fig = px.line(
        agg, x="bucket", y="Comments", color="Category",
        color_discrete_map=colour_map,
        labels={"bucket": "Time (30-min buckets)", "Comments": "# Comments"},
        title="Account-level comment volume — 30-minute buckets",
        markers=True,
    )
    fig.update_traces(line=dict(width=2), marker=dict(size=6))
    fig.update_layout(
        height=450, margin=dict(t=40, b=120, l=0, r=0),
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", yanchor="top", y=-0.3, xanchor="center", x=0.5),
        yaxis=dict(rangemode="tozero"), font=dict(size=11),
    )
    fig.update_xaxes(tickformat="%H:%M\n%b %d", tickfont=dict(size=10))
    st.plotly_chart(fig, use_container_width=True)


def _render_account_comments_table(media_analytics: list) -> None:
    flat = _build_flat_comments(media_analytics)
    if flat.empty:
        st.caption("No comment data.")
        return
    flat["Time"] = pd.to_datetime(
        flat["comment_ts"], unit="s", utc=True, errors="coerce"
    ).dt.strftime("%Y-%m-%d %H:%M UTC")
    flat = flat.drop(columns=["comment_ts"])
    col_order = ["post_id", "Category", "Username", "Comment", "Response", "Time"]
    flat = flat[[c for c in col_order if c in flat.columns]]

    all_cats = sorted(flat["Category"].unique().tolist())
    fa, fb   = st.columns([4, 1])
    selected = fa.multiselect(
        "Filter by category", options=all_cats, default=all_cats,
        key="acct_cat_filter", placeholder="Select categories…",
    )
    filtered = flat[flat["Category"].isin(selected)] if selected else flat
    fb.metric("Comments", f"{len(filtered)} / {len(flat)}")
    st.dataframe(
        filtered, use_container_width=True, hide_index=True,
        height=min(450, 44 + 36 * len(filtered)),
        column_config={
            "post_id":  st.column_config.TextColumn("Post ID", width="small"),
            "Response": st.column_config.TextColumn(width="small"),
            "Time":     st.column_config.TextColumn(width="medium"),
        },
    )

# ── main table ─────────────────────────────────────────────────────────────────

def plot_post_comment_table(
    df: pd.DataFrame,
    media_analytics: list,
    get_per_media_analytics,
    tag_and_value_pair: list = None,
    ai_enabled: dict = None,
) -> None:
    if df.empty:
        st.info("No post data to display.")
        return

    # 1 ── account config
    if ai_enabled or tag_and_value_pair:
        with st.expander("⚙️ Account config — AI automation & tag rules", expanded=True):
            if ai_enabled:
                st.markdown("**AI Automation**")
                _render_account_ai_enabled(ai_enabled)
            if tag_and_value_pair:
                st.divider()
                st.markdown("**Tag & Value Rules**")
                _render_account_tag_and_value(tag_and_value_pair)
        st.divider()

    # 2 ── per-post table
    st.markdown("#### 📋 Per-post breakdown")
    c1, c2, c3 = st.columns([2, 2, 1])
    sort_by   = c1.selectbox("Sort by", DISPLAY_COLS, index=0, key="pct_sort_col")
    sort_dir  = c2.radio("Direction", ["Descending", "Ascending"], horizontal=True, key="pct_sort_dir")
    min_total = c3.number_input("Min total", min_value=0, value=0, step=1, key="pct_min_total")

    view = (
        df[df["total"] >= min_total]
        .sort_values(sort_by, ascending=(sort_dir == "Ascending"))
        .reset_index(drop=True)
    )
    if view.empty:
        st.warning("No rows match the current filter.")
        return

    col_max      = {col: view[col].max() or 1 for col in DISPLAY_COLS}
    header_cells = (
        "<th>#</th><th>Post ID</th>"
        + "".join('<th style="min-width:140px">' + c.replace("_", " ").title() + "</th>" for c in DISPLAY_COLS)
    )
    rows_html = ""
    for rank, (_, row) in enumerate(view.iterrows(), 1):
        post_id   = str(row["id"])
        permalink = row.get("post_link") or "#"
        id_cell   = (
            '<span style="color:#4361ee;font-weight:600;font-family:monospace;font-size:0.82rem;">'
            + post_id + '</span>'
            + '&nbsp;<a href="' + permalink + '" target="_blank" '
            + 'style="color:#adb5bd;font-size:0.75rem;text-decoration:none;" title="Open post">&#8599;</a>'
        )
        metric_cells = "".join(
            "<td>" + _bar_html(int(row[c]), col_max[c], COL_COLOURS[c]) + "</td>"
            for c in DISPLAY_COLS
        )
        stripe      = "background:#f8f9fa" if rank % 2 == 0 else ""
        mouseout_bg = "#f8f9fa"            if rank % 2 == 0 else "transparent"
        tr_open = (
            '<tr style="{s};transition:background .15s" '
            'onmouseover="this.style.background=\'#eef2ff\'" '
            'onmouseout="this.style.background=\'{m}\'">'
        ).format(s=stripe, m=mouseout_bg)
        rows_html += (
            tr_open
            + '<td style="color:#adb5bd;text-align:center;font-size:0.8rem">' + str(rank) + "</td>"
            + "<td>" + id_cell + "</td>"
            + metric_cells + "</tr>"
        )

    table_html = """
<style>
  .pct-scroll-outer{overflow-x:auto;overflow-y:auto;max-height:400px;border-radius:10px;border:1px solid #dee2e6;}
  .pct-table{border-collapse:collapse;width:100%;font-size:0.88rem;font-family:'DM Sans',sans-serif;}
  .pct-table th{background:#1a1a2e;color:#e0e0ff;padding:10px 14px;text-align:left;font-weight:600;
    letter-spacing:.04em;font-size:0.8rem;position:sticky;top:0;z-index:2;border-bottom:2px solid #4361ee;}
  .pct-table td{padding:9px 14px;vertical-align:middle;border-bottom:1px solid #f0f0f0;white-space:nowrap;}
  .pct-table tr:last-child td{border-bottom:none;}
</style>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;600&display=swap" rel="stylesheet">
<div class="pct-scroll-outer">
  <table class="pct-table">
    <thead><tr>""" + header_cells + """</tr></thead>
    <tbody>""" + rows_html + """</tbody>
  </table>
</div>"""
    st.caption(f"{len(view)} posts · sorted by **{sort_by}** ({sort_dir.lower()})")
    components.html(table_html, height=450, scrolling=False)

    # 3 ── account timeseries
    st.divider()
    st.markdown("#### 📊 Comment volume over time")
    _render_account_timeseries(media_analytics)

    # 4 ── flat comments table
    st.divider()
    st.markdown("#### 🗂️ All comments by category")
    _render_account_comments_table(media_analytics)

    # 5 ── post inspect
    st.divider()
    left, right = st.columns([5, 1], vertical_alignment="bottom")
    options = [
        f"#{i+1} · …{str(row['id'])[-6:]}  (total: {row['total']})"
        for i, (_, row) in enumerate(view.iterrows())
    ]
    id_map = {label: str(row["id"]) for label, (_, row) in zip(options, view.iterrows())}
    selected_label = left.selectbox(
        "🔍 Inspect post", options, index=None,
        placeholder="Select a post to see full detail…", key="pct_inspect_select",
    )
    inspect_clicked = right.button(
        "Inspect ↗", key="pct_inspect_btn", type="primary",
        use_container_width=True, disabled=selected_label is None,
    )
    if inspect_clicked and selected_label:
        pid = id_map[selected_label]
        with st.spinner(f"Fetching detail for post {pid}…"):
            result = get_per_media_analytics(pid)
        if isinstance(result, tuple) and len(result) == 3:
            detail, tvp, ai = result
        elif isinstance(result, tuple) and len(result) == 2:
            detail, tvp, ai = result[0], result[1], None
        elif isinstance(result, dict):
            detail, tvp, ai = result, None, None
        else:
            detail, tvp, ai = {}, None, None
        if not detail:
            st.error(f"No analytics found for post {pid}")
        else:
            _show_modal(detail, tvp, ai)

# ── app UI ─────────────────────────────────────────────────────────────────────

st.title("📊 Repaly Analytics")
account_id = st.text_input("Enter Account ID", placeholder="e.g. 25398043726462840")

if account_id:
    with st.spinner("Loading data…"):
        media_analytics = get_items_by_sk(instagram_media_analytics_table, "accountId", account_id)
        media_details   = get_items_by_sk(instagram_media_table,           "accountId", account_id)
        account_details = get_item_by_pk(instagram_account_table, "id", account_id) or {}

    if not media_analytics:
        st.warning("No data found for this account ID.")
    else:
        category_total = get_category_totals(media_analytics)
        plot_category_data(category_total)

        df = get_post_comment_totals(media_analytics, media_details)
        plot_post_comment_table(
            df,
            media_analytics,
            get_per_media_analytics,
            account_details.get("tag_and_value_pair"),
            account_details.get("ai_enabled"),
        )
