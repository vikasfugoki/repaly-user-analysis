import streamlit as st
import pandas as pd
from boto3.dynamodb.conditions import Attr, Key
from decimal import Decimal
import streamlit.components.v1 as components

FLOW_TABLE                   = "flow_repository"
FLOW_ACCOUNT_ID_INDEX        = "accountId-index"
FLOW_ANALYTICS_TABLE         = "flow_node_analytics_repository"
FLOW_ANALYTICS_FLOW_ID_INDEX = "flow_id-index"


def _dec_to_native(obj):
    if isinstance(obj, Decimal):
        v = float(obj)
        return int(v) if v == int(v) else v
    if isinstance(obj, dict):
        return {k: _dec_to_native(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_dec_to_native(i) for i in obj]
    return obj


def get_flows_for_account(account_id, dynamodb):
    table, items = dynamodb.Table(FLOW_TABLE), []
    kwargs = {"IndexName": FLOW_ACCOUNT_ID_INDEX,
              "KeyConditionExpression": Key("accountId").eq(account_id)}
    while True:
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        lk = resp.get("LastEvaluatedKey")
        if not lk: break
        kwargs["ExclusiveStartKey"] = lk
    return [_dec_to_native(i) for i in items]


def get_node_analytics_for_flow(flow_id, dynamodb):
    table, items = dynamodb.Table(FLOW_ANALYTICS_TABLE), []
    kwargs = {"IndexName": FLOW_ANALYTICS_FLOW_ID_INDEX,
              "KeyConditionExpression": Key("flow_id").eq(flow_id)}
    while True:
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        lk = resp.get("LastEvaluatedKey")
        if not lk: break
        kwargs["ExclusiveStartKey"] = lk
    return [_dec_to_native(i) for i in items]


def _bare(raw_nid, flow_id):
    s = "_" + flow_id
    return raw_nid[:-len(s)] if raw_nid.endswith(s) else raw_nid


def _short_label(node):
    data, ntype = node.get("data", {}), node.get("type", "")
    blocks = data.get("blocks", [])
    if blocks:
        t = blocks[0].get("text", "").strip().replace("\n", " ")
        if t:
            return t[:42] + ("…" if len(t) > 42 else "")
    if ntype == "triggerNode":
        for key in ("user_dm", "post_dm", "story_dm", "media_share_dm"):
            conds = data.get(key, {}).get("conditions", [])
            if conds and conds[0].get("keywords"):
                kws = conds[0]["keywords"]
                src = key.replace("_dm","").replace("_"," ").title()
                return src + ": " + ", ".join(kws[:3])
        return data.get("label", "Trigger")
    if ntype == "conditionNode":
        rules = data.get("rules", [])
        if rules:
            r = rules[0]
            return "Condition: " + (r.get("label") or r.get("intent",""))[:35]
        return "Condition"
    if ntype == "actionNode":
        action = data.get("actionType","")
        tags   = data.get("tagName", [])
        return "Action: " + action.replace("_"," ").title() + (f" [{', '.join(tags)}]" if tags else "")
    return data.get("label", ntype or node.get("id","?")[:8])


def _colour(ntype):
    return {
        "triggerNode":   ("#4361ee","#2d4bc4","#fff"),
        "sendDMNode":    ("#0096c7","#007aa3","#fff"),
        "conditionNode": ("#f77f00","#c46500","#fff"),
        "actionNode":    ("#2dc653","#1fa03e","#fff"),
    }.get(ntype, ("#6c757d","#495057","#fff"))


def _type_label(ntype):
    return {"triggerNode":"Trigger","sendDMNode":"Send DM",
            "conditionNode":"Condition","actionNode":"Action"}.get(ntype, ntype)


def _safe(s):
    return str(s).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")


def _render_flow_diagram(flow, analytics_map):
    nodes = flow.get("flow",{}).get("nodes",[])
    edges = flow.get("flow",{}).get("edges",[])
    if not nodes:
        st.caption("No nodes in this flow.")
        return

    children = {n["id"]: [] for n in nodes}
    parents  = {n["id"]: [] for n in nodes}
    for e in edges:
        s, t = e.get("source"), e.get("target")
        if s in children and t in children:
            if t not in children[s]:
                children[s].append(t)
            parents[t].append(s)

    roots   = [n["id"] for n in nodes if not parents[n["id"]]]
    level   = {r: 0 for r in roots}
    queue, visited = list(roots), set(roots)
    while queue:
        cur = queue.pop(0)
        for child in children[cur]:
            if child not in visited:
                level[child] = level[cur] + 1
                visited.add(child)
                queue.append(child)
    for n in nodes:
        if n["id"] not in level:
            level[n["id"]] = 0

    from collections import defaultdict
    lvls = defaultdict(list)
    for n in nodes:
        lvls[level[n["id"]]].append(n)
    max_lvl = max(lvls.keys()) if lvls else 0

    def inline_stats(nid):
        a = analytics_map.get(nid, {})
        if not a: return ""
        parts = []
        if a.get("trigger_count") is not None:
            parts.append(f"Triggers: <b>{int(a['trigger_count']):,}</b>")
        if a.get("sent") is not None:
            parts.append(f"Sent: <b>{int(a['sent']):,}</b>")
        if a.get("read") is not None:
            parts.append(f"Read: <b>{int(a['read']):,}</b>")
        bc = a.get("button_counts", {})
        if bc:
            parts.append(f"Clicks: <b>{sum(int(v) for v in bc.values()):,}</b>")
        cc = a.get("condition_counts", {})
        if cc:
            parts.append(f"Pass: <b>{sum(int(v) for v in cc.values()):,}</b>")
        return " · ".join(parts)

    html = """<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;600;700&display=swap" rel="stylesheet">
<style>
.fw{display:flex;align-items:flex-start;gap:0;overflow-x:auto;padding:20px 12px;
    font-family:'DM Sans',sans-serif;background:#f0f2ff;border-radius:12px;}
.fc{display:flex;flex-direction:column;gap:10px;min-width:210px;max-width:230px;}
.farr{display:flex;align-items:center;justify-content:center;width:40px;flex-shrink:0;
      color:#4361ee;font-size:1.4rem;align-self:center;font-weight:700;}
.fn{border-radius:10px;padding:11px 13px;font-size:0.76rem;line-height:1.4;
    box-shadow:0 2px 8px rgba(0,0,0,0.13);}
.fn-type{font-size:0.59rem;font-weight:700;letter-spacing:.07em;text-transform:uppercase;
         opacity:0.78;margin-bottom:3px;}
.fn-label{font-weight:600;font-size:0.78rem;margin-bottom:6px;line-height:1.3;
          overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.fn-stats{font-size:0.69rem;opacity:0.88;line-height:1.7;}
.fn-nostats{font-size:0.67rem;opacity:0.42;font-style:italic;}
</style>
<div class="fw">"""

    for lvl in range(max_lvl + 1):
        if lvl > 0:
            html += '<div class="farr">→</div>'
        html += '<div class="fc">'
        for node in lvls[lvl]:
            nid = node["id"]
            bg, border, fg = _colour(node.get("type",""))
            label = _safe(_short_label(node))
            tl    = _type_label(node.get("type",""))
            stats = inline_stats(nid)
            s_html = (f'<div class="fn-stats">{stats}</div>'
                      if stats else '<div class="fn-nostats">no analytics</div>')
            html += (f'<div class="fn" style="background:{bg};border:2px solid {border};color:{fg};">'
                     f'<div class="fn-type">{tl}</div>'
                     f'<div class="fn-label">{label}</div>'
                     f'{s_html}</div>')
        html += '</div>'
    html += '</div>'

    max_col = max(len(v) for v in lvls.values()) if lvls else 1
    components.html(html, height=max(280, max_col * 115 + 60), scrolling=True)


def _stat_bar(label, value, max_val):
    pct = (value / max_val * 100) if max_val else 0
    return (f'<div style="margin-bottom:9px;">'
            f'<div style="display:flex;justify-content:space-between;font-size:0.72rem;'
            f'color:rgba(255,255,255,0.72);margin-bottom:3px;">'
            f'<span>{label}</span>'
            f'<span style="font-weight:700;color:#fff">{value:,}</span></div>'
            f'<div style="background:rgba(255,255,255,0.18);border-radius:4px;height:7px;">'
            f'<div style="width:{pct:.1f}%;background:rgba(255,255,255,0.85);'
            f'border-radius:4px;height:7px;"></div></div></div>')


def _sub_bar_section(title, items_dict, lookup):
    """Render a titled sub-section of named bars (buttons or condition rules)."""
    if not items_dict:
        return ""
    max_val = max(int(v) for v in items_dict.values()) or 1
    rows = ""
    for kid, count in sorted(items_dict.items(), key=lambda x: -int(x[1])):
        name = _safe(lookup.get(kid, kid[:14]))
        pct  = int(count) / max_val * 100
        rows += (f'<div style="margin-bottom:8px;">'
                 f'<div style="display:flex;justify-content:space-between;font-size:0.71rem;'
                 f'color:rgba(255,255,255,0.75);margin-bottom:3px;">'
                 f'<span>{name}</span>'
                 f'<span style="font-weight:700;color:#fff">{int(count):,}</span></div>'
                 f'<div style="background:rgba(255,255,255,0.18);border-radius:4px;height:6px;">'
                 f'<div style="width:{pct:.1f}%;background:rgba(255,255,255,0.65);'
                 f'border-radius:4px;height:6px;"></div></div></div>')
    return (f'<div style="border-top:1px solid rgba(255,255,255,0.18);'
            f'margin-top:11px;padding-top:11px;">'
            f'<div style="font-size:0.62rem;font-weight:700;letter-spacing:.06em;'
            f'text-transform:uppercase;opacity:0.7;margin-bottom:9px;">{title}</div>'
            f'{rows}</div>')


def _render_node_analytics_cards(node_analytics, nodes, flow_id):
    if not node_analytics:
        st.caption("No analytics data available for this flow.")
        return

    node_lookup = {n["id"]: n for n in nodes}

    btn_lookup  = {}
    rule_lookup = {}
    for node in nodes:
        data = node.get("data", {})
        for block in data.get("blocks", []):
            for btn in block.get("buttons", []):
                bid = btn.get("id")
                if bid:
                    btn_lookup[bid] = btn.get("title", bid[:10])
        for rule in data.get("rules", []):
            rid = rule.get("id")
            if rid:
                rule_lookup[rid] = rule.get("label") or rule.get("intent", rid[:12])

    TYPE_ORDER = {"triggerNode":0,"sendDMNode":1,"conditionNode":2,"actionNode":3}
    sorted_a = sorted(
        node_analytics,
        key=lambda a: TYPE_ORDER.get(
            node_lookup.get(_bare(a.get("node_id",""), flow_id), {}).get("type",""), 9)
    )

    all_vals = {}
    for a in sorted_a:
        for k, v in a.items():
            if isinstance(v, (int, float)) and k not in ("created_at","updated_at"):
                all_vals.setdefault(k,[]).append(int(v))
    gmax = {k: max(vs) or 1 for k, vs in all_vals.items()}

    cols = st.columns(2)
    for idx, a in enumerate(sorted_a):
        nid   = _bare(a.get("node_id",""), flow_id)
        node  = node_lookup.get(nid, {})
        ntype = node.get("type","")
        label = _safe(_short_label(node) if node else nid[:30])
        bg, border, _ = _colour(ntype)
        tl    = _type_label(ntype)
        updated = (pd.to_datetime(a.get("updated_at"), unit="s", utc=True)
                   .strftime("%b %d %H:%M UTC") if a.get("updated_at") else "—")

        bars_html  = ""
        extra_html = ""

        if ntype == "triggerNode":
            for key, display in [
                ("trigger_count","Total Triggers"),
                ("user_dm","User DM"),
                ("post_dm","Post Comment"),
                ("media_share_dm","Media Share"),
                ("story_dm","Story"),
            ]:
                val = a.get(key)
                if val is not None:
                    bars_html += _stat_bar(display, int(val), gmax.get(key,1))

        elif ntype == "sendDMNode":
            for key, display in [
                ("trigger_count","Triggered"),
                ("sent","Sent"),
                ("delivered","Delivered"),
                ("read","Read"),
            ]:
                val = a.get(key)
                if val is not None:
                    bars_html += _stat_bar(display, int(val), gmax.get(key,1))
            bc = a.get("button_counts", {})
            if bc:
                extra_html += _sub_bar_section("Button Clicks", bc, btn_lookup)

        elif ntype == "conditionNode":
            for key, display in [
                ("trigger_count","Total Evaluated"),
                ("condition_pass","Condition Pass"),
                ("condition_fail","Condition Fail"),
            ]:
                val = a.get(key)
                if val is not None:
                    bars_html += _stat_bar(display, int(val), gmax.get(key,1))
            cc = a.get("condition_counts", {})
            if cc:
                extra_html += _sub_bar_section("Rule Results", cc, rule_lookup)

        else:
            for k, v in a.items():
                if isinstance(v, (int, float)) and k not in ("created_at","updated_at"):
                    bars_html += _stat_bar(k.replace("_"," ").title(), int(v), gmax.get(k,1))

        if not bars_html and not extra_html:
            bars_html = '<div style="font-size:0.73rem;opacity:0.5;font-style:italic;padding:4px 0;">No metrics</div>'

        # estimate height
        bar_count   = bars_html.count("margin-bottom:9px")
        sub_rows    = extra_html.count("margin-bottom:8px")
        sub_headers = extra_html.count("border-top:")
        card_height = 115 + bar_count*36 + sub_rows*32 + sub_headers*44

        card = (
            '<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;600;700&display=swap" rel="stylesheet">'
            f'<div style="background:{bg};border:2px solid {border};border-radius:14px;'
            f'padding:16px 18px;font-family:DM Sans,sans-serif;color:#fff;">'
            f'<div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:12px;">'
            f'<div style="flex:1;min-width:0;">'
            f'<div style="font-size:0.6rem;font-weight:700;letter-spacing:.08em;'
            f'text-transform:uppercase;opacity:0.72;margin-bottom:3px;">{tl}</div>'
            f'<div style="font-size:0.88rem;font-weight:700;line-height:1.3;'
            f'overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{label}</div>'
            f'</div>'
            f'<div style="font-size:0.63rem;opacity:0.6;text-align:right;white-space:nowrap;'
            f'padding-top:2px;margin-left:10px;">Updated<br>{updated}</div>'
            f'</div>'
            f'<div style="border-top:1px solid rgba(255,255,255,0.18);padding-top:11px;">'
            f'{bars_html}</div>'
            f'{extra_html}</div>'
        )
        with cols[idx % 2]:
            components.html(card, height=card_height, scrolling=False)


def render_flow_analytics(account_id, dynamodb):
    with st.spinner("Loading flows…"):
        flows = get_flows_for_account(account_id, dynamodb)
    if not flows:
        st.info("No flows found for this account.")
        return

    flow_options = {
        f"{f.get('flow',{}).get('name','Unnamed')}  "
        f"({'● active' if f.get('isActiveAutomation') else '○ inactive'})  "
        f"· {f.get('id','')[:8]}…": f
        for f in sorted(flows, key=lambda x: x.get("flow",{}).get("updatedAt",""), reverse=True)
    }
    sel   = st.selectbox("Select a flow", list(flow_options.keys()), key="flow_select")
    flow  = flow_options[sel]
    fid   = flow.get("id","")
    fmeta = flow.get("flow",{})

    name      = fmeta.get("name","—")
    is_active = flow.get("isActiveAutomation", False)
    updated   = fmeta.get("updatedAt","")[:10] if fmeta.get("updatedAt") else "—"
    n_nodes   = len(fmeta.get("nodes",[]))
    n_edges   = len(fmeta.get("edges",[]))
    ac        = "#2dc653" if is_active else "#6c757d"
    st.markdown(
        f'<div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center;'
        f'margin-bottom:4px;font-family:sans-serif;">'
        f'<span style="font-size:1.05rem;font-weight:700;color:#1a1a2e">{_safe(name)}</span>'
        f'<span style="background:{ac}18;color:{ac};border:1px solid {ac}55;'
        f'border-radius:10px;padding:1px 10px;font-size:0.75rem;font-weight:600">'
        f'{"● Active" if is_active else "○ Inactive"}</span>'
        f'<span style="font-size:0.78rem;color:#6c757d">Updated: {updated}</span>'
        f'<span style="font-size:0.78rem;color:#6c757d">{n_nodes} nodes · {n_edges} edges</span>'
        f'</div>', unsafe_allow_html=True,
    )
    st.divider()

    with st.spinner("Loading node analytics…"):
        node_analytics = get_node_analytics_for_flow(fid, dynamodb)

    analytics_map = {_bare(a.get("node_id",""), fid): a for a in node_analytics}
    nodes = fmeta.get("nodes", [])

    st.markdown("#### 🔀 Flow")
    st.caption("Nodes laid out left → right by level. Inline stats where available.")
    _render_flow_diagram(flow, analytics_map)

    st.divider()
    st.markdown("#### 📊 Node Analytics")
    _render_node_analytics_cards(node_analytics, nodes, fid)