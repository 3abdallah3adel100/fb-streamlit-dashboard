import re
import requests
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="Meta Ads Team Dashboard", layout="wide")

BASE_URL = "https://graph.facebook.com"
API_VERSION = st.secrets.get("META_API_VERSION", "v17.0")
ACCESS_TOKEN = st.secrets["META_ACCESS_TOKEN"]

BUSINESS_ID = "751488620224306"

MEDIA_BUYER_MAP = {
    "AA": "Abdallah Adel",
    "HM": "Ahmed Hesham",
    "BM": "Bassem Shalawy",
    "EK": "Esraa Kamal",
    "MA": "Mahmoud",
    "AF": "Amr Fathy",
    "SQ": "Ahmed Sharkawy",
    "OS": "Osama Fathy",
    "MM": "Mohamed Mahmoud",
    "NB": "Mohamed Nabih",
}

OBJECTIVE_ORDER = [
    "Lead generation",
    "Lead - Message",
    "Whatsapp Message",
    "Conversion",
    "Unknown",
]


def to_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def safe_div(a, b):
    try:
        if b and b != 0:
            return a / b
    except Exception:
        pass
    return None


def normalize_text(value):
    if value is None:
        return ""
    return str(value).strip().upper()


def extract_buyer_code(account_name: str) -> str:
    text = normalize_text(account_name)
    for code in MEDIA_BUYER_MAP.keys():
        pattern = rf"(?<![A-Z0-9]){re.escape(code)}(?![A-Z0-9])"
        if re.search(pattern, text):
            return code
    return "UNKNOWN"


def classify_objective_from_campaign_name(campaign_name: str) -> str:
    text = normalize_text(campaign_name)

    if re.search(r"(?<![A-Z0-9])CONVL(?![A-Z0-9])", text):
        return "Conversion"
    if re.search(r"(?<![A-Z0-9])CONVS(?![A-Z0-9])", text):
        return "Conversion"
    if re.search(r"(?<![A-Z0-9])CONV(?![A-Z0-9])", text):
        return "Conversion"

    if re.search(r"(?<![A-Z0-9])WA(?![A-Z0-9])", text):
        return "Whatsapp Message"
    if re.search(r"(?<![A-Z0-9])LG(?![A-Z0-9])", text):
        return "Lead generation"
    if re.search(r"(?<![A-Z0-9])LM(?![A-Z0-9])", text):
        return "Lead - Message"

    return "Unknown"


def flatten_actions(actions):
    result = {}
    if not isinstance(actions, list):
        return result

    for item in actions:
        action_type = str(item.get("action_type", "")).strip().lower()
        value = to_float(item.get("value", 0))
        if action_type:
            result[action_type] = result.get(action_type, 0.0) + value
    return result


def get_result_by_objective(objective_label, actions_map):
    # Lead generation + Lead - Message + Conversion
    if objective_label in {"Lead generation", "Lead - Message", "Conversion"}:
        keys = [
            "offsite_conversion.fb_pixel_lead",
            "onsite_conversion.lead_grouped",
            "offsite_conversion.custom",
        ]
        return sum(actions_map.get(k, 0.0) for k in keys)

    # Whatsapp
    if objective_label == "Whatsapp Message":
        keys = [
            "onsite_conversion.messaging_conversation_started",
            "onsite_conversion.messaging_conversation_started_7d",
        ]
        return sum(actions_map.get(k, 0.0) for k in keys)

    return 0.0


def format_display_df(df):
    out = df.copy()

    money_cols = ["spend", "cpl", "cpc"]
    pct_cols = ["ctr"]
    float_cols = ["frequency"]
    int_like_cols = ["results", "campaigns", "impressions", "clicks"]

    for col in money_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").round(2)

    for col in pct_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").round(2)

    for col in float_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").round(2)

    for col in int_like_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0).round(0)

    return out


@st.cache_data(ttl=1800)
def fetch_all_pages(url, params=None):
    all_rows = []

    while True:
        response = requests.get(url, params=params, timeout=120)
        response.raise_for_status()
        data = response.json()

        all_rows.extend(data.get("data", []))

        paging = data.get("paging", {})
        next_url = paging.get("next")
        if not next_url:
            break

        url = next_url
        params = None

    return all_rows


@st.cache_data(ttl=1800)
def get_ad_accounts():
    all_dfs = []

    try:
        url = f"{BASE_URL}/{API_VERSION}/me/adaccounts"
        params = {
            "fields": "id,account_id,name,account_status,currency",
            "access_token": ACCESS_TOKEN,
            "limit": 500,
        }
        rows = fetch_all_pages(url, params)
        df = pd.DataFrame(rows)
        if not df.empty:
            df["source"] = "me/adaccounts"
            all_dfs.append(df)
    except Exception:
        pass

    try:
        url = f"{BASE_URL}/{API_VERSION}/{BUSINESS_ID}/owned_ad_accounts"
        params = {
            "fields": "id,account_id,name,account_status,currency",
            "access_token": ACCESS_TOKEN,
            "limit": 500,
        }
        rows = fetch_all_pages(url, params)
        df = pd.DataFrame(rows)
        if not df.empty:
            df["source"] = "business/owned_ad_accounts"
            all_dfs.append(df)
    except Exception:
        pass

    try:
        url = f"{BASE_URL}/{API_VERSION}/{BUSINESS_ID}/client_ad_accounts"
        params = {
            "fields": "id,account_id,name,account_status,currency",
            "access_token": ACCESS_TOKEN,
            "limit": 500,
        }
        rows = fetch_all_pages(url, params)
        df = pd.DataFrame(rows)
        if not df.empty:
            df["source"] = "business/client_ad_accounts"
            all_dfs.append(df)
    except Exception:
        pass

    if not all_dfs:
        return pd.DataFrame(), pd.DataFrame()

    raw_accounts = pd.concat(all_dfs, ignore_index=True)

    dedup = raw_accounts.copy()
    if "id" in dedup.columns:
        dedup = dedup.sort_values(["name", "source"]).drop_duplicates(subset=["id"], keep="first")
    elif "account_id" in dedup.columns:
        dedup = dedup.sort_values(["name", "source"]).drop_duplicates(subset=["account_id"], keep="first")

    return dedup.reset_index(drop=True), raw_accounts.reset_index(drop=True)


@st.cache_data(ttl=1800)
def get_campaigns(account_id):
    clean_id = str(account_id).replace("act_", "")
    url = f"{BASE_URL}/{API_VERSION}/act_{clean_id}/campaigns"
    params = {
        "fields": "id,name,status,effective_status",
        "access_token": ACCESS_TOKEN,
        "limit": 1000,
    }
    rows = fetch_all_pages(url, params)
    df = pd.DataFrame(rows)
    if not df.empty:
        df["account_id"] = f"act_{clean_id}"
    return df


@st.cache_data(ttl=1800)
def get_insights_for_account(account_id, since, until):
    clean_id = str(account_id).replace("act_", "")
    url = f"{BASE_URL}/{API_VERSION}/act_{clean_id}/insights"
    params = {
        "fields": ",".join([
            "account_id",
            "account_name",
            "campaign_id",
            "campaign_name",
            "spend",
            "impressions",
            "clicks",
            "ctr",
            "cpc",
            "frequency",
            "actions",
            "date_start",
            "date_stop",
        ]),
        "level": "campaign",
        "time_range": f'{{"since":"{since}","until":"{until}"}}',
        "access_token": ACCESS_TOKEN,
        "limit": 1000,
    }

    rows = fetch_all_pages(url, params)
    df = pd.DataFrame(rows)

    if "actions" not in df.columns:
        df["actions"] = None

    return df


def prepare_data(all_campaigns_df, all_insights_df):
    if all_insights_df.empty:
        return pd.DataFrame()

    fact = all_insights_df.copy()

    if "actions" not in fact.columns:
        fact["actions"] = None

    for col in ["spend", "clicks", "impressions", "ctr", "cpc", "frequency"]:
        if col in fact.columns:
            fact[col] = pd.to_numeric(fact[col], errors="coerce").fillna(0)
        else:
            fact[col] = 0

    if not all_campaigns_df.empty:
        campaigns_map = all_campaigns_df[["id", "name", "account_id"]].rename(
            columns={"id": "campaign_id", "name": "campaign_name_master"}
        )
        fact = fact.merge(campaigns_map, on=["campaign_id", "account_id"], how="left")
    else:
        fact["campaign_name_master"] = None

    if "campaign_name" not in fact.columns:
        fact["campaign_name"] = fact["campaign_name_master"]
    else:
        fact["campaign_name"] = fact["campaign_name"].fillna(fact["campaign_name_master"])

    if "account_name" not in fact.columns:
        fact["account_name"] = "Unknown"

    fact["campaign_name"] = fact["campaign_name"].fillna("Unknown")
    fact["account_name"] = fact["account_name"].fillna("Unknown")

    fact["buyer_code"] = fact["account_name"].apply(extract_buyer_code)
    fact["media_buyer"] = fact["buyer_code"].map(MEDIA_BUYER_MAP).fillna("Unknown")

    fact["objective_label"] = fact["campaign_name"].apply(classify_objective_from_campaign_name)

    fact["actions_map"] = fact["actions"].apply(flatten_actions)

    fact["results"] = fact.apply(
        lambda r: get_result_by_objective(
            r["objective_label"],
            r["actions_map"]
        ),
        axis=1
    )

    fact["cpl"] = fact.apply(lambda r: safe_div(r["spend"], r["results"]), axis=1)

    return fact


def build_overall_summary(fact):
    return {
        "total_spend": fact["spend"].sum(),
        "total_results": fact["results"].sum(),
        "total_cpl": safe_div(fact["spend"].sum(), fact["results"].sum()),
    }


def build_objective_summary(fact):
    out = (
        fact.groupby("objective_label", dropna=False)
        .agg(
            spend=("spend", "sum"),
            results=("results", "sum"),
            campaigns=("campaign_id", "nunique"),
            impressions=("impressions", "sum"),
            clicks=("clicks", "sum"),
        )
        .reset_index()
    )

    out["cpl"] = out.apply(lambda r: safe_div(r["spend"], r["results"]), axis=1)
    out["ctr"] = out.apply(lambda r: safe_div(r["clicks"], r["impressions"]) * 100 if r["impressions"] > 0 else None, axis=1)
    out["cpc"] = out.apply(lambda r: safe_div(r["spend"], r["clicks"]), axis=1)

    out["objective_label"] = pd.Categorical(out["objective_label"], OBJECTIVE_ORDER)
    return out.sort_values(["objective_label", "spend"], ascending=[True, False]).reset_index(drop=True)


def build_buyer_summary(fact):
    out = (
        fact.groupby(["buyer_code", "media_buyer"], dropna=False)
        .agg(
            spend=("spend", "sum"),
            results=("results", "sum"),
            campaigns=("campaign_id", "nunique"),
            impressions=("impressions", "sum"),
            clicks=("clicks", "sum"),
        )
        .reset_index()
    )

    out["cpl"] = out.apply(lambda r: safe_div(r["spend"], r["results"]), axis=1)
    out["ctr"] = out.apply(lambda r: safe_div(r["clicks"], r["impressions"]) * 100 if r["impressions"] > 0 else None, axis=1)
    out["cpc"] = out.apply(lambda r: safe_div(r["spend"], r["clicks"]), axis=1)
    return out.sort_values("spend", ascending=False).reset_index(drop=True)


def build_buyer_objective_summary(fact):
    out = (
        fact.groupby(["media_buyer", "objective_label"], dropna=False)
        .agg(
            spend=("spend", "sum"),
            results=("results", "sum"),
            campaigns=("campaign_id", "nunique"),
            impressions=("impressions", "sum"),
            clicks=("clicks", "sum"),
        )
        .reset_index()
    )

    out["cpl"] = out.apply(lambda r: safe_div(r["spend"], r["results"]), axis=1)
    out["ctr"] = out.apply(lambda r: safe_div(r["clicks"], r["impressions"]) * 100 if r["impressions"] > 0 else None, axis=1)
    out["cpc"] = out.apply(lambda r: safe_div(r["spend"], r["clicks"]), axis=1)

    out["objective_label"] = pd.Categorical(out["objective_label"], OBJECTIVE_ORDER)
    return out.sort_values(["media_buyer", "objective_label", "spend"], ascending=[True, True, False]).reset_index(drop=True)


def build_campaign_summary(fact):
    if fact.empty:
        return pd.DataFrame()

    rows = []
    for keys, grp in fact.groupby(["media_buyer", "objective_label", "campaign_id", "campaign_name"], dropna=False):
        media_buyer, objective_label, campaign_id, campaign_name = keys
        spend = grp["spend"].sum()
        results = grp["results"].sum()
        impressions = grp["impressions"].sum()
        clicks = grp["clicks"].sum()

        rows.append({
            "media_buyer": media_buyer,
            "objective_label": objective_label,
            "campaign_id": campaign_id,
            "campaign_name": campaign_name,
            "spend": spend,
            "results": results,
            "cpl": safe_div(spend, results),
            "ctr": safe_div(clicks, impressions) * 100 if impressions > 0 else None,
            "cpc": safe_div(spend, clicks),
            "frequency": pd.to_numeric(grp["frequency"], errors="coerce").dropna().mean() if not grp.empty else None,
            "impressions": impressions,
            "clicks": clicks,
        })

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows).sort_values("spend", ascending=False).reset_index(drop=True)


def build_account_sources_table(raw_accounts):
    if raw_accounts.empty:
        return pd.DataFrame()

    out = (
        raw_accounts.groupby(["id", "account_id", "name"], dropna=False)
        .agg(
            sources=("source", lambda x: ", ".join(sorted(set(x))))
        )
        .reset_index()
        .sort_values("name")
        .reset_index(drop=True)
    )
    return out


def build_action_types_table(fact):
    action_types = set()

    for amap in fact.get("actions_map", pd.Series(dtype=object)).dropna():
        if isinstance(amap, dict):
            for k in amap.keys():
                if k:
                    action_types.add(k)

    return pd.DataFrame({"action_type": sorted(action_types)})


defaults = {
    "data_loaded": False,
    "fact": pd.DataFrame(),
    "overall": None,
    "objective_summary": pd.DataFrame(),
    "buyer_summary": pd.DataFrame(),
    "buyer_objective_summary": pd.DataFrame(),
    "campaign_summary": pd.DataFrame(),
    "accounts_dedup": pd.DataFrame(),
    "accounts_raw": pd.DataFrame(),
    "action_types_df": pd.DataFrame(),
    "filter_buyer": "All",
    "filter_objective": "All",
}

for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v


st.title("Meta Ads Team Dashboard")
st.caption("Overall > Objective > Media Buyer > Campaign")

with st.sidebar:
    st.header("Data Load")
    since = st.date_input("From")
    until = st.date_input("To")
    show_account_sources = st.checkbox("Show account sources", value=True)
    show_diagnostics = st.checkbox("Show diagnostics", value=False)
    fetch_clicked = st.button("Fetch Data", use_container_width=True)

if fetch_clicked:
    try:
        with st.status("Loading data...", expanded=True) as status:
            accounts_df, raw_accounts_df = get_ad_accounts()

            if accounts_df.empty:
                st.warning("No ad accounts found.")
                st.stop()

            all_campaigns = []
            all_insights = []

            total = len(accounts_df)
            progress = st.progress(0)

            for i, row in accounts_df.iterrows():
                account_id = row["id"]
                account_name = row.get("name", account_id)
                st.write(f"Processing: {account_name}")

                try:
                    campaigns_df = get_campaigns(account_id)
                    if not campaigns_df.empty:
                        all_campaigns.append(campaigns_df)

                    insights_df = get_insights_for_account(account_id, str(since), str(until))
                    if not insights_df.empty:
                        all_insights.append(insights_df)

                except Exception as e:
                    st.warning(f"Skipped account {account_name}: {e}")

                progress.progress((i + 1) / total)

            all_campaigns_df = pd.concat(all_campaigns, ignore_index=True) if all_campaigns else pd.DataFrame()
            all_insights_df = pd.concat(all_insights, ignore_index=True) if all_insights else pd.DataFrame()

            fact = prepare_data(all_campaigns_df, all_insights_df)

            if fact.empty:
                st.warning("No data returned for selected date range.")
                st.stop()

            st.session_state["fact"] = fact
            st.session_state["overall"] = build_overall_summary(fact)
            st.session_state["objective_summary"] = build_objective_summary(fact)
            st.session_state["buyer_summary"] = build_buyer_summary(fact)
            st.session_state["buyer_objective_summary"] = build_buyer_objective_summary(fact)
            st.session_state["campaign_summary"] = build_campaign_summary(fact)
            st.session_state["accounts_dedup"] = accounts_df
            st.session_state["accounts_raw"] = build_account_sources_table(raw_accounts_df)
            st.session_state["action_types_df"] = build_action_types_table(fact)
            st.session_state["data_loaded"] = True

            status.update(label="Loaded successfully", state="complete")

    except Exception as e:
        st.error(f"App error: {e}")
        st.stop()

if not st.session_state["data_loaded"]:
    st.info("Choose date range, then click Fetch Data.")
    st.stop()

fact = st.session_state["fact"]
overall = st.session_state["overall"]
objective_summary = st.session_state["objective_summary"]
buyer_summary = st.session_state["buyer_summary"]
buyer_objective_summary = st.session_state["buyer_objective_summary"]
campaign_summary = st.session_state["campaign_summary"]
accounts_dedup = st.session_state["accounts_dedup"]
accounts_raw = st.session_state["accounts_raw"]
action_types_df = st.session_state["action_types_df"]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Ad Accounts", f"{accounts_dedup['id'].nunique()}")
c2.metric("Total Spend", f"{overall['total_spend']:,.2f}")
c3.metric("Total Results", f"{overall['total_results']:,.0f}")
c4.metric("Overall CPL", "-" if overall["total_cpl"] is None else f"{overall['total_cpl']:,.2f}")

if show_account_sources:
    st.subheader("Loaded Ad Accounts")
    st.dataframe(
        accounts_raw[["name", "sources"]],
        use_container_width=True,
        hide_index=True
    )

st.divider()

st.subheader("Overall by Objective")
st.dataframe(
    format_display_df(objective_summary[[
        "objective_label", "spend", "results", "campaigns", "cpl", "ctr", "cpc"
    ]]),
    use_container_width=True,
    hide_index=True
)

fig_obj = px.bar(objective_summary, x="objective_label", y="spend", title="Spend by Objective")
st.plotly_chart(fig_obj, use_container_width=True)

st.divider()

st.subheader("Overall by Media Buyer")
st.dataframe(
    format_display_df(buyer_summary[[
        "media_buyer", "buyer_code", "spend", "results", "campaigns", "cpl", "ctr", "cpc"
    ]]),
    use_container_width=True,
    hide_index=True
)

st.divider()

st.subheader("Drill-down")

with st.form("drilldown_form"):
    col1, col2 = st.columns(2)

    buyers_list = ["All"] + sorted([x for x in fact["media_buyer"].dropna().unique().tolist()])
    selected_buyer = col1.selectbox(
        "Select Media Buyer",
        buyers_list,
        index=buyers_list.index(st.session_state["filter_buyer"]) if st.session_state["filter_buyer"] in buyers_list else 0
    )

    temp_fact = fact.copy()
    if selected_buyer != "All":
        temp_fact = temp_fact[temp_fact["media_buyer"] == selected_buyer]

    objectives_list = ["All"] + sorted([x for x in temp_fact["objective_label"].dropna().unique().tolist()])
    selected_objective = col2.selectbox(
        "Select Objective",
        objectives_list,
        index=objectives_list.index(st.session_state["filter_objective"]) if st.session_state["filter_objective"] in objectives_list else 0
    )

    apply_filters = st.form_submit_button("Apply Filters")

if apply_filters:
    st.session_state["filter_buyer"] = selected_buyer
    st.session_state["filter_objective"] = selected_objective

filtered_fact = fact.copy()
if st.session_state["filter_buyer"] != "All":
    filtered_fact = filtered_fact[filtered_fact["media_buyer"] == st.session_state["filter_buyer"]]
if st.session_state["filter_objective"] != "All":
    filtered_fact = filtered_fact[filtered_fact["objective_label"] == st.session_state["filter_objective"]]

if st.session_state["filter_buyer"] != "All":
    st.markdown(f"### {st.session_state['filter_buyer']} - Objectives")
    buyer_obj_df = buyer_objective_summary[
        buyer_objective_summary["media_buyer"] == st.session_state["filter_buyer"]
    ].copy()

    st.dataframe(
        format_display_df(buyer_obj_df[[
            "media_buyer", "objective_label", "spend", "results", "campaigns", "cpl", "ctr", "cpc"
        ]]),
        use_container_width=True,
        hide_index=True
    )

st.markdown("### Campaigns")
campaign_drill = build_campaign_summary(filtered_fact)

if campaign_drill.empty:
    st.info("No campaigns found for this filter.")
else:
    st.dataframe(
        format_display_df(campaign_drill[[
            "media_buyer", "objective_label", "campaign_name", "spend",
            "results", "cpl", "ctr", "cpc", "frequency", "impressions", "clicks"
        ]]),
        use_container_width=True,
        hide_index=True
    )

if show_diagnostics:
    with st.expander("Detected Action Types"):
        st.dataframe(action_types_df, use_container_width=True, hide_index=True)

    with st.expander("Sample Campaign Results"):
        sample_df = fact[[
            "account_name", "campaign_name", "media_buyer",
            "objective_label", "spend", "results"
        ]].copy()
        st.dataframe(format_display_df(sample_df), use_container_width=True, hide_index=True)