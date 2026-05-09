from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

try:
    import happybase
except ImportError:
    happybase = None


def apply_professional_style() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background: radial-gradient(1200px 500px at 10% -10%, #1b2a4a 0%, #0b1220 45%);
            color: #e2e8f0;
        }
        .main > div {
            padding-top: 0.85rem;
        }
        .top-title {
            margin: 0 0 0.25rem 0;
            font-size: 1.5rem;
            font-weight: 700;
            color: #f8fafc;
            letter-spacing: 0.2px;
        }
        .top-caption {
            margin: 0 0 0.85rem 0;
            color: #c7d2fe;
            font-size: 0.93rem;
        }
        .pill {
            display: inline-block;
            font-size: 0.76rem;
            color: #bfdbfe;
            background: rgba(15, 23, 42, 0.55);
            border: 1px solid #35507a;
            border-radius: 999px;
            padding: 0.22rem 0.62rem;
            margin-right: 0.35rem;
            margin-bottom: 0.45rem;
        }
        .kpi-card {
            background: linear-gradient(180deg, #131f36 0%, #0f172a 100%);
            border: 1px solid #2a3b58;
            border-radius: 16px;
            padding: 0.9rem 1rem;
            min-height: 92px;
            box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.03);
        }
        .kpi-label {
            color: #9fb4d3;
            font-size: 0.8rem;
            margin: 0;
        }
        .kpi-value {
            color: #f8fafc;
            font-size: 1.95rem;
            font-weight: 700;
            margin: 0.1rem 0 0 0;
            line-height: 1.05;
        }
        .section-title {
            margin-top: 0.7rem;
            margin-bottom: 0.72rem;
            font-weight: 600;
            color: #f8fafc;
        }
        .chart-shell {
            background: linear-gradient(180deg, #0f1b33 0%, #0c1628 100%);
            border: 1px solid #2a3b58;
            border-radius: 18px;
            padding: 0.72rem 0.8rem 0.32rem 0.8rem;
            margin-bottom: 0.95rem;
            box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.02);
            min-height: 72px;
        }
        .chart-shell [data-testid="stAltairChart"] {
            background: transparent !important;
        }
        .block-gap {
            height: 0.62rem;
        }
        .stMarkdown, .stCaption, label, .stSlider, .stSelectbox, .stToggle {
            color: #dbeafe !important;
        }
        [data-testid="stDataFrame"] {
            background: #0f172a;
            border: 1px solid #2a3b58;
            border-radius: 14px;
            padding: 0.25rem;
            box-shadow: 0 8px 20px rgba(2, 6, 23, 0.22);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def load_rows(limit: int) -> pd.DataFrame:
    host = os.environ.get("HBASE_THRIFT_HOST", "").strip()
    port = int(os.environ.get("HBASE_THRIFT_PORT", "9090"))
    table_name = os.environ.get("HBASE_TABLE", "crypto_stream_metrics")
    # Thrift can take 10–30s to accept connections right after `docker compose up`.
    timeout_ms = int(os.environ.get("HBASE_THRIFT_TIMEOUT_MS", "30000"))
    retries = int(os.environ.get("HBASE_THRIFT_RETRIES", "2"))
    retry_sleep_seconds = float(os.environ.get("HBASE_THRIFT_RETRY_SLEEP_SECONDS", "1.5"))
    host_candidates = []

    # Support multiple fallback hosts via env (e.g., "hbase,127.0.0.1").
    extra_hosts = os.environ.get("HBASE_THRIFT_HOSTS", "")
    if host:
        host_candidates.append(host)
    if extra_hosts:
        host_candidates.extend([h.strip() for h in extra_hosts.split(",") if h.strip()])

    if not host_candidates:
        # In containerized runs, localhost points to the app container itself.
        in_container = Path("/.dockerenv").exists()
        host_candidates = ["hbase", "127.0.0.1"] if in_container else ["127.0.0.1", "localhost"]
    else:
        # Keep deterministic order while removing duplicates.
        host_candidates = list(dict.fromkeys(host_candidates))

    if happybase is None:
        st.error("Install dependencies: pip install -r requirements.txt")
        return pd.DataFrame()

    last_err = None
    for candidate in host_candidates:
        attempt = 0
        while attempt <= retries:
            conn = None
            try:
                conn = happybase.Connection(host=candidate, port=port, timeout=timeout_ms)
                table = conn.table(table_name)
                rows = []
                for key, data in table.scan(limit=limit, reverse=True):
                    row = {"row_key": key.decode()}
                    for col, val in data.items():
                        q = col.decode().split(":", 1)[-1]
                        row[q] = val.decode()
                    rows.append(row)
                st.session_state["hbase_endpoint"] = f"{candidate}:{port}"
                return pd.DataFrame(rows)
            except Exception as e:  # noqa: BLE001
                last_err = e
                attempt += 1
                if attempt <= retries:
                    time.sleep(retry_sleep_seconds)
            finally:
                if conn is not None:
                    conn.close()

    st.session_state["hbase_endpoint"] = "unreachable"
    raise RuntimeError(
        f"Unable to connect to HBase Thrift endpoints {host_candidates} on port {port}. Last error: {last_err}"
    ) from last_err


def render_island_line_chart(df_long: pd.DataFrame, y_col: str, title: str) -> None:
    st.markdown(f"<p class='section-title'>{title}</p>", unsafe_allow_html=True)
    if df_long.empty:
        st.markdown("<div class='chart-shell'>", unsafe_allow_html=True)
        st.info("No Data in the Selected Time Window.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    chart = (
        alt.Chart(df_long)
        .mark_line(strokeWidth=2.4)
        .encode(
            x=alt.X("event_time:T", title="Time"),
            y=alt.Y(f"{y_col}:Q", title=None),
            color=alt.Color("symbol:N", title="Symbol"),
            tooltip=["event_time:T", "symbol:N", alt.Tooltip(f"{y_col}:Q", format=".4f")],
        )
        .properties(
            height=300,
            padding={"left": 18, "right": 18, "top": 18, "bottom": 14},
        )
        .configure(background="transparent")
        .configure_view(
            fill="#111a2b",
            stroke="#2b3b52",
            strokeWidth=1.2,
            cornerRadius=22,
        )
        .configure_axis(
            labelColor="#cbd5e1",
            titleColor="#cbd5e1",
            gridColor="#243247",
            domainColor="#334155",
            tickColor="#334155",
        )
        .configure_legend(
            titleColor="#cbd5e1",
            labelColor="#cbd5e1",
            orient="top",
        )
        .configure_title(color="#f8fafc", fontSize=15, anchor="start")
    )
    with st.container(border=True):
        st.altair_chart(chart, use_container_width=True)


def main() -> None:
    st.set_page_config(page_title="CS523 Crypto Stream", layout="wide")
    apply_professional_style()
    window_duration = os.environ.get("WINDOW_DURATION", "30 seconds")
    window_slide = os.environ.get("WINDOW_SLIDE", "10 seconds")
    watermark_delay = os.environ.get("WATERMARK_DELAY", "2 minutes")
    anomaly_threshold_pct = os.environ.get("ANOMALY_THRESHOLD_PCT", "0.3")

    st.markdown("<p class='top-title'>CS523 Crypto Stream Dashboard</p>", unsafe_allow_html=True)
    st.markdown(
        "<p class='top-caption'>Data path: Binance WebSocket → Kafka → Spark Structured Streaming → HBase. "
        "This view polls HBase via Thrift.</p>",
        unsafe_allow_html=True,
    )
    st.markdown(
        "<span class='pill'>Realtime Analytics</span>"
        "<span class='pill'>Kafka → Spark → HBase</span>",
        unsafe_allow_html=True,
    )

    left_col, right_col = st.columns([1.1, 3.2], gap="large")

    with left_col:
        with st.container(border=True):
            st.markdown("**Dashboard Configuration**")
            st.caption("Tune realtime refresh and chart window for smoother monitoring.")

            with st.container(border=True):
                st.caption("Live Mode")
                poll = st.toggle("Auto-refresh", value=True)

            with st.container(border=True):
                st.caption("Data Load")
                limit = st.slider("Rows to load", min_value=50, max_value=5000, value=500, step=50)

            with st.container(border=True):
                st.caption("Chart Horizon")
                n_seconds = st.slider("Chart window (seconds)", min_value=10, max_value=600, value=120, step=10)

            with st.container(border=True):
                st.caption("Refresh Rate")
                refresh_seconds = st.slider("Refresh interval (seconds)", min_value=1, max_value=30, value=5, step=1)

            with st.container(border=True):
                st.caption("Spark Analytics")
                st.write(f"Window: `{window_duration}`")
                st.write(f"Slide: `{window_slide}`")
                st.write(f"Watermark: `{watermark_delay}`")
                st.write(f"Anomaly threshold: `{anomaly_threshold_pct}%`")
            endpoint = st.session_state.get("hbase_endpoint", "unknown")
            st.caption(f"HBase endpoint: `{endpoint}`")

    try:
        df = load_rows(limit)
    except Exception as e:
        st.warning(
            f"Could not read HBase ({e}). "
            "Confirm: `docker compose ps` shows **hbase** running; Thrift listens on **9090** "
            f"(host `{os.environ.get('HBASE_THRIFT_HOST', 'auto')}`). "
            "If the container just started, wait 30s and refresh. "
            "Optional: set `HBASE_THRIFT_TIMEOUT_MS` (default 30000) or `HBASE_THRIFT_HOST` "
            "if Streamlit is not on the same machine as Docker. "
            "You can also set `HBASE_THRIFT_HOSTS` with comma-separated fallbacks "
            "(for example: `hbase,127.0.0.1`). "
            "Table `crypto_stream_metrics`, column family `m`."
        )
        return

    if df.empty:
        st.info("No rows yet. Run the producer and Spark job, then enable polling or refresh.")
        return

    if "window_end_ms" in df.columns:
        df["window_end_ms"] = pd.to_numeric(df["window_end_ms"], errors="coerce")
        df["event_time"] = pd.to_datetime(df["window_end_ms"], unit="ms", utc=True)
    else:
        df["event_time"] = pd.NaT
    if "avg_price" in df.columns:
        df["avg_price"] = pd.to_numeric(df["avg_price"], errors="coerce")
    if "symbol" not in df.columns:
        df["symbol"] = "UNKNOWN"

    df_chart = (
        df.dropna(subset=["event_time", "avg_price"])
        .sort_values("event_time")
        .copy()
    )

    latest_event = df_chart["event_time"].max() if not df_chart.empty else pd.Timestamp.now(tz="UTC")
    now_utc = pd.Timestamp.now(tz="UTC")
    age_seconds = int((now_utc - latest_event).total_seconds()) if pd.notna(latest_event) else -1
    total_symbols = int(df["symbol"].nunique()) if "symbol" in df.columns else 0

    with right_col:
        kpi1, kpi2, kpi3 = st.columns(3)
        with kpi1:
            st.markdown(
                f"<div class='kpi-card'><p class='kpi-label'>Last Refresh (UTC)</p>"
                f"<p class='kpi-value'>{datetime.now(timezone.utc).strftime('%H:%M:%S')}</p></div>",
                unsafe_allow_html=True,
            )
        with kpi2:
            st.markdown(
                f"<div class='kpi-card'><p class='kpi-label'>Latest Event Age (s)</p>"
                f"<p class='kpi-value'>{max(age_seconds, 0)}</p></div>",
                unsafe_allow_html=True,
            )
        with kpi3:
            st.markdown(
                f"<div class='kpi-card'><p class='kpi-label'>Tracked Symbols</p>"
                f"<p class='kpi-value'>{total_symbols}</p></div>",
                unsafe_allow_html=True,
            )

        st.markdown("<div class='block-gap'></div>", unsafe_allow_html=True)

        if not df_chart.empty:
            cutoff = now_utc - pd.Timedelta(seconds=n_seconds)
            df_recent = df_chart[df_chart["event_time"] >= cutoff].copy()

            if not df_recent.empty:
                price_wide = (
                    df_recent
                    .drop_duplicates(subset=["event_time", "symbol"], keep="last")
                    .pivot(index="event_time", columns="symbol", values="avg_price")
                    .sort_index()
                )
                price_long = (
                    price_wide.reset_index()
                    .melt(id_vars="event_time", var_name="symbol", value_name="avg_price")
                    .dropna(subset=["avg_price"])
                )
                render_island_line_chart(
                    price_long,
                    y_col="avg_price",
                    title=f"Real-Time Price Trend (Last {n_seconds}s)",
                )

                df_recent["base_price"] = df_recent.groupby("symbol")["avg_price"].transform("first")
                df_recent["price_change"] = df_recent["avg_price"] - df_recent["base_price"]
                change_wide = (
                    df_recent
                    .drop_duplicates(subset=["event_time", "symbol"], keep="last")
                    .pivot(index="event_time", columns="symbol", values="price_change")
                    .sort_index()
                )
                change_long = (
                    change_wide.reset_index()
                    .melt(id_vars="event_time", var_name="symbol", value_name="price_change")
                    .dropna(subset=["price_change"])
                )
                render_island_line_chart(
                    change_long,
                    y_col="price_change",
                    title=f"Price Delta vs. Window Start (Last {n_seconds}s)",
                )
            else:
                st.warning(
                    f"No points in the last {n_seconds}s. "
                    "Data source may be idle; keep refresh enabled and wait for new trades."
                )

        info_col1, info_col2, info_col3 = st.columns(3)
        with info_col1:
            with st.container(border=True):
                st.markdown("**System Status**")
                st.caption("Pipeline heartbeat & data freshness")
                if age_seconds < 0:
                    st.caption("No data available")
                else:
                    freshness = max(0, 100 - min(max(age_seconds, 0), 100))
                    st.progress(freshness)
                    st.caption(f"Freshness score: {freshness}%")
        with info_col2:
            with st.container(border=True):
                st.markdown("**Symbol Distribution**")
                symbol_counts = df["symbol"].value_counts().rename_axis("symbol").reset_index(name="count")
                if symbol_counts.empty:
                    st.caption("No data available")
                else:
                    for _, row in symbol_counts.head(5).iterrows():
                        st.write(f"`{row['symbol']}`")
                        pct = int((row["count"] / max(len(df), 1)) * 100)
                        st.progress(max(6, pct))
                        st.caption(f"{pct}% of loaded rows")
        with info_col3:
            with st.container(border=True):
                st.markdown("**Quick Info**")
                if len(df) == 0:
                    st.caption("No data available")
                else:
                    st.caption(f"Rows loaded: {len(df)}")
                    st.caption(f"Window: last {n_seconds}s")
                    st.caption(f"Refresh interval: {refresh_seconds}s")
                    st.caption(f"Auto-refresh: {'ON' if poll else 'OFF'}")

        st.markdown("<p class='section-title'>Latest HBase Rows</p>", unsafe_allow_html=True)
        column_labels = {
            "row_key": "Row Key",
            "anomaly": "Anomaly",
            "asset_name": "Asset Name",
            "avg_price": "Avg Price",
            "max_price": "Max Price",
            "min_price": "Min Price",
            "price_range_pct": "Price Range (%)",
            "risk_tier": "Risk Tier",
            "symbol": "Symbol",
            "trade_count": "Trade Count",
            "window_end_ms": "Window End (ms)",
            "event_time": "Event Time (UTC)",
        }
        table_df = df.rename(columns=column_labels)
        st.dataframe(table_df, use_container_width=True)

    if poll:
        time.sleep(refresh_seconds)
        st.rerun()


if __name__ == "__main__":
    main()
