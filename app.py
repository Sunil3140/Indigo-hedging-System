"""
Indigo Airlines Real-Time Hedging Dashboard
=============================================
A Streamlit-based dashboard for monitoring and analyzing real-time fuel prices
and currency exchange rates for hedging decisions.

Author: Indigo Airlines
Date: 2024
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sqlite3
import os
import time
import subprocess
import sys


# ============================================================================
# PAGE CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title="Indigo Airlines Real-Time Hedging Dashboard",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded"
)


# ============================================================================
# CUSTOM CSS STYLING
# ============================================================================

st.markdown("""
<style>
.main-header {
    font-size: 2.5rem;
    color: #1f77b4;
    text-align: center;
    margin-bottom: 2rem;
    font-weight: bold;
}
.metric-card {
    background-color: #f0f2f6;
    padding: 1rem;
    border-radius: 0.5rem;
    border-left: 4px solid #1f77b4;
    margin: 0.5rem 0;
}
.status-online {
    color: #28a745;
    font-weight: bold;
}
.status-offline {
    color: #dc3545;
    font-weight: bold;
}
.refresh-button {
    background-color: #1f77b4;
    color: white;
    padding: 0.5rem 1rem;
    border: none;
    border-radius: 0.25rem;
    cursor: pointer;
}
</style>
""", unsafe_allow_html=True)


# ============================================================================
# DATABASE FUNCTIONS
# ============================================================================

def get_database_connection():
    try:
        return sqlite3.connect('hedging_data.db')
    except Exception as e:
        st.error(f"Database connection error: {e}")
        return None


def load_data():
    conn = get_database_connection()
    if conn is None:
        return None, None
    try:
        fuel_df = pd.read_sql_query(
            "SELECT * FROM fuel_prices ORDER BY timestamp DESC LIMIT 100",
            conn
        )
        fuel_df['timestamp'] = pd.to_datetime(fuel_df['timestamp'])

        currency_df = pd.read_sql_query(
            "SELECT * FROM currency_rates ORDER BY timestamp DESC LIMIT 100",
            conn
        )
        currency_df['timestamp'] = pd.to_datetime(currency_df['timestamp'])

        conn.close()
        return fuel_df, currency_df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        conn.close()
        return None, None


# ============================================================================
# DATA COLLECTION FUNCTIONS
# ============================================================================

def collect_realtime_data():
    try:
        result = subprocess.run(
            [sys.executable, 'realtime_data_collector.py'],
            capture_output=True,
            text=True,
            timeout=30
        )
        if result.returncode == 0:
            return True, "Real-time data collected successfully!"
        else:
            return False, f"Error: {result.stderr}"
    except subprocess.TimeoutExpired:
        return False, "Data collection timed out"
    except Exception as e:
        return False, f"Error running data collector: {e}"


# ============================================================================
# VISUALIZATION FUNCTIONS
# ============================================================================

def create_fuel_chart(fuel_df):
    if fuel_df.empty:
        return None

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=fuel_df['timestamp'],
        y=fuel_df['jet_fuel'],
        mode='lines+markers',
        name='Jet Fuel',
        line=dict(color='#1f77b4', width=3),
        hovertemplate='<b>Jet Fuel</b><br>Time: %{x}<br>Price: $%{y:.3f}<extra></extra>'
    ))

    fig.add_trace(go.Scatter(
        x=fuel_df['timestamp'],
        y=fuel_df['brent_crude'],
        mode='lines+markers',
        name='Brent Crude',
        line=dict(color='#ff7f0e', width=2),
        hovertemplate='<b>Brent Crude</b><br>Time: %{x}<br>Price: $%{y:.2f}<extra></extra>'
    ))

    fig.add_trace(go.Scatter(
        x=fuel_df['timestamp'],
        y=fuel_df['wti_crude'],
        mode='lines+markers',
        name='WTI Crude',
        line=dict(color='#2ca02c', width=2),
        hovertemplate='<b>WTI Crude</b><br>Time: %{x}<br>Price: $%{y:.2f}<extra></extra>'
    ))

    fig.update_layout(
        title="Real-Time Fuel Prices",
        xaxis_title="Time",
        yaxis_title="Price ($/gallon)",
        hovermode='x unified',
        height=400,
        showlegend=True
    )

    return fig


def create_currency_chart(currency_df):
    if currency_df.empty:
        return None

    fig = go.Figure()

    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
    currencies = ['USD/INR', 'EUR/INR', 'GBP/INR', 'JPY/INR']
    columns = ['usd_inr', 'eur_inr', 'gbp_inr', 'jpy_inr']

    for i, (currency, column) in enumerate(zip(currencies, columns)):
        fig.add_trace(go.Scatter(
            x=currency_df['timestamp'],
            y=currency_df[column],
            mode='lines+markers',
            name=currency,
            line=dict(color=colors[i], width=2),
            hovertemplate=f'<b>{currency}</b><br>Time: %{{x}}<br>Rate: ₹%{{y:.2f}}<extra></extra>'
        ))

    fig.update_layout(
        title="Real-Time Currency Exchange Rates",
        xaxis_title="Time",
        yaxis_title="Exchange Rate (INR)",
        hovermode='x unified',
        height=400,
        showlegend=True
    )

    return fig


# ============================================================================
# ANALYSIS FUNCTIONS
# ============================================================================

def calculate_price_changes(fuel_df, currency_df):
    if fuel_df.empty or currency_df.empty:
        return {}

    changes = {}
    if len(fuel_df) >= 2:
        latest_fuel = fuel_df.iloc[0]
        previous_fuel = fuel_df.iloc[1]
        changes['jet_fuel_change'] = (
            (latest_fuel['jet_fuel'] - previous_fuel['jet_fuel']) /
            previous_fuel['jet_fuel'] * 100
        )
        changes['brent_change'] = (
            (latest_fuel['brent_crude'] - previous_fuel['brent_crude']) /
            previous_fuel['brent_crude'] * 100
        )
        changes['wti_change'] = (
            (latest_fuel['wti_crude'] - previous_fuel['wti_crude']) /
            previous_fuel['wti_crude'] * 100
        )

    if len(currency_df) >= 2:
        latest_currency = currency_df.iloc[0]
        previous_currency = currency_df.iloc[1]
        changes['usd_inr_change'] = (
            (latest_currency['usd_inr'] - previous_currency['usd_inr']) /
            previous_currency['usd_inr'] * 100
        )
        changes['eur_inr_change'] = (
            (latest_currency['eur_inr'] - previous_currency['eur_inr']) /
            previous_currency['eur_inr'] * 100
        )
        changes['gbp_inr_change'] = (
            (latest_currency['gbp_inr'] - previous_currency['gbp_inr']) /
            previous_currency['gbp_inr'] * 100
        )
        changes['jpy_inr_change'] = (
            (latest_currency['jpy_inr'] - previous_currency['jpy_inr']) /
            previous_currency['jpy_inr'] * 100
        )

    return changes


# ============================================================================
# MAIN APPLICATION
# ============================================================================

def main():
    st.markdown(
        '<h1 class="main-header">✈️ Indigo Airlines Real-Time Hedging Dashboard</h1>',
        unsafe_allow_html=True
    )

    # Sidebar Controls
    st.sidebar.title("Dashboard Controls")
    st.sidebar.markdown("### Real-Time Data Collection")
    if st.sidebar.button("🔄 Collect Live Data", type="primary"):
        with st.spinner("Collecting real-time data..."):
            success, message = collect_realtime_data()
            if success:
                st.sidebar.success(message)
                st.rerun()
            else:
                st.sidebar.error(message)

    auto_refresh = st.sidebar.checkbox("Auto-refresh every 5 minutes", value=False)
    if auto_refresh:
        time.sleep(300)
        st.rerun()

    st.sidebar.markdown("### Data Sources")
    st.sidebar.info("""
    **Fuel Prices:** Yahoo Finance  
    **Currency Rates:** ExchangeRate-API / Yahoo Finance
    """)

    # Load Data
    fuel_df, currency_df = load_data()
    if fuel_df is None or currency_df is None:
        st.error("Unable to load data. Please check your database connection.")
        return
    if fuel_df.empty or currency_df.empty:
        st.warning("No data available. Click 'Collect Live Data' to fetch real-time data.")
        return

    changes = calculate_price_changes(fuel_df, currency_df)

    # Real-Time Metrics
    st.markdown("## 📊 Real-Time Market Metrics")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        latest_jet = fuel_df['jet_fuel'].iloc[0]
        st.metric("Jet Fuel Price", f"${latest_jet:.3f}",
                  delta=f"{changes.get('jet_fuel_change', 0):+.2f}%")

    with col2:
        latest_usd = currency_df['usd_inr'].iloc[0]
        st.metric("USD/INR Rate", f"₹{latest_usd:.2f}",
                  delta=f"{changes.get('usd_inr_change', 0):+.2f}%")

    with col3:
        latest_brent = fuel_df['brent_crude'].iloc[0]
        st.metric("Brent Crude", f"${latest_brent:.2f}",
                  delta=f"{changes.get('brent_change', 0):+.2f}%")

    with col4:
        latest_eur = currency_df['eur_inr'].iloc[0]
        st.metric("EUR/INR Rate", f"₹{latest_eur:.2f}",
                  delta=f"{changes.get('eur_inr_change', 0):+.2f}%")

    # Data Freshness
    latest_ts = max(fuel_df['timestamp'].max(), currency_df['timestamp'].max())
    age = datetime.now() - latest_ts
    if age.total_seconds() < 300:
        st.success(f"🟢 Data is fresh (updated {age.seconds // 60} minutes ago)")
    elif age.total_seconds() < 3600:
        st.warning(f"🟡 Data is {age.seconds // 60} minutes old")
    else:
        st.error(f"🔴 Data is {age.seconds // 3600} hours old")

    # Charts
    st.markdown("## 📈 Real-Time Market Visualization")
    fuel_chart = create_fuel_chart(fuel_df)
    if fuel_chart:
        st.plotly_chart(fuel_chart, use_container_width=True)
    currency_chart = create_currency_chart(currency_df)
    if currency_chart:
        st.plotly_chart(currency_chart, use_container_width=True)

    # ========================================================================
    # NEW SECTION: BAR AND PIE CHARTS
    # ========================================================================
    st.markdown("## 📊 Additional Market Insights")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Fuel Price Comparison")
        fuel_snapshot = pd.DataFrame({
            "Fuel Type": ["Jet Fuel", "Brent Crude", "WTI Crude"],
            "Price ($/gallon)": [
                fuel_df["jet_fuel"].iloc[0],
                fuel_df["brent_crude"].iloc[0],
                fuel_df["wti_crude"].iloc[0]
            ]
        })
        bar_fig = px.bar(fuel_snapshot, x="Fuel Type", y="Price ($/gallon)",
                         color="Fuel Type", text_auto=".2f",
                         title="Latest Fuel Price Comparison")
        bar_fig.update_layout(showlegend=False, height=400)
        st.plotly_chart(bar_fig, use_container_width=True)

    with col2:
        st.subheader("Currency Exposure Distribution")
        currency_snapshot = pd.DataFrame({
            "Currency Pair": ["USD/INR", "EUR/INR", "GBP/INR", "JPY/INR"],
            "Rate": [
                currency_df["usd_inr"].iloc[0],
                currency_df["eur_inr"].iloc[0],
                currency_df["gbp_inr"].iloc[0],
                currency_df["jpy_inr"].iloc[0]
            ]
        })
        pie_fig = px.pie(currency_snapshot, names="Currency Pair", values="Rate",
                         title="Current Exchange Rate Proportions",
                         hole=0.3,
                         color_discrete_sequence=px.colors.sequential.Blues)
        pie_fig.update_traces(textinfo='percent+label')
        st.plotly_chart(pie_fig, use_container_width=True)

    # Data Tables
    st.markdown("## 📋 Recent Market Data")
    tab1, tab2 = st.tabs(["Fuel Prices", "Currency Rates"])
    with tab1:
        st.dataframe(fuel_df.head(10), use_container_width=True)
    with tab2:
        st.dataframe(currency_df.head(10), use_container_width=True)

    # Hedging Recommendations
    st.markdown("## 🛡️ Real-Time Hedging Recommendations")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Fuel Hedging Analysis")
        jet_change = changes.get('jet_fuel_change', 0)
        if jet_change > 2:
            st.error("🚨 **HIGH HEDGE RECOMMENDED** - Jet fuel prices rising rapidly")
        elif jet_change > 0.5:
            st.warning("⚠️ **MODERATE HEDGE** - Jet fuel prices trending up")
        elif jet_change < -1:
            st.success("✅ **LOW HEDGE** - Jet fuel prices declining")
        else:
            st.info("📊 **MONITOR** - Jet fuel prices stable")

    with col2:
        st.markdown("### Currency Hedging Analysis")
        usd_change = changes.get('usd_inr_change', 0)
        if abs(usd_change) > 1:
            st.error("🚨 **HIGH VOLATILITY** - USD/INR moving significantly")
        elif abs(usd_change) > 0.3:
            st.warning("⚠️ **MODERATE VOLATILITY** - USD/INR showing movement")
        else:
            st.success("✅ **STABLE** - USD/INR relatively stable")

    st.markdown("---")
    st.markdown(
        f"<div style='text-align: center; color: #666;'>"
        f"Indigo Airlines Real-Time Hedging System | Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        f"</div>", unsafe_allow_html=True)


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    main()
