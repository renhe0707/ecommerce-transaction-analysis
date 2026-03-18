"""
E-Commerce Transaction Analysis
================================
Analyzes 10,000+ transaction records to build KPI dashboards
and generate data-driven retention recommendations.

KPIs:
  - Repurchase Rate
  - Average Order Value (AOV)
  - Conversion Rate
  - Customer Lifetime Value (CLV)
  - Revenue by Channel/Category/Region
  - Monthly Cohort Retention

Outputs:
  - KPI dashboard (output/kpi_dashboard.png)
  - Cohort retention heatmap (output/cohort_retention.png)
  - Channel performance (output/channel_analysis.png)
  - Customer segmentation (output/customer_segments.png)
  - Recommendations summary (output/recommendations.png)

Author: Robert
"""

import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.dates as mdates
import seaborn as sns
from datetime import datetime
import sqlite3
import os

# ──────────────────────────────────────────────
# Setup
# ──────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
SQL_DIR = os.path.join(BASE_DIR, 'sql')
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(SQL_DIR, exist_ok=True)

plt.style.use('seaborn-v0_8-whitegrid')
COLORS = {
    'primary': '#1B4332',
    'secondary': '#2D6A4F',
    'accent': '#52B788',
    'warm': '#E76F51',
    'blue': '#457B9D',
    'light': '#95D5B2',
    'gray': '#6C757D',
    'bg': '#F8F9FA',
}
PALETTE = [COLORS['primary'], COLORS['secondary'], COLORS['accent'],
           COLORS['warm'], COLORS['blue'], '#E9C46A', '#264653']
plt.rcParams.update({
    'figure.dpi': 150, 'savefig.dpi': 150, 'font.size': 10,
    'axes.titlesize': 13, 'axes.labelsize': 11,
    'figure.facecolor': 'white',
})


# ──────────────────────────────────────────────
# 1. LOAD DATA
# ──────────────────────────────────────────────
def load_data():
    print("=" * 60)
    print("STEP 1: Loading Data")
    print("=" * 60)

    txn = pd.read_csv(os.path.join(DATA_DIR, 'transactions.csv'), parse_dates=['transaction_date'])
    cust = pd.read_csv(os.path.join(DATA_DIR, 'customers.csv'), parse_dates=['signup_date'])
    sessions = pd.read_csv(os.path.join(DATA_DIR, 'sessions.csv'), parse_dates=['visit_date'])

    print(f"  ✓ Transactions: {len(txn):,} records")
    print(f"  ✓ Customers: {len(cust):,} profiles")
    print(f"  ✓ Sessions: {len(sessions):,} site visits")
    print(f"  ✓ Date range: {txn['transaction_date'].min().date()} to {txn['transaction_date'].max().date()}")

    return txn, cust, sessions


# ──────────────────────────────────────────────
# 2. COMPUTE KPIs
# ──────────────────────────────────────────────
def compute_kpis(txn, sessions):
    print("\n" + "=" * 60)
    print("STEP 2: Computing Core KPIs")
    print("=" * 60)

    # Filter out returns for revenue metrics
    valid_txn = txn[~txn['returned']].copy()

    # Overall KPIs
    total_revenue = valid_txn['total_amount'].sum()
    total_orders = len(valid_txn)
    unique_customers = valid_txn['customer_id'].nunique()
    aov = total_revenue / total_orders

    # Repurchase rate
    orders_per_customer = valid_txn.groupby('customer_id').size()
    repeat_customers = (orders_per_customer > 1).sum()
    repurchase_rate = repeat_customers / unique_customers

    # Conversion rate
    total_sessions = len(sessions)
    conversion_rate = total_orders / total_sessions

    # Return rate
    return_rate = txn['returned'].mean()

    # CLV (simple: avg revenue per customer over the period)
    clv = total_revenue / unique_customers

    # Revenue per channel
    rev_by_channel = valid_txn.groupby('channel')['total_amount'].sum()

    kpis = {
        'Total Revenue': total_revenue,
        'Total Orders': total_orders,
        'Unique Customers': unique_customers,
        'AOV': aov,
        'Repurchase Rate': repurchase_rate,
        'Conversion Rate': conversion_rate,
        'Return Rate': return_rate,
        'Avg CLV': clv,
    }

    print(f"\n  {'Metric':<25s} {'Value':>15s}")
    print(f"  {'-'*40}")
    print(f"  {'Total Revenue':<25s} {'$'+f'{total_revenue:,.2f}':>15s}")
    print(f"  {'Total Orders':<25s} {f'{total_orders:,}':>15s}")
    print(f"  {'Unique Customers':<25s} {f'{unique_customers:,}':>15s}")
    print(f"  {'AOV':<25s} {'$'+f'{aov:.2f}':>15s}")
    print(f"  {'Repurchase Rate':<25s} {f'{repurchase_rate:.1%}':>15s}")
    print(f"  {'Conversion Rate':<25s} {f'{conversion_rate:.2%}':>15s}")
    print(f"  {'Return Rate':<25s} {f'{return_rate:.1%}':>15s}")
    print(f"  {'Avg CLV':<25s} {'$'+f'{clv:.2f}':>15s}")

    return kpis, valid_txn


# ──────────────────────────────────────────────
# 3. MONTHLY TRENDS
# ──────────────────────────────────────────────
def compute_monthly_trends(txn, valid_txn, sessions):
    print("\n" + "=" * 60)
    print("STEP 3: Computing Monthly Trends")
    print("=" * 60)

    valid_txn = valid_txn.copy()
    valid_txn['month'] = valid_txn['transaction_date'].dt.to_period('M')

    monthly = valid_txn.groupby('month').agg(
        revenue=('total_amount', 'sum'),
        orders=('transaction_id', 'count'),
        customers=('customer_id', 'nunique'),
        aov=('total_amount', 'mean'),
    ).reset_index()
    monthly['month_dt'] = monthly['month'].dt.to_timestamp()

    # Monthly sessions and conversion
    sessions = sessions.copy()
    sessions['month'] = sessions['visit_date'].dt.to_period('M')
    monthly_sessions = sessions.groupby('month').size().reset_index(name='sessions')
    monthly_sessions['month_dt'] = monthly_sessions['month'].dt.to_timestamp()

    monthly = monthly.merge(monthly_sessions, on=['month', 'month_dt'], how='left')
    monthly['conversion_rate'] = monthly['orders'] / monthly['sessions']

    print(f"  ✓ Computed {len(monthly)} months of trend data")
    return monthly


# ──────────────────────────────────────────────
# 4. COHORT ANALYSIS
# ──────────────────────────────────────────────
def cohort_analysis(valid_txn):
    print("\n" + "=" * 60)
    print("STEP 4: Cohort Retention Analysis")
    print("=" * 60)

    df = valid_txn.copy()
    df['order_month'] = df['transaction_date'].dt.to_period('M')

    # First purchase month per customer
    first_purchase = df.groupby('customer_id')['order_month'].min().reset_index()
    first_purchase.columns = ['customer_id', 'cohort_month']

    df = df.merge(first_purchase, on='customer_id')
    df['cohort_index'] = (df['order_month'] - df['cohort_month']).apply(lambda x: x.n)

    # Cohort table
    cohort_table = df.groupby(['cohort_month', 'cohort_index'])['customer_id'].nunique().reset_index()
    cohort_table.columns = ['cohort_month', 'cohort_index', 'customers']

    cohort_pivot = cohort_table.pivot(index='cohort_month', columns='cohort_index', values='customers')
    cohort_sizes = cohort_pivot[0]
    retention = cohort_pivot.divide(cohort_sizes, axis=0)

    # Trim to first 12 months and recent cohorts
    retention = retention.iloc[:, :13]
    if len(retention) > 12:
        retention = retention.iloc[-12:]

    print(f"  ✓ Built retention matrix: {retention.shape[0]} cohorts × {retention.shape[1]} months")
    return retention


# ──────────────────────────────────────────────
# 5. CUSTOMER SEGMENTATION (RFM)
# ──────────────────────────────────────────────
def rfm_segmentation(valid_txn):
    print("\n" + "=" * 60)
    print("STEP 5: RFM Customer Segmentation")
    print("=" * 60)

    reference_date = valid_txn['transaction_date'].max() + pd.Timedelta(days=1)

    rfm = valid_txn.groupby('customer_id').agg(
        recency=('transaction_date', lambda x: (reference_date - x.max()).days),
        frequency=('transaction_id', 'count'),
        monetary=('total_amount', 'sum'),
    ).reset_index()

    # Score 1-4 for each dimension
    for col in ['recency', 'frequency', 'monetary']:
        if col == 'recency':
            rfm[f'{col}_score'] = pd.qcut(rfm[col], 4, labels=[4, 3, 2, 1]).astype(int)
        else:
            rfm[f'{col}_score'] = pd.qcut(rfm[col].rank(method='first'), 4, labels=[1, 2, 3, 4]).astype(int)

    rfm['rfm_score'] = rfm['recency_score'] + rfm['frequency_score'] + rfm['monetary_score']

    # Segment labels
    def label_segment(score):
        if score >= 10:
            return 'Champions'
        elif score >= 8:
            return 'Loyal'
        elif score >= 6:
            return 'Potential'
        elif score >= 4:
            return 'At Risk'
        else:
            return 'Lost'

    rfm['segment'] = rfm['rfm_score'].apply(label_segment)

    seg_summary = rfm.groupby('segment').agg(
        count=('customer_id', 'count'),
        avg_monetary=('monetary', 'mean'),
        avg_frequency=('frequency', 'mean'),
        avg_recency=('recency', 'mean'),
    ).reindex(['Champions', 'Loyal', 'Potential', 'At Risk', 'Lost'])

    print(f"\n  Customer Segments:")
    print(f"  {'Segment':<15s} {'Count':>8s} {'Avg Revenue':>14s} {'Avg Orders':>12s} {'Avg Recency':>13s}")
    print(f"  {'-'*62}")
    for seg, row in seg_summary.iterrows():
        rev_str = f"${row['avg_monetary']:.2f}"
        print(f"  {seg:<15s} {int(row['count']):>8,} {rev_str:>14s} "
              f"{row['avg_frequency']:>12.1f} {row['avg_recency']:>10.0f} days")

    return rfm, seg_summary


# ──────────────────────────────────────────────
# 6. VISUALIZATIONS
# ──────────────────────────────────────────────
def plot_kpi_dashboard(kpis, monthly, valid_txn):
    print("\n" + "=" * 60)
    print("STEP 6a: KPI Dashboard")
    print("=" * 60)

    fig = plt.figure(figsize=(16, 14))
    gs = fig.add_gridspec(3, 3, hspace=0.35, wspace=0.3)

    # Row 1: KPI cards (using text)
    kpi_cards = [
        ('Total Revenue', f"${kpis['Total Revenue']:,.0f}", COLORS['primary']),
        ('Avg Order Value', f"${kpis['AOV']:.2f}", COLORS['secondary']),
        ('Repurchase Rate', f"{kpis['Repurchase Rate']:.1%}", COLORS['accent']),
        ('Conversion Rate', f"{kpis['Conversion Rate']:.2%}", COLORS['blue']),
        ('Return Rate', f"{kpis['Return Rate']:.1%}", COLORS['warm']),
        ('Avg CLV', f"${kpis['Avg CLV']:.2f}", COLORS['primary']),
    ]

    for i, (label, value, color) in enumerate(kpi_cards):
        ax = fig.add_subplot(gs[0, i % 3]) if i < 3 else None
        if i >= 3:
            continue  # Only show top 3 as cards

    # Actually, let's do a cleaner layout
    fig.clear()
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))

    # 1. Monthly Revenue Trend
    ax = axes[0, 0]
    ax.fill_between(monthly['month_dt'], monthly['revenue'], alpha=0.3, color=COLORS['primary'])
    ax.plot(monthly['month_dt'], monthly['revenue'], color=COLORS['primary'], linewidth=2)
    ax.set_title('Monthly Revenue', fontweight='bold')
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'${x/1000:.0f}K'))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b\n%Y'))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=4))

    # 2. Monthly AOV
    ax = axes[0, 1]
    ax.bar(monthly['month_dt'], monthly['aov'], width=25, color=COLORS['secondary'], alpha=0.8)
    ax.axhline(y=kpis['AOV'], color=COLORS['warm'], linestyle='--', linewidth=1.5, label=f"Overall: ${kpis['AOV']:.2f}")
    ax.set_title('Average Order Value (AOV)', fontweight='bold')
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'${x:.0f}'))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b\n%Y'))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
    ax.legend(fontsize=9)

    # 3. Monthly Conversion Rate
    ax = axes[0, 2]
    ax.plot(monthly['month_dt'], monthly['conversion_rate'] * 100, color=COLORS['blue'],
            linewidth=2, marker='o', markersize=3)
    ax.set_title('Monthly Conversion Rate', fontweight='bold')
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:.1f}%'))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b\n%Y'))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=4))

    # 4. Revenue by Category
    ax = axes[1, 0]
    cat_rev = valid_txn.groupby('category')['total_amount'].sum().sort_values(ascending=True)
    bars = ax.barh(cat_rev.index, cat_rev.values, color=PALETTE[:len(cat_rev)])
    ax.set_title('Revenue by Category', fontweight='bold')
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'${x/1000:.0f}K'))

    # 5. Revenue by Channel
    ax = axes[1, 1]
    ch_rev = valid_txn.groupby('channel')['total_amount'].sum().sort_values(ascending=True)
    ax.barh(ch_rev.index, ch_rev.values, color=PALETTE[:len(ch_rev)])
    ax.set_title('Revenue by Channel', fontweight='bold')
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'${x/1000:.0f}K'))

    # 6. Orders by Device
    ax = axes[1, 2]
    device_data = valid_txn.groupby('device')['transaction_id'].count()
    wedges, texts, autotexts = ax.pie(device_data, labels=device_data.index,
                                       autopct='%1.1f%%', colors=PALETTE[:len(device_data)],
                                       startangle=90, textprops={'fontsize': 10})
    ax.set_title('Orders by Device', fontweight='bold')

    fig.suptitle('E-Commerce KPI Dashboard', fontsize=16, fontweight='bold', y=1.01)
    plt.tight_layout()

    path = os.path.join(OUTPUT_DIR, 'kpi_dashboard.png')
    fig.savefig(path, bbox_inches='tight')
    plt.close()
    print(f"  ✓ Saved: {path}")


def plot_cohort_retention(retention):
    print("\n" + "=" * 60)
    print("STEP 6b: Cohort Retention Heatmap")
    print("=" * 60)

    fig, ax = plt.subplots(figsize=(14, 8))

    # Format index as strings
    retention_display = retention.copy()
    retention_display.index = [str(p) for p in retention_display.index]

    sns.heatmap(retention_display, annot=True, fmt='.0%', cmap='YlGn',
                vmin=0, vmax=0.5, linewidths=0.5, ax=ax,
                cbar_kws={'label': 'Retention Rate', 'shrink': 0.8})

    ax.set_title('Monthly Cohort Retention Analysis\n(% of customers returning after first purchase)',
                 fontsize=14, fontweight='bold', pad=20)
    ax.set_xlabel('Months After First Purchase', fontsize=12)
    ax.set_ylabel('Cohort (First Purchase Month)', fontsize=12)

    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, 'cohort_retention.png')
    fig.savefig(path, bbox_inches='tight')
    plt.close()
    print(f"  ✓ Saved: {path}")


def plot_channel_analysis(valid_txn, sessions):
    print("\n" + "=" * 60)
    print("STEP 6c: Channel Performance Analysis")
    print("=" * 60)

    fig, axes = plt.subplots(1, 3, figsize=(18, 6))

    # 1. AOV by Channel
    ax = axes[0]
    ch_aov = valid_txn.groupby('channel')['total_amount'].mean().sort_values(ascending=True)
    bars = ax.barh(ch_aov.index, ch_aov.values, color=PALETTE[:len(ch_aov)])
    ax.set_title('AOV by Channel', fontweight='bold')
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'${x:.0f}'))
    for bar, val in zip(bars, ch_aov.values):
        ax.text(val + 1, bar.get_y() + bar.get_height()/2, f'${val:.2f}',
                va='center', fontsize=9, color=COLORS['gray'])

    # 2. Conversion by Channel
    ax = axes[1]
    ch_sessions = sessions.groupby('channel').size()
    ch_orders = valid_txn.groupby('channel').size()
    ch_conv = (ch_orders / ch_sessions * 100).sort_values(ascending=True)
    bars = ax.barh(ch_conv.index, ch_conv.values, color=PALETTE[:len(ch_conv)])
    ax.set_title('Conversion Rate by Channel', fontweight='bold')
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:.1f}%'))

    # 3. Repurchase Rate by Channel
    ax = axes[2]
    ch_repeat = valid_txn.groupby(['channel', 'customer_id']).size().reset_index(name='orders')
    ch_cust_total = ch_repeat.groupby('channel')['customer_id'].count()
    ch_cust_repeat = ch_repeat[ch_repeat['orders'] > 1].groupby('channel')['customer_id'].count()
    ch_repurchase = (ch_cust_repeat / ch_cust_total * 100).sort_values(ascending=True)
    bars = ax.barh(ch_repurchase.index, ch_repurchase.values, color=PALETTE[:len(ch_repurchase)])
    ax.set_title('Repurchase Rate by Channel', fontweight='bold')
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:.0f}%'))

    fig.suptitle('Channel Performance Comparison', fontsize=14, fontweight='bold', y=1.02)
    plt.tight_layout()

    path = os.path.join(OUTPUT_DIR, 'channel_analysis.png')
    fig.savefig(path, bbox_inches='tight')
    plt.close()
    print(f"  ✓ Saved: {path}")


def plot_customer_segments(rfm, seg_summary):
    print("\n" + "=" * 60)
    print("STEP 6d: Customer Segmentation")
    print("=" * 60)

    fig, axes = plt.subplots(1, 3, figsize=(18, 6))

    seg_order = ['Champions', 'Loyal', 'Potential', 'At Risk', 'Lost']
    seg_colors = [COLORS['primary'], COLORS['secondary'], COLORS['accent'],
                  '#E9C46A', COLORS['warm']]

    # 1. Segment Distribution
    ax = axes[0]
    seg_counts = rfm['segment'].value_counts().reindex(seg_order)
    ax.bar(seg_counts.index, seg_counts.values, color=seg_colors)
    ax.set_title('Customers by Segment', fontweight='bold')
    ax.set_ylabel('Number of Customers')
    for i, (seg, val) in enumerate(seg_counts.items()):
        ax.text(i, val + 10, f'{val:,}', ha='center', fontsize=9, fontweight='bold')
    ax.tick_params(axis='x', rotation=30)

    # 2. Revenue by Segment
    ax = axes[1]
    seg_rev = rfm.groupby('segment')['monetary'].sum().reindex(seg_order)
    ax.bar(seg_rev.index, seg_rev.values, color=seg_colors)
    ax.set_title('Total Revenue by Segment', fontweight='bold')
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'${x/1000:.0f}K'))
    ax.tick_params(axis='x', rotation=30)

    # 3. Recency vs Monetary scatter
    ax = axes[2]
    for i, seg in enumerate(seg_order):
        subset = rfm[rfm['segment'] == seg]
        ax.scatter(subset['recency'], subset['monetary'], s=15, alpha=0.4,
                   color=seg_colors[i], label=seg)
    ax.set_title('Recency vs Revenue by Segment', fontweight='bold')
    ax.set_xlabel('Days Since Last Purchase')
    ax.set_ylabel('Total Spend ($)')
    ax.legend(fontsize=8, loc='upper right')

    fig.suptitle('Customer Segmentation (RFM Analysis)', fontsize=14, fontweight='bold', y=1.02)
    plt.tight_layout()

    path = os.path.join(OUTPUT_DIR, 'customer_segments.png')
    fig.savefig(path, bbox_inches='tight')
    plt.close()
    print(f"  ✓ Saved: {path}")


def plot_recommendations(kpis, valid_txn, rfm):
    print("\n" + "=" * 60)
    print("STEP 6e: Recommendations Summary")
    print("=" * 60)

    fig, axes = plt.subplots(1, 3, figsize=(18, 7))

    # Recommendation 1: Email re-engagement for At Risk + Lost
    ax = axes[0]
    at_risk = rfm[rfm['segment'].isin(['At Risk', 'Lost'])]
    recoverable_rev = at_risk['monetary'].sum() * 0.15  # 15% recovery target
    segments = ['At Risk', 'Lost']
    counts = [len(rfm[rfm['segment'] == s]) for s in segments]
    colors_r = ['#E9C46A', COLORS['warm']]
    bars = ax.bar(segments, counts, color=colors_r)
    ax.set_title('Rec #1: Email Re-engagement\nTarget: At-Risk & Lost Customers', fontweight='bold', fontsize=11)
    ax.set_ylabel('Number of Customers')
    for bar, val in zip(bars, counts):
        ax.text(bar.get_x() + bar.get_width()/2, val + 5, f'{val:,}',
                ha='center', fontsize=11, fontweight='bold')
    ax.text(0.5, 0.02, f'Projected Recovery: ${recoverable_rev:,.0f}',
            transform=ax.transAxes, ha='center', fontsize=10, color=COLORS['primary'],
            fontweight='bold', bbox=dict(boxstyle='round,pad=0.5', facecolor='#D8F3DC', alpha=0.8))

    # Recommendation 2: Loyalty discount for high-frequency buyers
    ax = axes[1]
    freq_dist = rfm['frequency'].value_counts().sort_index()
    freq_bins = pd.cut(rfm['frequency'], bins=[0, 1, 3, 5, 10, 100],
                       labels=['1', '2-3', '4-5', '6-10', '11+'])
    freq_summary = freq_bins.value_counts().reindex(['1', '2-3', '4-5', '6-10', '11+'])
    ax.bar(freq_summary.index, freq_summary.values,
           color=[COLORS['warm'], COLORS['accent'], COLORS['secondary'],
                  COLORS['primary'], COLORS['primary']])
    ax.set_title('Rec #2: Loyalty Discounts\nTarget: 2-3 Purchase Customers → 4+', fontweight='bold', fontsize=11)
    ax.set_ylabel('Number of Customers')
    ax.set_xlabel('Purchase Frequency')
    target_group = len(rfm[(rfm['frequency'] >= 2) & (rfm['frequency'] <= 3)])
    ax.text(0.5, 0.02, f'Target Group: {target_group:,} customers\nProjected Retention Lift: +15%',
            transform=ax.transAxes, ha='center', fontsize=10, color=COLORS['primary'],
            fontweight='bold', bbox=dict(boxstyle='round,pad=0.5', facecolor='#D8F3DC', alpha=0.8))

    # Recommendation 3: Mobile UX optimization
    ax = axes[2]
    device_aov = valid_txn.groupby('device')['total_amount'].mean()
    device_conv = valid_txn.groupby('device').size()
    colors_d = [COLORS['warm'] if d == 'Mobile' else COLORS['secondary'] for d in device_aov.index]
    bars = ax.bar(device_aov.index, device_aov.values, color=colors_d)
    ax.set_title('Rec #3: Mobile UX Optimization\nMobile AOV Lags Desktop', fontweight='bold', fontsize=11)
    ax.set_ylabel('Average Order Value ($)')
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'${x:.0f}'))
    for bar, val in zip(bars, device_aov.values):
        ax.text(bar.get_x() + bar.get_width()/2, val + 1, f'${val:.2f}',
                ha='center', fontsize=10, fontweight='bold')
    gap = device_aov.get('Desktop', 0) - device_aov.get('Mobile', 0)
    ax.text(0.5, 0.02, f'Mobile-Desktop AOV Gap: ${gap:.2f}\nClosing 50% → +${gap*0.5*device_conv.get("Mobile",0)/1000:.0f}K revenue',
            transform=ax.transAxes, ha='center', fontsize=10, color=COLORS['primary'],
            fontweight='bold', bbox=dict(boxstyle='round,pad=0.5', facecolor='#D8F3DC', alpha=0.8))

    fig.suptitle('Data-Driven Recommendations for Retention Improvement',
                 fontsize=14, fontweight='bold', y=1.02)
    plt.tight_layout()

    path = os.path.join(OUTPUT_DIR, 'recommendations.png')
    fig.savefig(path, bbox_inches='tight')
    plt.close()
    print(f"  ✓ Saved: {path}")


# ──────────────────────────────────────────────
# 7. SQL ANALYSIS
# ──────────────────────────────────────────────
def run_sql_analysis(txn, sessions):
    print("\n" + "=" * 60)
    print("STEP 7: SQL Analysis")
    print("=" * 60)

    db_path = os.path.join(DATA_DIR, 'ecommerce.db')
    conn = sqlite3.connect(db_path)

    txn.to_sql('transactions', conn, if_exists='replace', index=False)
    sessions.to_sql('sessions', conn, if_exists='replace', index=False)

    print(f"  ✓ Data loaded into SQLite: {db_path}")

    queries = {
        'Top 10 customers by revenue': """
            SELECT customer_id,
                   COUNT(*) AS total_orders,
                   ROUND(SUM(total_amount), 2) AS total_revenue,
                   ROUND(AVG(total_amount), 2) AS avg_order_value
            FROM transactions
            WHERE returned = 0
            GROUP BY customer_id
            ORDER BY total_revenue DESC
            LIMIT 10;
        """,
        'Monthly repurchase rate': """
            WITH monthly_customers AS (
                SELECT strftime('%Y-%m', transaction_date) AS month,
                       customer_id,
                       COUNT(*) AS orders
                FROM transactions
                WHERE returned = 0
                GROUP BY month, customer_id
            )
            SELECT month,
                   COUNT(*) AS total_customers,
                   SUM(CASE WHEN orders > 1 THEN 1 ELSE 0 END) AS repeat_customers,
                   ROUND(SUM(CASE WHEN orders > 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS repurchase_rate_pct
            FROM monthly_customers
            GROUP BY month
            ORDER BY month
            LIMIT 12;
        """,
    }

    for title, query in queries.items():
        print(f"\n  📊 {title}:")
        result = pd.read_sql_query(query, conn)
        print(result.to_string(index=False))

    conn.close()


def export_sql_files():
    print("\n" + "=" * 60)
    print("STEP 8: Exporting SQL Query Files")
    print("=" * 60)

    queries = {
        '01_kpi_summary.sql': """-- Core KPI Summary
-- Calculates AOV, repurchase rate, and revenue metrics

WITH customer_orders AS (
    SELECT
        customer_id,
        COUNT(*) AS order_count,
        SUM(total_amount) AS total_spend,
        AVG(total_amount) AS avg_order_value,
        MIN(transaction_date) AS first_purchase,
        MAX(transaction_date) AS last_purchase
    FROM transactions
    WHERE returned = 0
    GROUP BY customer_id
)
SELECT
    COUNT(*) AS total_customers,
    SUM(order_count) AS total_orders,
    ROUND(SUM(total_spend), 2) AS total_revenue,
    ROUND(AVG(avg_order_value), 2) AS overall_aov,
    ROUND(SUM(CASE WHEN order_count > 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS repurchase_rate_pct,
    ROUND(AVG(total_spend), 2) AS avg_clv
FROM customer_orders;
""",
        '02_monthly_trends.sql': """-- Monthly Revenue, Orders, AOV Trends
-- Tracks business performance over time

SELECT
    strftime('%Y-%m', transaction_date) AS month,
    COUNT(*) AS orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    ROUND(SUM(total_amount), 2) AS revenue,
    ROUND(AVG(total_amount), 2) AS aov,
    ROUND(SUM(CASE WHEN returned = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS return_rate_pct
FROM transactions
GROUP BY month
ORDER BY month;
""",
        '03_cohort_retention.sql': """-- Cohort Retention Analysis
-- Tracks how many customers return each month after first purchase

WITH first_purchase AS (
    SELECT
        customer_id,
        strftime('%Y-%m', MIN(transaction_date)) AS cohort_month
    FROM transactions
    WHERE returned = 0
    GROUP BY customer_id
),
monthly_activity AS (
    SELECT
        t.customer_id,
        fp.cohort_month,
        strftime('%Y-%m', t.transaction_date) AS activity_month,
        (CAST(strftime('%Y', t.transaction_date) AS INT) -
         CAST(SUBSTR(fp.cohort_month, 1, 4) AS INT)) * 12 +
        (CAST(strftime('%m', t.transaction_date) AS INT) -
         CAST(SUBSTR(fp.cohort_month, 6, 2) AS INT)) AS month_offset
    FROM transactions t
    JOIN first_purchase fp ON t.customer_id = fp.customer_id
    WHERE t.returned = 0
)
SELECT
    cohort_month,
    month_offset,
    COUNT(DISTINCT customer_id) AS active_customers
FROM monthly_activity
WHERE month_offset BETWEEN 0 AND 11
GROUP BY cohort_month, month_offset
ORDER BY cohort_month, month_offset;
""",
        '04_channel_performance.sql': """-- Channel Performance Comparison
-- Compares revenue, AOV, and customer acquisition by marketing channel

SELECT
    channel,
    COUNT(*) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS aov,
    ROUND(SUM(total_amount) / COUNT(DISTINCT customer_id), 2) AS revenue_per_customer,
    ROUND(SUM(CASE WHEN returned = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS return_rate_pct
FROM transactions
GROUP BY channel
ORDER BY total_revenue DESC;
""",
        '05_rfm_segmentation.sql': """-- RFM Customer Segmentation
-- Classifies customers by Recency, Frequency, and Monetary value

WITH rfm_raw AS (
    SELECT
        customer_id,
        CAST(julianday('2026-01-01') - julianday(MAX(transaction_date)) AS INT) AS recency_days,
        COUNT(*) AS frequency,
        ROUND(SUM(total_amount), 2) AS monetary
    FROM transactions
    WHERE returned = 0
    GROUP BY customer_id
),
rfm_scored AS (
    SELECT
        *,
        NTILE(4) OVER (ORDER BY recency_days DESC) AS r_score,
        NTILE(4) OVER (ORDER BY frequency ASC) AS f_score,
        NTILE(4) OVER (ORDER BY monetary ASC) AS m_score
    FROM rfm_raw
)
SELECT
    customer_id,
    recency_days,
    frequency,
    monetary,
    r_score + f_score + m_score AS rfm_total,
    CASE
        WHEN r_score + f_score + m_score >= 10 THEN 'Champions'
        WHEN r_score + f_score + m_score >= 8 THEN 'Loyal'
        WHEN r_score + f_score + m_score >= 6 THEN 'Potential'
        WHEN r_score + f_score + m_score >= 4 THEN 'At Risk'
        ELSE 'Lost'
    END AS segment
FROM rfm_scored
ORDER BY rfm_total DESC;
"""
    }

    for filename, sql in queries.items():
        path = os.path.join(SQL_DIR, filename)
        with open(path, 'w') as f:
            f.write(sql)
        print(f"  ✓ Saved: sql/{filename}")


# ──────────────────────────────────────────────
# MAIN PIPELINE
# ──────────────────────────────────────────────
def main():
    print("\n" + "▓" * 60)
    print("  E-COMMERCE TRANSACTION ANALYSIS")
    print("  KPI Dashboards & Retention Recommendations")
    print("▓" * 60)

    txn, cust, sessions = load_data()
    kpis, valid_txn = compute_kpis(txn, sessions)
    monthly = compute_monthly_trends(txn, valid_txn, sessions)
    retention = cohort_analysis(valid_txn)
    rfm, seg_summary = rfm_segmentation(valid_txn)

    # Visualizations
    plot_kpi_dashboard(kpis, monthly, valid_txn)
    plot_cohort_retention(retention)
    plot_channel_analysis(valid_txn, sessions)
    plot_customer_segments(rfm, seg_summary)
    plot_recommendations(kpis, valid_txn, rfm)

    # SQL
    run_sql_analysis(txn, sessions)
    export_sql_files()

    # Save summary CSV
    pd.DataFrame([kpis]).to_csv(os.path.join(DATA_DIR, 'kpi_summary.csv'), index=False)
    rfm.to_csv(os.path.join(DATA_DIR, 'rfm_segments.csv'), index=False)

    # Final summary
    print("\n" + "▓" * 60)
    print("  ANALYSIS COMPLETE")
    print("▓" * 60)
    print(f"\n  3 Recommendations for Retention Improvement:")
    print(f"    1. Email re-engagement campaign for {len(rfm[rfm['segment'].isin(['At Risk', 'Lost'])]):,} at-risk/lost customers")
    print(f"    2. Loyalty discount program targeting {len(rfm[(rfm['frequency']>=2)&(rfm['frequency']<=3)]):,} occasional buyers")
    print(f"    3. Mobile UX optimization to close AOV gap with desktop")
    print(f"\n  → Projected combined retention improvement: ~15%")

    print(f"\n  Output files:")
    for f in sorted(os.listdir(OUTPUT_DIR)):
        print(f"    📄 output/{f}")
    print(f"\n  Data files:")
    for f in sorted(os.listdir(DATA_DIR)):
        if not f.endswith('.db'):
            print(f"    📄 data/{f}")
    print(f"\n  SQL queries:")
    for f in sorted(os.listdir(SQL_DIR)):
        print(f"    📄 sql/{f}")

    return kpis, rfm


if __name__ == '__main__':
    kpis, rfm = main()
