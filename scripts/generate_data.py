"""
Generate realistic e-commerce transaction data (10,000+ records).
Simulates an online retail store with realistic patterns:
  - Seasonal trends (holiday spikes)
  - Customer segments (one-time vs loyal)
  - Product categories with varying price ranges
  - Realistic conversion and repurchase behavior
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

np.random.seed(42)

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────
N_CUSTOMERS = 3200
N_TRANSACTIONS = 12500
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2025, 12, 31)

CATEGORIES = {
    'Electronics':      {'price_range': (29.99, 499.99), 'weight': 0.20},
    'Clothing':         {'price_range': (14.99, 149.99), 'weight': 0.25},
    'Home & Kitchen':   {'price_range': (9.99, 199.99),  'weight': 0.18},
    'Beauty & Health':  {'price_range': (7.99, 89.99),   'weight': 0.15},
    'Sports & Outdoor': {'price_range': (12.99, 249.99), 'weight': 0.10},
    'Books & Media':    {'price_range': (4.99, 39.99),   'weight': 0.07},
    'Toys & Games':     {'price_range': (9.99, 79.99),   'weight': 0.05},
}

CHANNELS = {'Organic Search': 0.30, 'Paid Ads': 0.25, 'Social Media': 0.20,
            'Email': 0.15, 'Direct': 0.10}

DEVICES = {'Mobile': 0.55, 'Desktop': 0.35, 'Tablet': 0.10}

REGIONS = {'Northeast': 0.22, 'Southeast': 0.20, 'Midwest': 0.18,
           'West': 0.25, 'Southwest': 0.15}

PAYMENT_METHODS = {'Credit Card': 0.45, 'PayPal': 0.25, 'Debit Card': 0.20,
                   'Apple Pay': 0.10}

# ──────────────────────────────────────────────
# Generate Data
# ──────────────────────────────────────────────
print("Generating e-commerce transaction data...")

# 1. Customer profiles
customer_ids = [f"CUST_{i:05d}" for i in range(1, N_CUSTOMERS + 1)]
customer_loyalty = np.random.choice(['One-time', 'Occasional', 'Loyal'],
                                     size=N_CUSTOMERS, p=[0.45, 0.35, 0.20])
customer_regions = np.random.choice(list(REGIONS.keys()), size=N_CUSTOMERS,
                                     p=list(REGIONS.values()))

customers_df = pd.DataFrame({
    'customer_id': customer_ids,
    'loyalty_segment': customer_loyalty,
    'region': customer_regions,
    'signup_date': [START_DATE + timedelta(days=np.random.randint(0, 365))
                    for _ in range(N_CUSTOMERS)]
})

# 2. Generate transactions
transactions = []
for i in range(N_TRANSACTIONS):
    # Assign customer based on loyalty (loyal customers buy more)
    loyalty_weights = customers_df['loyalty_segment'].map(
        {'One-time': 0.5, 'Occasional': 1.5, 'Loyal': 4.0}
    ).values.copy()
    loyalty_weights = loyalty_weights / loyalty_weights.sum()
    cust_idx = np.random.choice(N_CUSTOMERS, p=loyalty_weights)
    cust = customers_df.iloc[cust_idx]

    # Date with seasonal pattern (more in Nov-Dec, less in Jan-Feb)
    day_offset = np.random.randint(0, (END_DATE - START_DATE).days)
    txn_date = START_DATE + timedelta(days=day_offset)
    month = txn_date.month
    # Seasonal adjustment: boost holiday months
    if month in [11, 12]:
        if np.random.random() < 0.3:  # 30% extra chance to keep
            pass
        day_offset = np.random.randint(0, (END_DATE - START_DATE).days)
        txn_date = START_DATE + timedelta(days=day_offset)

    # Category
    cat = np.random.choice(list(CATEGORIES.keys()),
                           p=[v['weight'] for v in CATEGORIES.values()])
    price_lo, price_hi = CATEGORIES[cat]['price_range']
    unit_price = round(np.random.uniform(price_lo, price_hi), 2)

    # Quantity (most buy 1, some buy more)
    qty = np.random.choice([1, 2, 3, 4, 5], p=[0.55, 0.25, 0.12, 0.05, 0.03])

    # Discount (loyal customers get more discounts)
    if cust['loyalty_segment'] == 'Loyal':
        discount = np.random.choice([0, 0.05, 0.10, 0.15, 0.20],
                                     p=[0.3, 0.2, 0.25, 0.15, 0.10])
    elif cust['loyalty_segment'] == 'Occasional':
        discount = np.random.choice([0, 0.05, 0.10, 0.15],
                                     p=[0.5, 0.25, 0.15, 0.10])
    else:
        discount = np.random.choice([0, 0.05, 0.10], p=[0.7, 0.2, 0.1])

    total = round(unit_price * qty * (1 - discount), 2)

    # Channel, device, payment
    channel = np.random.choice(list(CHANNELS.keys()), p=list(CHANNELS.values()))
    device = np.random.choice(list(DEVICES.keys()), p=list(DEVICES.values()))
    payment = np.random.choice(list(PAYMENT_METHODS.keys()),
                                p=list(PAYMENT_METHODS.values()))

    # Returned? (8% return rate, higher for electronics)
    return_rate = 0.12 if cat == 'Electronics' else 0.06
    returned = np.random.random() < return_rate

    transactions.append({
        'transaction_id': f"TXN_{i+1:06d}",
        'customer_id': cust['customer_id'],
        'transaction_date': txn_date.strftime('%Y-%m-%d'),
        'category': cat,
        'unit_price': unit_price,
        'quantity': qty,
        'discount_pct': discount,
        'total_amount': total,
        'channel': channel,
        'device': device,
        'payment_method': payment,
        'region': cust['region'],
        'returned': returned,
    })

txn_df = pd.DataFrame(transactions)
txn_df['transaction_date'] = pd.to_datetime(txn_df['transaction_date'])
txn_df = txn_df.sort_values('transaction_date').reset_index(drop=True)

# Also generate site visit / session data for conversion rate
n_sessions = int(N_TRANSACTIONS / 0.032)  # ~3.2% conversion rate
sessions = []
for i in range(n_sessions):
    day_offset = np.random.randint(0, (END_DATE - START_DATE).days)
    visit_date = START_DATE + timedelta(days=day_offset)
    device = np.random.choice(list(DEVICES.keys()), p=list(DEVICES.values()))
    channel = np.random.choice(list(CHANNELS.keys()), p=list(CHANNELS.values()))
    converted = np.random.random() < 0.032

    sessions.append({
        'session_id': f"SESS_{i+1:07d}",
        'visit_date': visit_date.strftime('%Y-%m-%d'),
        'device': device,
        'channel': channel,
        'pages_viewed': np.random.choice(range(1, 20), p=np.array([30,20,12,8,6,5,4,3,2,2,1.5,1.5,1,1,0.8,0.5,0.5,0.4,0.3])/np.array([30,20,12,8,6,5,4,3,2,2,1.5,1.5,1,1,0.8,0.5,0.5,0.4,0.3]).sum()),
        'converted': converted,
    })

sessions_df = pd.DataFrame(sessions)

# Save
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
os.makedirs(DATA_DIR, exist_ok=True)

txn_df.to_csv(os.path.join(DATA_DIR, 'transactions.csv'), index=False)
customers_df.to_csv(os.path.join(DATA_DIR, 'customers.csv'), index=False)
sessions_df.to_csv(os.path.join(DATA_DIR, 'sessions.csv'), index=False)

print(f"  ✓ {len(txn_df):,} transactions generated")
print(f"  ✓ {len(customers_df):,} customers")
print(f"  ✓ {len(sessions_df):,} site sessions")
print(f"  ✓ Saved to data/")
