import csv
import random
from datetime import datetime, timedelta

random.seed(42)

branches = [
    ("BR_NYC_01", "New York - Manhattan"),
    ("BR_CHI_02", "Chicago - Downtown"),
    ("BR_SFO_03", "San Francisco - Bay Area"),
    ("BR_SEA_04", "Seattle - Central"),
    ("BR_BOS_05", "Boston - Back Bay")
]

products = [
    # (product_id, product_name, category, unit_cost, unit_price)
    ("PROD_001", "Organic Whole Milk 1 Gal", "Dairy & Eggs", 3.20, 4.99),
    ("PROD_002", "Farm Fresh Large Brown Eggs (12pk)", "Dairy & Eggs", 2.10, 3.89),
    ("PROD_003", "Greek Plain Yogurt 32oz", "Dairy & Eggs", 3.00, 5.49),
    ("PROD_004", "Organic Hass Avocados (4pk)", "Produce", 2.50, 4.49),
    ("PROD_005", "Cavendish Bananas (per lb)", "Produce", 0.35, 0.69),
    ("PROD_006", "Honeycrisp Apples (per lb)", "Produce", 1.20, 2.79),
    ("PROD_007", "Organic Baby Spinach 16oz", "Produce", 2.00, 3.99),
    ("PROD_008", "Whole Grain Sourdough Bread", "Bakery", 1.80, 4.29),
    ("PROD_009", "Extra Virgin Olive Oil 1L", "Pantry & Staples", 8.50, 14.99),
    ("PROD_010", "Basmati Rice 5kg", "Pantry & Staples", 7.00, 12.99),
    ("PROD_011", "Artisan Roasted Coffee Beans 12oz", "Beverages", 6.50, 13.49),
    ("PROD_012", "Cold-Pressed Orange Juice 52oz", "Beverages", 2.80, 5.29),
    ("PROD_013", "Sparkling Spring Water (12pk)", "Beverages", 3.20, 6.49),
    ("PROD_014", "Organic Dark Chocolate 85%", "Snacks & Sweets", 1.50, 3.49),
    ("PROD_015", "Roasted Salted Almonds 16oz", "Snacks & Sweets", 4.50, 8.99),
    ("PROD_016", "Eco-Friendly Laundry Detergent 100oz", "Household", 9.00, 16.99),
    ("PROD_017", "Antibacterial Disinfecting Wipes (3pk)", "Household", 4.20, 8.49),
    ("PROD_018", "Citrus Concentrated Dish Soap 28oz", "Household", 1.80, 3.79),
    ("PROD_019", "Wild Alaskan Salmon Fillet (per lb)", "Meat & Seafood", 8.00, 15.99),
    ("PROD_020", "Boneless Chicken Breast (per lb)", "Meat & Seafood", 2.80, 5.49)
]

# Weighting hours to reflect realistic peak retail shopping patterns:
# High between 11-13 (lunch) and 17-20 (evening post-work rush)
hour_weights = [
    0.01, 0.005, 0.005, 0.005, 0.005, 0.01,   # 00:00 - 05:00 (closed/minimal)
    0.02, 0.05, 0.07, 0.06, 0.07, 0.09,       # 06:00 - 11:00
    0.09, 0.07, 0.06, 0.06, 0.08, 0.12,       # 12:00 - 17:00
    0.13, 0.11, 0.06, 0.04, 0.02, 0.01        # 18:00 - 23:00
]
hours_pool = list(range(24))

start_date = datetime(2024, 1, 1)
total_days = 90  # 3 months: Jan, Feb, Mar 2024

output_file = "C:/Users/Aditya Mundra/.gemini/antigravity/scratch/big_data_systems_24ci51/activity_1_hive/supermarket_sales.csv"

rows = []
transaction_counter = 100000

for day_offset in range(total_days):
    current_date = start_date + timedelta(days=day_offset)
    # Daily transactions: ~150-200 per day across 5 branches
    daily_tx_count = random.randint(140, 180)
    for _ in range(daily_tx_count):
        transaction_counter += 1
        tx_id = f"TXN_{transaction_counter}"
        branch_id, branch_name = random.choice(branches)
        chosen_hour = random.choices(hours_pool, weights=hour_weights, k=1)[0]
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        tx_timestamp = current_date.replace(hour=chosen_hour, minute=minute, second=second)
        
        # Basket size: 1 to 4 distinct items per transaction
        basket_items = random.sample(products, k=random.randint(1, 4))
        for item in basket_items:
            prod_id, prod_name, category, unit_cost, unit_price = item
            quantity = random.choices([1, 2, 3, 4, 5], weights=[0.55, 0.25, 0.12, 0.05, 0.03])[0]
            line_total = round(quantity * unit_price, 2)
            payment_method = random.choices(["Credit Card", "Mobile Pay", "Debit Card", "Cash"], weights=[0.50, 0.30, 0.15, 0.05])[0]
            
            rows.append([
                tx_id,
                tx_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                tx_timestamp.strftime("%Y-%m-%d"),
                tx_timestamp.hour,
                branch_id,
                branch_name,
                prod_id,
                prod_name,
                category,
                quantity,
                unit_price,
                line_total,
                payment_method
            ])

with open(output_file, mode="w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "transaction_id", "transaction_timestamp", "sale_date", "sale_hour",
        "branch_id", "branch_name", "product_id", "product_name",
        "category", "quantity", "unit_price", "total_amount", "payment_method"
    ])
    writer.writerows(rows)

print(f"Dataset generated successfully with {len(rows)} records at: {output_file}")
