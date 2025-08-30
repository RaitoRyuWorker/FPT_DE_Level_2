import pandas as pd
import random
from faker import Faker
import datetime

fake = Faker()

# Set seed for reproducible results
random.seed(42)
fake.seed_instance(42)

def generate_customers(n=100):
    customers = []
    
    for i in range(n):
        name = fake.name()
        customer_id = fake.uuid4()
        
        # Create various email quality issues
        email_quality = random.random()
        
        if email_quality < 0.15:  # 15% - completely missing emails
            email = ""
        elif email_quality < 0.25:  # 10% - invalid email patterns
            email = random.choice([
                "invalid_email",
                "user@.com",  # missing domain
                "user@",      # incomplete
                "@example.com", # missing username
                "user..name@example.com", # double dots
                "user name@example.com",  # spaces in username
                "user@example..com",      # double dots in domain
                "user@.example.com",      # leading dot in domain
                "user@example.com.",      # trailing dot in domain
                "user@-example.com",      # leading hyphen in domain
                "user@example-.com"       # trailing hyphen in domain
            ])
        elif email_quality < 0.35:  # 10% - malformed but plausible emails
            email = random.choice([
                "user@example",           # missing TLD
                "user@example.c",         # incomplete TLD
                "user@example.co.uk",     # valid but different format
                "user+tag@example.com",   # valid with plus
                "user.tag@example.com",   # valid with dot
                "user_tag@example.com"    # valid with underscore
            ])
        else:  # 65% - valid emails
            email = fake.email()
        
        customers.append({
            "customer_id": customer_id,
            "name": name,
            "email": email
        })
    
    return pd.DataFrame(customers)

def generate_products(n=20):
    categories = [
        "Electronics", "electronics", "ELECTRONICS", "Electronics", "electronics",
        "Books", "books", "BOOKS", "Books", "books", 
        "Home", "home", "HOME", "Home", "home",
        "Electronics", "electronics", "Electronics", "electronics", "Electronics"
    ]
    
    products = []
    for i in range(1, n+1):
        name = fake.word().capitalize()
        category = random.choice(categories)
        price = round(random.uniform(5, 1000), 2)
        
        products.append({
            "product_id": i,
            "name": name,
            "category": category,
            "price": price
        })
    
    return pd.DataFrame(products)

def generate_transactions(customers_df, products_df, n=200):
    transactions = []
    customer_ids = customers_df["customer_id"].tolist()
    product_ids = products_df["product_id"].tolist()
    
    # Generate some base transactions
    for i in range(n):
        customer_id = random.choice(customer_ids)
        product_id = random.choice(product_ids)
        amount = round(random.uniform(10, 500), 2)
        
        # Create various date quality issues
        date_quality = random.random()
        
        if date_quality < 0.10:  # 10% - completely missing dates
            date = ""
        elif date_quality < 0.20:  # 10% - invalid date formats
            date = random.choice([
                "2024-13-45",    # invalid month and day
                "2023-02-30",    # February 30th doesn't exist
                "2024-04-31",    # April 31st doesn't exist
                "2023-06-31",    # June 31st doesn't exist
                "2024-09-31",    # September 31st doesn't exist
                "2023-11-31",    # November 31st doesn't exist
                "2024-02-29",    # February 29th in non-leap year
                "2023-02-29",    # February 29th in non-leap year
                "2024-00-15",    # month 0
                "2024-13-01",    # month 13
                "2024-01-00",    # day 0
                "2024-01-32",    # day 32
                "invalid_date",  # completely invalid
                "2024/01/15",    # wrong separator
                "15-01-2024",    # wrong order
                "01-15-2024",    # wrong order
                "2024.01.15",    # wrong separator
                "2024 01 15"     # wrong separator
            ])
        elif date_quality < 0.30:  # 10% - dates in wrong format
            date = random.choice([
                "2024-1-5",      # missing leading zeros
                "24-01-15",      # 2-digit year
                "2024-1-15",     # missing leading zero in month
                "2024-01-5",     # missing leading zero in day
                "2024-1-5",      # missing leading zeros in both
                "24-1-5"         # 2-digit year and missing leading zeros
            ])
        elif date_quality < 0.40:  # 10% - future dates (unrealistic for historical data)
            future_date = datetime.datetime.now() + datetime.timedelta(days=random.randint(1, 365))
            date = future_date.strftime("%Y-%m-%d")
        elif date_quality < 0.50:  # 10% - very old dates (unrealistic)
            old_date = datetime.datetime(1800, random.randint(1, 12), random.randint(1, 28))
            date = old_date.strftime("%Y-%m-%d")
        else:  # 50% - valid dates
            date = fake.date_between(start_date='-10y', end_date='today').strftime("%Y-%m-%d")
        
        entry = {
            "transaction_id": fake.uuid4(),
            "customer_id": customer_id,
            "product_id": product_id,
            "amount": amount,
            "date": date
        }
        transactions.append(entry)
        
        # Create duplicate entries (5% chance)
        if random.random() < 0.05:
            # Exact duplicate
            transactions.append(entry)
        elif random.random() < 0.03:
            # Partial duplicate (same customer, product, amount, different date)
            duplicate_entry = entry.copy()
            duplicate_entry["transaction_id"] = fake.uuid4()
            duplicate_entry["date"] = fake.date_between(start_date='-10y', end_date='today').strftime("%Y-%m-%d")
            transactions.append(duplicate_entry)
    
    return pd.DataFrame(transactions)

def main():
    print("Generating realistic data with data quality issues...")
    print("=" * 50)
    
    # Generate data
    customers_df = generate_customers(100)
    products_df = generate_products(20)
    transactions_df = generate_transactions(customers_df, products_df, 200)
    
    # Save to CSV files
    customers_df.to_csv("customers.csv", index=False)
    products_df.to_csv("products.csv", index=False)
    transactions_df.to_csv("transactions.csv", index=False)
    
    print("Data generation completed!")
    print(f"Generated {len(customers_df)} customers")
    print(f"Generated {len(products_df)} products")
    print(f"Generated {len(transactions_df)} transactions")
    
    # Show data quality issues summary
    print("Data Quality Issues Summary:")
    print("-" * 50)
    
    # Customer issues
    invalid_emails = customers_df[customers_df['email'].isin(['', 'invalid_email', 'user@.com', 'user@', '@example.com', 'user..name@example.com', 'user name@example.com', 'user@example..com', 'user@.example.com', 'user@example.com.', 'user@-example.com', 'user@example-.com'])]
    missing_emails = customers_df[customers_df['email'] == '']
    print(f"  Customers with invalid emails: {len(invalid_emails)}")
    print(f"  Customers with missing emails: {len(missing_emails)}")
    
    # Product issues
    inconsistent_categories = products_df['category'].value_counts()
    print(f"  Product category variations: {len(inconsistent_categories)}")
    print(f"  Category distribution: {dict(inconsistent_categories)}")
    
    # Transaction issues
    invalid_dates = transactions_df[transactions_df['date'].isin(['', '2024-13-45', '2023-02-30', '2024-04-31', '2023-06-31', '2024-09-31', '2023-11-31', '2024-02-29', '2023-02-29', '2024-00-15', '2024-13-01', '2024-01-00', '2024-01-32', 'invalid_date', '2024/01/15', '15-01-2024', '01-15-2024', '2024.01.15', '2024 01 15'])]
    missing_dates = transactions_df[transactions_df['date'] == '']
    print(f"  Transactions with invalid dates: {len(invalid_dates)}")
    print(f"  Transactions with missing dates: {len(missing_dates)}")
    
    # Check for duplicates
    duplicate_transactions = transactions_df.duplicated().sum()
    print(f"  Duplicate transactions: {duplicate_transactions}")
    
    print("Files saved:")
    print("  - customers.csv")
    print("  - products.csv") 
    print("  - transactions.csv")
    
    print("This data now contains realistic data quality issues that will test your ETL pipeline!")

if __name__ == "__main__":
    main()
