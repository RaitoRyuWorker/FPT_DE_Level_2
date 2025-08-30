import pandas as pd
import sqlite3
import numpy as np
import re
from datetime import datetime
import warnings
import os
warnings.filterwarnings('ignore')

class ETLPipeline:
    def __init__(self, db_path='healthcare_data.db'):
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        
    def connect_database(self):
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            print(f"Connected to database: {self.db_path}")
        except Exception as e:
            print(f"Error connecting to database: {e}")
            raise
    
    def create_tables(self):
        try:
            # Create customers table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS customers (
                    customer_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create products table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS products (
                    product_id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    category TEXT NOT NULL,
                    price REAL NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create transactions table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id TEXT PRIMARY KEY,
                    customer_id TEXT NOT NULL,
                    product_id INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    transaction_date DATE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (customer_id) REFERENCES customers (customer_id),
                    FOREIGN KEY (product_id) REFERENCES products (product_id)
                )
            ''')
            
            self.conn.commit()
            print("Database tables created")
            
        except Exception as e:
            print(f"Error creating tables: {e}")
            raise
    
    def extract_data(self):
        print("Extract data from CSV files...")
        try:
            customers_df = pd.read_csv('customers.csv')
            products_df = pd.read_csv('products.csv')
            transactions_df = pd.read_csv('transactions.csv')
            print(f"Raw data: {len(customers_df)} customers, {len(products_df)} products, {len(transactions_df)} transactions")
            return customers_df, products_df, transactions_df
            
        except Exception as e:
            print(f"Error extracting data: {e}")
            raise
    
    def transform_customers(self, df):
        print("Transform customers data...")
        
        initial_count = len(df)
        
        # Remove rows with completely empty emails
        df = df.dropna(subset=['email'])
        
        # Remove rows with invalid email patterns (expanded list)
        invalid_patterns = [
            'invalid_email', 'user@.com', 'user@', '@example.com', 
            'user..name@example.com', 'user name@example.com', 'user@example..com',
            'user@.example.com', 'user@example.com.', 'user@-example.com', 'user@example-.com'
        ]
        df = df[~df['email'].isin(invalid_patterns)]
        
        # Enhanced email validation using regex
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        df = df[df['email'].str.match(email_pattern, na=False)]
        
        # Remove duplicates based on email
        df = df.drop_duplicates(subset=['email'], keep='first')
        
        # Remove duplicates based on customer_id
        df = df.drop_duplicates(subset=['customer_id'], keep='first')
        
        # Ensure required columns exist
        if 'name' not in df.columns:
            df['name'] = 'Unknown'
        
        final_count = len(df)
        
        print(f"After transform: {final_count} customers")
        
        return df
    
    def transform_products(self, df):
        print("Transform products data...")
                
        # Enhanced category standardization (handle more variations)
        category_mapping = {
            'electronics': 'Electronics',
            'ELECTRONICS': 'Electronics',
            'books': 'Books',
            'BOOKS': 'Books',
            'home': 'Home',
            'HOME': 'Home'
        }
        
        # Apply standardization
        df['category'] = df['category'].str.lower().map(category_mapping).fillna(df['category'])
        
        # Remove duplicates based on product_id
        df = df.drop_duplicates(subset=['product_id'], keep='first')
        
        # Remove duplicates based on name and category combination
        df = df.drop_duplicates(subset=['name', 'category'], keep='first')
        
        # Ensure price is numeric and reasonable
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df = df.dropna(subset=['price'])
        
        # Additional price validation (reasonable range)
        df = df[(df['price'] > 0) & (df['price'] <= 10000)]  # Reasonable price range
        
        final_count = len(df)
        
        print(f"After transform: {final_count} products")
        
        return df
    
    def transform_transactions(self, df):
        print("Transform transactions data...")
                
        # Remove completely empty rows
        df = df.dropna(how='all')
        
        # Enhanced date validation with detailed reporting
        def validate_date(date_str):
            if pd.isna(date_str) or date_str == '':
                return None
            try:
                # Try to parse the date with various formats
                parsed_date = pd.to_datetime(date_str, errors='coerce')
                if pd.isna(parsed_date):
                    return None
                # Check if date is reasonable (not too far in past/future)
                if parsed_date.year < 1900 or parsed_date.year > 2100:
                    return None
                return parsed_date
            except:
                return None
        
        # Apply date validation
        df['date'] = df['date'].apply(validate_date)
        df = df.dropna(subset=['date'])
        
        # Enhanced duplicate detection
        df = df.drop_duplicates(keep='first')
        df = df.drop_duplicates(subset=['transaction_id'], keep='first')
        
        # Enhanced amount validation
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        df = df.dropna(subset=['amount'])
        
        # Additional amount validation (reasonable range)
        df = df[(df['amount'] > 0) & (df['amount'] <= 10000)]  # Reasonable amount range
        
        # Rename date column for database and convert to string format
        df = df.rename(columns={'date': 'transaction_date'})
        
        # Convert datetime to string format for SQLite
        df['transaction_date'] = df['transaction_date'].dt.strftime('%Y-%m-%d')
        
        final_count = len(df)
        
        print(f"After transform: {final_count} transactions")
        
        return df
    
    def validate_referential_integrity(self, customers_df, products_df, transactions_df):
        print("Validating referential integrity...")
        
        # Get valid customer_ids and product_ids
        valid_customer_ids = set(customers_df['customer_id'])
        valid_product_ids = set(products_df['product_id'])
        
        # Filter transactions to only include valid references
        transactions_df = transactions_df[
            (transactions_df['customer_id'].isin(valid_customer_ids)) &
            (transactions_df['product_id'].isin(valid_product_ids))
        ]
        
        final_count = len(transactions_df)
        
        print(f"After referential integrity check: {final_count} transactions")
        
        return transactions_df
    
    def load_customers(self, df):
        print("Loading customers data...")
        
        try:
            # Clear existing data
            self.cursor.execute('DELETE FROM customers')
            
            # Insert new data
            for _, row in df.iterrows():
                self.cursor.execute('''
                    INSERT INTO customers (customer_id, name, email)
                    VALUES (?, ?, ?)
                ''', (row['customer_id'], row['name'], row['email']))
            
            self.conn.commit()
            print(f"Loaded: {len(df)} customers")
            
        except Exception as e:
            print(f"Error loading customers: {e}")
            raise
    
    def load_products(self, df):
        print("Loading products data...")
        
        try:
            # Clear existing data
            self.cursor.execute('DELETE FROM products')
            
            # Insert new data
            for _, row in df.iterrows():
                self.cursor.execute('''
                    INSERT INTO products (product_id, name, category, price)
                    VALUES (?, ?, ?, ?)
                ''', (row['product_id'], row['name'], row['category'], row['price']))
            
            self.conn.commit()
            print(f"Loaded: {len(df)} products")
            
        except Exception as e:
            print(f"Error loading products: {e}")
            raise
    
    def load_transactions(self, df):
        print("Loading transactions data...")
        
        try:
            # Clear existing data
            self.cursor.execute('DELETE FROM transactions')
            
            # Insert new data
            for _, row in df.iterrows():
                self.cursor.execute('''
                    INSERT INTO transactions (transaction_id, customer_id, product_id, amount, transaction_date)
                    VALUES (?, ?, ?, ?, ?)
                ''', (row['transaction_id'], row['customer_id'], row['product_id'], 
                      row['amount'], row['transaction_date']))
            
            self.conn.commit()
            print(f"Loaded: {len(df)} transactions")
            
        except Exception as e:
            print(f"Error loading transactions: {e}")
            raise
    
    def verify_data_loaded(self):
        print("Verifying data in database...")
        
        try:
            # Check record counts
            self.cursor.execute('SELECT COUNT(*) FROM customers')
            customer_count = self.cursor.fetchone()[0]
            
            self.cursor.execute('SELECT COUNT(*) FROM products')
            product_count = self.cursor.fetchone()[0]
            
            self.cursor.execute('SELECT COUNT(*) FROM transactions')
            transaction_count = self.cursor.fetchone()[0]
            
            print(f"Database counts: {customer_count} customers, {product_count} products, {transaction_count} transactions")
            
            return {
                'customers': customer_count,
                'products': product_count,
                'transactions': transaction_count
            }
                
        except Exception as e:
            print(f"Error verifying data: {e}")
            raise
    
    def check_etl_pipeline(self, transformed_counts, loaded_counts):
        print("\n" + "=" * 50)
        print("ETL PIPELINE VALIDATION CHECK")
        print("=" * 50)
        
        # Check if transformed counts match loaded counts
        customer_match = transformed_counts['customers'] == loaded_counts['customers']
        product_match = transformed_counts['products'] == loaded_counts['products']
        transaction_match = transformed_counts['transactions'] == loaded_counts['transactions']
        
        print(f"Customers: {transformed_counts['customers']} transformed vs {loaded_counts['customers']} loaded - {'PASS' if customer_match else 'FAIL'}")
        print(f"Products: {transformed_counts['products']} transformed vs {loaded_counts['products']} loaded - {'PASS' if product_match else 'FAIL'}")
        print(f"Transactions: {transformed_counts['transactions']} transformed vs {loaded_counts['transactions']} loaded - {'PASS' if transaction_match else 'FAIL'}")
        
        # Overall pipeline status
        if customer_match and product_match and transaction_match:
            print("\nETL Pipeline Status: PASS - All data loaded correctly")
        else:
            print("\nETL Pipeline Status: FAIL - Data mismatch detected")
            print("Pipeline is broken - investigate the loading process")
        
        print("=" * 50)
    
    def run_pipeline(self):
        print("Starting ETL Pipeline...")
        print("=" * 50)
        
        try:
            # Step 1: Connect to database
            self.connect_database()
            
            # Step 2: Create tables
            self.create_tables()
            
            # Step 3: Extract data
            customers_df, products_df, transactions_df = self.extract_data()
            
            # Step 4: Transform data
            customers_clean = self.transform_customers(customers_df.copy())
            products_clean = self.transform_products(products_df.copy())
            transactions_clean = self.transform_transactions(transactions_df.copy())
            
            # Step 5: Validate referential integrity
            transactions_clean = self.validate_referential_integrity(
                customers_clean, products_clean, transactions_clean
            )
            
            # Store transformed counts for validation
            transformed_counts = {
                'customers': len(customers_clean),
                'products': len(products_clean),
                'transactions': len(transactions_clean)
            }
            
            # Step 6: Load data into database
            self.load_customers(customers_clean)
            self.load_products(products_clean)
            self.load_transactions(transactions_clean)
            
            # Step 7: Verify data and get loaded counts
            loaded_counts = self.verify_data_loaded()
            
            # Step 8: Check ETL pipeline integrity
            self.check_etl_pipeline(transformed_counts, loaded_counts)
            
            print("ETL Pipeline completed!")
            print("=" * 50)
            
        except Exception as e:
            print(f"ETL Pipeline failed: {e}")
            raise
        
        finally:
            if self.conn:
                self.conn.close()
                print("Database connection closed")

def main():
    pipeline = ETLPipeline()
    pipeline.run_pipeline()

if __name__ == "__main__":
    main()
