import sqlite3
import pandas as pd

def create_customer_revenue_table():
    conn = sqlite3.connect('retail_data.db')
    cursor = conn.cursor()
    
    print("Creating customer_revenue table...")
    print("=" * 50)
    
    try:
        # Step 1: Create the customer_revenue table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS customer_revenue (
                customer_id TEXT PRIMARY KEY,
                customer_name TEXT NOT NULL,
                customer_email TEXT NOT NULL,
                total_transaction_amount REAL NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
            )
        ''')
        
        # Step 2: Calculate and insert aggregated data
        cursor.execute('''
            INSERT OR REPLACE INTO customer_revenue (
                customer_id,
                customer_name,
                customer_email,
                total_transaction_amount
            )
            SELECT 
                c.customer_id,
                c.name as customer_name,
                c.email as customer_email,
                COALESCE(SUM(t.amount), 0.0) as total_transaction_amount
            FROM customers c
            LEFT JOIN transactions t ON c.customer_id = t.customer_id
            GROUP BY c.customer_id, c.name, c.email
            ORDER BY total_transaction_amount DESC
        ''')
        conn.commit()
        
        # Step 3: Verify the data was inserted
        cursor.execute('SELECT COUNT(*) FROM customer_revenue')
        count = cursor.fetchone()[0]
        
        print(f"Customer revenue table created successfully!")
        print(f"Total records inserted: {count}")
        
        print("\nSample customer revenue data:")
        print("-" * 40)
        
        sample_query = '''
            SELECT customer_name, customer_email, total_transaction_amount
            FROM customer_revenue
            ORDER BY total_transaction_amount DESC
            LIMIT 5
        '''
        
        sample_data = pd.read_sql_query(sample_query, conn)
        for _, row in sample_data.iterrows():
            print(f"  {row['customer_name']}: ${row['total_transaction_amount']:.2f}")
        
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
        raise
    
    finally:
        conn.close()
    
    print("\n" + "=" * 50)
    print("Customer revenue table creation completed!")
    print("=" * 50)

if __name__ == "__main__":
    create_customer_revenue_table()
