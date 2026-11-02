"""sql_analysis.py

Executes the business analysis SQL queries on cleaned_food_orders table.
Outputs the results clearly for reporting.
"""

from config import get_db_connection, ANALYTICS_TABLE_NAME


def run_sql_analysis():
    """Runs business SQL analysis queries on the analytics database."""
    conn, engine = get_db_connection(target="analytics")
    cursor = conn.cursor()

    print("\n" + "=" * 80)
    print("           BUSINESS ANALYSIS USING SQL (cleaned_food_orders)")
    print("=" * 80)

    try:
        # Q1: Total Order Value
        cursor.execute(f"SELECT ROUND(SUM(order_value), 2) FROM {ANALYTICS_TABLE_NAME} WHERE order_value IS NOT NULL;")
        total_ov = cursor.fetchone()[0]
        print(f"1. Total Order Value:                 ${total_ov:,.2f}")

        # Q2: Average Order Value
        cursor.execute(f"SELECT ROUND(AVG(order_value), 2) FROM {ANALYTICS_TABLE_NAME} WHERE order_value IS NOT NULL;")
        avg_ov = cursor.fetchone()[0]
        print(f"2. Average Order Value:               ${avg_ov:,.2f}")

        # Q3: Top Payment Method
        cursor.execute(f"""
            SELECT payment_method, COUNT(*) AS cnt 
            FROM {ANALYTICS_TABLE_NAME} 
            WHERE payment_method != 'Unknown' 
            GROUP BY payment_method 
            ORDER BY cnt DESC LIMIT 1;
        """)
        top_pm = cursor.fetchone()
        print(f"3. Top Payment Method (known):        {top_pm[0]} ({top_pm[1]} orders)")

        # Q4: Top Order Channel
        cursor.execute(f"""
            SELECT order_channel, COUNT(*) AS cnt 
            FROM {ANALYTICS_TABLE_NAME} 
            GROUP BY order_channel 
            ORDER BY cnt DESC LIMIT 1;
        """)
        top_ch = cursor.fetchone()
        print(f"4. Top Order Channel:                 {top_ch[0]} ({top_ch[1]} orders)")

        # Q5: Average Delivery Time
        cursor.execute(f"""
            SELECT ROUND(AVG(delivery_time_minutes), 2) 
            FROM {ANALYTICS_TABLE_NAME} 
            WHERE delivery_time_minutes IS NOT NULL;
        """)
        avg_dt = cursor.fetchone()[0]
        print(f"5. Average Delivery Time:             {avg_dt:.2f} minutes")

        # Q6: Total Platform Net Revenue
        cursor.execute(f"""
            SELECT ROUND(SUM(platform_net_revenue), 2), COUNT(platform_net_revenue) 
            FROM {ANALYTICS_TABLE_NAME} 
            WHERE platform_net_revenue IS NOT NULL;
        """)
        pnr_row = cursor.fetchone()
        print(f"6. Total Platform Net Revenue:        ${pnr_row[0]:,.2f} (from {pnr_row[1]} orders)")

        print("=" * 80 + "\n")

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    run_sql_analysis()

