from pyspark.sql.functions import from_json, col
import psycopg2
 

def process_batch(batch_df, batch_id):

    db_params = {
        'dbname': 'reliance',
        'user': 'docker',
        'password': 'docker',
        'host': '192.168.1.223',
        'port': 5432
    }

    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    row = batch_df.collect()
    if row.operation == "INSERT":
        insert_query = """
            INSERT INTO sink_profit_loss_changes (change_id, change_time, index, year, sales, expenses, operating_profit, 
                                             opm_percent, other_income, interest, depreciation, profit_before_tax, 
                                             tax_percent, net_profit, eps_in_rs, dividend_payout_percent, stock, Com_Key)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
            """
        cur.execute(insert_query, (
                row.change_id, row.change_time, row.index, row.year, row.sales, row.expenses,
                row.operating_profit, row.opm_percent, row.other_income, row.interest, row.depreciation,
                row.profit_before_tax, row.tax_percent, row.net_profit, row.eps_in_rs, row.dividend_payout_percent,
                row.stock, row.Com_Key
            ))
        

    conn.commit()
    cur.close()
    conn.close()