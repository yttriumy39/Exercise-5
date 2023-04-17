import psycopg2
import os


def main():
    host = 'postgres'
    database = 'postgres'
    user = 'postgres'
    pas = 'postgres'
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    # your code here

    cur = conn.cursor()

    #create the table's if they don't exist
    cur.execute(""" CREATE TABLE IF NOT EXISTS accounts (
    customer int PRIMARY KEY,
    first_name varchar(40) NOT NULL,
    last_name varchar(40) NOT NULL,
    address_1 varchar(80) NOT NULL,
    address_2 varchar(10),
    city varchar(40) NOT NULL,
    state varchar(40) NOT NULL,
    zipcode varchar(40) NOT NULL,
    join_date date NOT NULL
    );""")

    cur.execute(""" CREATE TABLE IF NOT EXISTS products (
    product_id int PRIMARY KEY,
    product_code int UNIQUE NOT NULL,
    product_description varchar(40) NOT NULL
    );""")

    cur.execute(""" CREATE TABLE IF NOT EXISTS transactions (
    transaction_id int PRIMARY KEY,
    transaction_date date NOT NULL,
    product_id int NOT NULL,
    product_code int UNIQUE NOT NULL,
    quantity int NOT NULL,
    account_id int NOT NULL,
    FOREIGN KEY (product_id)
      REFERENCES products (product_id),
    FOREIGN KEY (account_id)
      REFERENCES accounts (customer)
    );""")

    #copy into the sql tables from the csv's
    with open('/app/data/accounts.csv', 'r') as f:
        next(f) 
        cur.copy_from(f, 'accounts', sep=',')

    with open('/app/data/products.csv', 'r') as f:
        next(f)
        cur.copy_from(f, 'products', sep=',')

    with open('/app/data/transactions.csv', 'r') as f:
        next(f)
        cur.copy_from(f, 'transactions', sep=',')

    #can now run select queries on the data
    sql3 = '''select * from products;'''
    cur.execute(sql3)
    for i in cur.fetchall():
        print(i)
  
    conn.commit()
    conn.close()
    
if __name__ == '__main__':
    main()
