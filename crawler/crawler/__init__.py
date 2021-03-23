from model.db_loader import MySQL

sql_db = MySQL(key_file='../keys/aws_sql_key.json',
               database='crawl_job')