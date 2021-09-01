import psycopg2


def print_results(cursor):
	results = cursor.fetchall()
	print(f"Retrieved {len(results)} rows")
	for result in results:
		print(result)


try:
	conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
	cur = conn.cursor()
	cur.execute("SELECT * FROM songplays LIMIT 5;")
	print_results(cur)
	cur.execute("SELECT * FROM users LIMIT 5;")
	print_results(cur)
	cur.execute("SELECT * FROM songs LIMIT 5;")
	print_results(cur)
	cur.execute("SELECT * FROM artists LIMIT 5;")
	print_results(cur)
	cur.execute("SELECT * FROM time LIMIT 5;")
	print_results(cur)
	cur.close()
	conn.close()
except Exception as e:
	print(f"Error executing tests: {e}")
	exit(1)

print("Done")
