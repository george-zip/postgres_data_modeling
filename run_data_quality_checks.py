import psycopg2
from create_tables import get_configuration_mgr


def run_and_evaluate(query, cursor):
	""""
	- Runs query and confirm that it returned results
	- Print query results to stdout.
	- Raises RuntimeError if no results were returned by the query
	"""
	cursor.execute(query)
	results = cursor.fetchall()
	if len(results):
		print(f"Retrieved {len(results)} rows")
		for result in results:
			print(result)
	else:
		raise RuntimeError(f"Query did not return results {query}")


data_quality_checks = [
	"SELECT * FROM songplays LIMIT 5;",
	"SELECT * FROM users LIMIT 5;",
	"SELECT * FROM songs LIMIT 5;",
	"SELECT * FROM artists LIMIT 5;",
	"SELECT * FROM time LIMIT 5;",
	"SELECT * FROM songplays sp, songs s where sp.song_id = s.song_id LIMIT 5;",
	"SELECT * FROM songplays sp, artists a where sp.artist_id = a.artist_id LIMIT 5;",
	"SELECT distinct u.last_name FROM songplays sp, users u where sp.user_id = u.user_id LIMIT 5;",
	"SELECT t.month FROM songplays sp, time t where sp.start_time = t.start_time LIMIT 5;"
]

"""
- Connect to database and run all data quality queries
- Close cursor and connection
- Catch any exceptions raised
"""
cfg = get_configuration_mgr()
connect_string = (
	f"host={cfg.get('postgres_host')} dbname={cfg.get('sparkify_dbname')} user={cfg.get('user')} "
	f"password={cfg.get('password')}"
)
with psycopg2.connect(connect_string) as conn:
	with conn.cursor() as cur:
		cur = conn.cursor()
		for query in data_quality_checks:
			run_and_evaluate(query, cur)

# leaving contexts doesn't close the connection
conn.close()
print("ALL TESTS PASSED")
