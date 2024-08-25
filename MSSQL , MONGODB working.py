import time
import pymssql
import pymongo
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# MSSQL Connection
mssql_conn = pymssql.connect(server='SQLSERVER', user='sqlread', password='sqlread', database='DB_TEST')
mssql_cursor = mssql_conn.cursor()

# MongoDB Connection
mongo_client = pymongo.MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['testdb']
mongo_collection = mongo_db['testcollection']
    
def measure_time(func, *args, **kwargs):
    start_time = time.time()
    func(*args, **kwargs)
    end_time = time.time()
    return end_time - start_time

def mssql_insert(records):
    for record in records:
        mssql_cursor.execute("INSERT INTO TestTable (id, value) VALUES (%s, %s)", (record['id'], record['value']))
    mssql_conn.commit()

def mongo_insert(records):
    mongo_collection.insert_many(records)

def mssql_select(pattern):
    mssql_cursor.execute("SELECT * FROM TestTable WHERE value LIKE %s", (pattern,))
    return mssql_cursor.fetchall()

def mongo_select(pattern):
    return list(mongo_collection.find({"value": {"$regex": pattern}}))


def mssql_delete(pattern):
    mssql_cursor.execute("DELETE FROM TestTable WHERE value LIKE %s", (pattern,))
    mssql_conn.commit()

def mongo_delete(pattern):
    mongo_collection.delete_many({"value": {"$regex": pattern}})


def generate_records(num_records):
    return [{'id': i, 'value': f'data_{i}'} for i in range(num_records)]

def benchmark(database, num_records):
    records = generate_records(num_records)
    pattern = 'data_2%'  # Pattern for MSSQL
    regex_pattern = '^data_2'  # Pattern for MongoDB
    
    if database == 'MSSQL':
        mssql_delete(pattern)  # Ensure table is empty before starting
        insert_time = measure_time(mssql_insert, records)
        select_time = measure_time(mssql_select, pattern)
        delete_time = measure_time(mssql_delete, pattern)
    elif database == 'MongoDB':
        mongo_delete(regex_pattern)  # Ensure collection is empty before starting
        insert_time = measure_time(mongo_insert, records)
        select_time = measure_time(mongo_select, regex_pattern)
        delete_time = measure_time(mongo_delete, regex_pattern)
    
    return insert_time, select_time, delete_time

def run_benchmarks():
    results = []
    record_counts = [1000000, 5000000, 10000000]
    
    for num_records in record_counts:
        for db in ['MSSQL', 'MongoDB']:
            insert_time, select_time, delete_time = benchmark(db, num_records)
            results.append({
                'Database': db,
                'Records': num_records,
                'Operation': 'Insert',
                'Time (s)': insert_time
            })
            results.append({
                'Database': db,
                'Records': num_records,
                'Operation': 'Select',
                'Time (s)': select_time
            })
            results.append({
                'Database': db,
                'Records': num_records,
                'Operation': 'Delete',
                'Time (s)': delete_time
            })
    
    df = pd.DataFrame(results)
    return df

def visualize_results(df):
    sns.set(style="whitegrid")
    
        
    # Line plots
    fig_line, axes_line = plt.subplots(3, 1, figsize=(10, 15))

    sns.lineplot(x='Records', y='Time (s)', hue='Database', style='Database', markers=True, dashes=False, data=df[df['Operation'] == 'Insert'], ax=axes_line[0])
    axes_line[0].set_title('Insert Operation Duration (lower is better)')
    axes_line[0].ticklabel_format(axis='x', style='plain')  # Remove scientific notation
    
    sns.lineplot(x='Records', y='Time (s)', hue='Database', style='Database', markers=True, dashes=False, data=df[df['Operation'] == 'Select'], ax=axes_line[1])
    axes_line[1].set_title('Select Operation Duration (lower is better)')
    axes_line[1].ticklabel_format(axis='x', style='plain')  # Remove scientific notation
    
    sns.lineplot(x='Records', y='Time (s)', hue='Database', style='Database', markers=True, dashes=False, data=df[df['Operation'] == 'Delete'], ax=axes_line[2])
    axes_line[2].set_title('Delete Operation Duration (lower is better)')
    axes_line[2].ticklabel_format(axis='x', style='plain')  # Remove scientific notation
    
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    results_df = run_benchmarks()
    print(results_df)
    visualize_results(results_df)

# Enable to run in loop 10 times.
#if __name__ == "__main__":
 #   for i in range(10):
  #      print(f"Run {i+1}/10")
   #     results_df = run_benchmarks()
    #    print(results_df)
     #   visualize_results(results_df)
