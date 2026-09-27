from pymongo import MongoClient

c = MongoClient("mongodb://localhost:27018")
doc = c.log_analytics.window_aggregates.find_one({}, {"_id": 0, "top_hosts": 1, "top_paths": 1})
print("Sample top_hosts item keys:", doc['top_hosts'][0].keys() if doc['top_hosts'] else "Empty lists")
print("Sample top_hosts full content:", doc['top_hosts'])
print("Sample top_paths full content:", doc['top_paths'])
