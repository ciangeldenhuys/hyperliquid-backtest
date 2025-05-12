from datetime import datetime, timezone

timestamp_ms = 1746838085203
timestamp_s = timestamp_ms / 1000
date_time = datetime.fromtimestamp(timestamp_s, tz=timezone.utc)

print(date_time)