from datetime import datetime
import time

while True:
    now = datetime.now()
    print(f"Seconds: {now.second}")
    time.sleep(0.1)  # 10ms sleep for more granular observation
