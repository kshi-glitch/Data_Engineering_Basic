#%%
import pandas as pd
from datetime import datetime, timedelta

# Create sample data
data = [
    # User 1 - Complete test (starts and completes within 15 min)
    {'user_id': 'user001', 'timestamp': '2025-04-25 10:00:00', 'event_type': 'device_connected', 'device_status': 'active'},
    {'user_id': 'user001', 'timestamp': '2025-04-25 10:01:00', 'event_type': 'test_started', 'device_status': 'testing'},
    {'user_id': 'user001', 'timestamp': '2025-04-25 10:10:00', 'event_type': 'test_completed', 'device_status': 'idle'},
    
    # User 1 - Incomplete test (no completion within 15 min)
    {'user_id': 'user001', 'timestamp': '2025-04-25 11:30:00', 'event_type': 'test_started', 'device_status': 'testing'},
    {'user_id': 'user001', 'timestamp': '2025-04-25 11:55:00', 'event_type': 'error', 'device_status': 'error'},
    {'user_id': 'user001', 'timestamp': '2025-04-25 12:00:00', 'event_type': 'device_disconnected', 'device_status': 'offline'},
    
    # User 2 - Complete test
    {'user_id': 'user002', 'timestamp': '2025-04-25 09:15:00', 'event_type': 'device_connected', 'device_status': 'active'},
    {'user_id': 'user002', 'timestamp': '2025-04-25 09:20:00', 'event_type': 'test_started', 'device_status': 'testing'},
    {'user_id': 'user002', 'timestamp': '2025-04-25 09:33:00', 'event_type': 'test_completed', 'device_status': 'idle'},
    
    # User 2 - Incomplete test (completion after 15 min)
    {'user_id': 'user002', 'timestamp': '2025-04-25 10:45:00', 'event_type': 'test_started', 'device_status': 'testing'},
    {'user_id': 'user002', 'timestamp': '2025-04-25 11:05:00', 'event_type': 'test_completed', 'device_status': 'idle'},
    
    # User 3 - Incomplete test (no completion at all)
    {'user_id': 'user003', 'timestamp': '2025-04-25 14:00:00', 'event_type': 'device_connected', 'device_status': 'active'},
    {'user_id': 'user003', 'timestamp': '2025-04-25 14:05:00', 'event_type': 'test_started', 'device_status': 'testing'},
    {'user_id': 'user003', 'timestamp': '2025-04-25 14:15:00', 'event_type': 'error', 'device_status': 'error'},
    {'user_id': 'user003', 'timestamp': '2025-04-25 14:20:00', 'event_type': 'device_disconnected', 'device_status': 'offline'},
    
    # User 4 - Multiple tests (one complete, one incomplete)
    {'user_id': 'user004', 'timestamp': '2025-04-25 16:00:00', 'event_type': 'device_connected', 'device_status': 'active'},
    {'user_id': 'user004', 'timestamp': '2025-04-25 16:05:00', 'event_type': 'test_started', 'device_status': 'testing'},
    {'user_id': 'user004', 'timestamp': '2025-04-25 16:12:00', 'event_type': 'test_completed', 'device_status': 'idle'},
    {'user_id': 'user004', 'timestamp': '2025-04-25 16:30:00', 'event_type': 'test_started', 'device_status': 'testing'},
    {'user_id': 'user004', 'timestamp': '2025-04-25 17:00:00', 'event_type': 'device_disconnected', 'device_status': 'offline'}
]

# Create DataFrame
df_logs = pd.DataFrame(data)

# Convert timestamp to datetime
df_logs['timestamp'] = pd.to_datetime(df_logs['timestamp'])

# Sort by user_id and timestamp (as would likely be needed in real scenario)
df_logs = df_logs.sort_values(['user_id', 'timestamp'])

# Display the sample dataset
print("Sample Device Log DataFrame:")
df_logs.head(10)
# %%
def detect_incomplete_tests(df_logs):
        test_events = df_logs[df_logs['event_type'].isin(['test_started', 'test_completed'])]
        # Convert timestamp to datetime if not already
        if not pd.api.types.is_datetime64_dtype(test_events['timestamp']):
            test_events['timestamp'] = pd.to_datetime(test_events['timestamp'])
        
        # Create separate DataFrames for starts and completions
        starts = test_events[test_events['event_type'] == 'test_started']
        completions = test_events[test_events['event_type'] == 'test_completed']
        
        incomplete_tests = []
        
        # Group starts by user_id to process each user efficiently
        for user_id, user_starts in starts.groupby('user_id'):
            user_completions = completions[completions['user_id'] == user_id]
            
            # For each test start, check if there's a completion within 15 minutes
            for _, start_row in user_starts.iterrows():
                start_time = start_row['timestamp']
                end_time = start_time + pd.Timedelta(minutes=15)
                
                # Find any matching completion within the time window
                matching_completions = user_completions[
                    (user_completions['timestamp'] >= start_time) & 
                    (user_completions['timestamp'] <= end_time)
                ]
                
                # If no matching completion found, add to incomplete tests
                if matching_completions.empty:
                    incomplete_tests.append((user_id, start_time))
        
        return incomplete_tests
