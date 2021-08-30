import random
from datetime import datetime
import pandas as pd
import time

class Simulation(object):

    minutes_file = pd.DataFrame(columns=['user_id','seg_start','seg_end','avg_hr','min_hr','max_hr','avg_rr'])
    seconds_file = pd.DataFrame(columns=["user_id","timestamp","heart_rate","respiration_rate","activity"])
    
    @classmethod
    def processor(cls,data,count):
        cls.seconds_file = cls.seconds_file.append({"user_id":data["user_id"],"timestamp":data["timestamp"],"heart_rate":data["heart_rate"],"respiration_rate":data["respiration_rate"],"activity":data["activity"]},ignore_index = True)
        print(f"{count}-sec")
        if  count and count % 15 == 0:
            fifteen_min_df = cls.seconds_file.tail(15)
            cls.minutes_file = cls.minutes_file.append({
                'user_id':fifteen_min_df['user_id'].iloc[0],
                "seg_start": fifteen_min_df['timestamp'].iloc[0],
                "seg_end": fifteen_min_df['timestamp'].iloc[-1],
                "avg_hr": fifteen_min_df['heart_rate'].mean(),
                "min_hr": fifteen_min_df['heart_rate'].min(),
                "max_hr": fifteen_min_df['heart_rate'].max(),
                "avg_rr": fifteen_min_df['respiration_rate'].mean()
                }, ignore_index=True)
            
    def simulator(self):
        epoch = datetime(1970,1,1)
        count = 0
        while count < 61: # Keep this as "While True" for running continuesly
            hr = random.randint(40,90)
            rr = random.randint(10,30)
            activity = random.randint(1,5)
            timestamp = int((datetime.utcnow() - epoch).total_seconds())
            data = {
                "user_id":'abc',
                "timestamp": timestamp,
                "heart_rate": hr,
                "respiration_rate": rr,
                "activity": activity
            }
            self.processor(data,count)
            count += 1
            time.sleep(1)

    @classmethod
    def get_result(cls):
        import json
        print("****************Resulting datframe ******************") 
        print(cls.minutes_file)
        print("**************** ******************") 
        pusle_data = cls.seconds_file.to_dict('records')
        with open('Pulse_result.json', 'w') as f:
            json.dump(pusle_data, f)
        cls.minutes_file.to_csv('15_minutes_simulation_result.csv')

testing = Simulation()
testing.simulator()
testing.get_result()