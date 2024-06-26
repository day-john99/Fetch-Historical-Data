



import time

import pandas as pd
#import openpyxl
from datetime import datetime
from datetime import timedelta

from breeze_connect import BreezeConnect

class Pull_spot :

         def __init__(self):

               self.start_date = "2023-01-01T07:00:00.000Z"           # start date : 01 october 2021
               self.end_date   = "2023-01-02T07:00:00.000Z"
               self.last_date  = "2024-05-02T07:00:00.000Z"           # last  date : 30 april 2024



         def start_update(self):            # fresh start date is one day plus-off the previous loop end date
             self.temp_date = datetime.fromisoformat(self.end_date[:-1])  # converting iso into datetime obj

             one_day = timedelta(days=1)
             self.temp_date = self.temp_date + one_day

             # convert back to iso format
             self.temp_date = self.temp_date.isoformat()

             self.temp_date = self.temp_date + '.000Z'  # to get perfect iso format

             return self.temp_date

         def end_update(self):                    # fresh end date is 2 days plus-off previous loop end date
             self.end_date = datetime.fromisoformat(self.end_date[:-1])  # converting iso into datetime obj

             two_days = timedelta(days=2)

             # counter updates end date ,every 2 days
             self.end_date = self.end_date + two_days  # here start_date is datetime object

             # converting it into iso format
             self.end_date = self.end_date.isoformat()

             self.end_date = self.end_date + '.000Z'  # to get perfect iso format

             # condition to correct enddate as if enddate > expiry date by 1,2 day as its updated by 2 days always
             if ((datetime.fromisoformat(self.end_date[:-1]) > datetime.fromisoformat(self.last_date[:-1]))):
                 self.end_date = self.last_date

             return self.end_date


#---------------------LOGIN---------------------

# Initialize SDK
breeze = BreezeConnect(api_key="BREEZE_API_KEY")

# Generate Session
breeze.generate_session(api_secret="BREEZE_API_SECRET",
                        session_token="SESSION_TOKEN")

print(breeze.get_funds())

#---------------------LOGGED_IN---------------------
obj = Pull_spot()
api_callcount = 0

df_full = pd.DataFrame()     # create an empty dataframe

# start timer
timer_start = time.time()  # start timer

# while loop runs from end date  to last date
while( (datetime.fromisoformat(obj.end_date)) < (datetime.fromisoformat(obj.last_date)) ):

 try:

   api_callcount = api_callcount + 1  # api call count

   df = breeze.get_historical_data(interval="1minute",
                            from_date= obj.start_date,
                            to_date= obj.end_date,
                            stock_code="NIFIN",
                            exchange_code="NSE",

                            )

   df = pd.DataFrame(df["Success"])  # 2 day data csv
   df_full = df_full._append(df, ignore_index=True)  # appended complete data csv

   # condition to check api is not called 100 times a minute breach limit
   if (api_callcount == 99 and (time.time() - timer_start) <= 60):
      sleepy = 60 - (time.time() - timer_start) + 3  # here 3 sec act as buffer
      time.sleep(sleepy)  # sleep for sleepy second
      api_callcount = 0
      timer_start = 0


#          ----------------DATA INTO DATAFRAME/CSV/EXCEL

   #df_full.to_excel('spotNifty.xlsx', index=False)    # dataframe to excel file

#--------------------------update start date and end date
   obj.start_date = obj.start_update()
   obj.end_date   = obj.end_update()

   print("\n",obj.start_date,"\n",obj.end_date,"\n",api_callcount)


   # speed speed speed
   if (api_callcount % 50 == 0):



      try:
       breeze = BreezeConnect(api_key="BREEZE_API_KEY")


       breeze.generate_session(api_secret="BREEZE_API_SECRET",
                               session_token="SESSION_TOKEN")

       print("\n",breeze.get_funds())
       print("\n", df_full)
       

      except Exception as e1:
          print(f"Error encountered: {e1}")
          time.sleep(5)  # Wait 10 seconds before retrying
          continue  # Continue to the next iteration of the loop




 except Exception as e:
     print(f"Error encountered: {e}")
     time.sleep(5)  # Wait 10 seconds before retrying
     continue  # Continue to the next iteration of the loop

#-----------------------END OF CODE----------------------
print("\n",df_full)

df_full.to_excel('spotFinNifty.xlsx', index=False)    # dataframe to excel file

print("\nEND OF CODE")





