import os
import pytz
import pandas as pd
import googleapiclient.discovery
import googleapiclient.errors

from pathlib import Path
from decouple import config
from datetime import datetime
from typing import Union, Callable
from config.quota_method_amounts import quota_method_amount_map

QUOTA_LOG_PATH = Path(os.getcwd()) / "logs"
YOUTUBE_API_SERVICE_NAME = config("YOUTUBE_API_SERVICE_NAME")
YOUTUBE_API_VERSION = config("YOUTUBE_API_VERSION")
YOUTUBE_API_KEY = config("YOUTUBE_API_KEY")

def update_quota_log(date: datetime, quota: int, method: str) -> None:
    """
    Tracks quota for the day

    Args:
        date (datetime): the date of the api request was called
        quota (int): the quota being used
        method (str): the method being called

    Returns:
        None
    """
    quota_log_filename = "quota_log.parquet"
    first_time_log = False
    try:
        old_quota_df = pd.read_parquet(QUOTA_LOG_PATH / quota_log_filename)
    except FileNotFoundError:
        print("Gennerating log file ...")
        first_time_log = True

    new_quota_log_df = pd.DataFrame({
        "method": [method],
        "date": [date.date()],
        "time": [date.time()],
        "used_quota": [quota],
        "total_quota_used": [quota]
    })

    if first_time_log:
        new_quota_log_df.to_parquet(QUOTA_LOG_PATH / quota_log_filename)
    else:
        quota_log_df = pd.concat([old_quota_df, new_quota_log_df])
        quota_log_df.sort_values(by=["date", "time"], inplace=True)
        quota_log_df.reset_index(drop=True, inplace=True)

        # -2 because -1 is the record we just put at the end
        previous_date = quota_log_df.iloc[-2]["date"]

        # if date not equal to the previous date this is the first record in quota_log today
        if previous_date != date.date():
            quota_log_df.to_parquet(QUOTA_LOG_PATH / quota_log_filename)
        else:
            # previous_quota_used = quota_log_df.iloc[-2]["used_quota"]
            previous_total_quota = quota_log_df.iloc[-2]["total_quota_used"]

            quota_log_df.iloc[-1, quota_log_df.columns.get_loc('total_quota_used')] += previous_total_quota

            quota_log_df.to_parquet(QUOTA_LOG_PATH / quota_log_filename)

class YoutubeRequestExecutor():
    def __init__(self, request: Callable, quota_limit: int):
        self.request = request
        self.quota_limit = quota_limit
        self.quota_method_amount_map = quota_method_amount_map
        self.quota_log_filename = "quota_log.parquet"

    def execute(self) -> Union[Callable, int]:
        """
        Executes a request to the youtube api while monitoring the specified quota limit

        Args:
            None
        
        Returns:
            A response object from the youtube API or an error code
        """

        # get methodId to calculate quota usage
        resource = self.request.methodId.split(".")[1]
        method = self.request.methodId.split(".")[2]

        try:
            quota_log_df = pd.read_parquet(QUOTA_LOG_PATH / self.quota_log_filename)

            last_date_ran = quota_log_df.iloc[-1]["date"]

            # # checks if run date is a new day if so reset everything
            # if last_date_ran != datetime.today().date():
            #     # total_quota_used = quota_method_amount[method]
            #     potential_quota = quota_method_amount[method]
            # else:
            #     total_quota_used = quota_log_df.iloc[-1]["total_quota_used"]
            #     potential_quota = quota_method_amount[method] + total_quota_used

            if last_date_ran == datetime.today().date():
                total_quota_used = quota_log_df.iloc[-1]["total_quota_used"]
                potential_quota = self.quota_method_amount_map[resource][method] + total_quota_used

                if potential_quota > self.quota_limit:
                    print(f"Potential quota (currently {total_quota_used}) will be over the pre-set limit of {self.quota_limit}. No more queries will be executed!")
                    # Too Many Requests (RFC 6585)
                    return 429
            else:
                print("First request execution of the day!")

        except FileNotFoundError:
            print("Quota log file not found ...")

        # have to convert timeframe to PT because quota resets at midnight PT time
        dt = datetime.now()
        pt_tz = pytz.timezone("America/Los_Angeles")
        pt_dt = dt.astimezone(pt_tz)

        try:
            response = self.request.execute()
        except googleapiclient.errors.HttpError as err:
            update_quota_log(
                date=pt_dt,
                quota=self.quota_method_amount_map[resource][method],
                method=method,
            )

            quota_log_df = pd.read_parquet(QUOTA_LOG_PATH / self.quota_log_filename)
            total_quota_used = quota_log_df.iloc[-1]["total_quota_used"]
            print(f"===========================================\nQUOTA USED IN '{method}' METHOD: {self.quota_method_amount_map[resource][method]}\nTotal QUOTA USED Today: {total_quota_used}\n===========================================\n")
            
            return err.status_code

        update_quota_log(
            date=pt_dt,
            quota=self.quota_method_amount_map[resource][method],
            method=method,
        )
        
        quota_log_df = pd.read_parquet(QUOTA_LOG_PATH / self.quota_log_filename)
        total_quota_used = quota_log_df.iloc[-1]["total_quota_used"]
        print(f"===========================================\nQUOTA USED IN '{method}' METHOD: {self.quota_method_amount_map[resource][method]}\nTotal QUOTA USED Today: {total_quota_used}\n===========================================\n")

        return response

if __name__ == "__main__":
    youtube = googleapiclient.discovery.build(
        YOUTUBE_API_SERVICE_NAME, 
        YOUTUBE_API_VERSION, 
        developerKey=YOUTUBE_API_KEY
    )

    youtube_request = YoutubeRequestExecutor(
        request=youtube.search().list(
            part="snippet",
            q="J. Cole"
        ),
        quota_limit=9500
    )

    response = youtube_request.execute()

    print(response)

