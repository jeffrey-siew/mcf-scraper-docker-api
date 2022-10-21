"""
project: MyCareerFuture scraper
version: 1.5.0
runtime env: aws docker container (does not requires secret.py, S3 access authentication would be handle by IAM
description: this is a multi-threaded api scraper of mycareerfuture that would be deploy in aws and its' save output into aws S3 bucket
notes: this script will not work outside of AWS and docker container
"""

#   ######  ##  ##  #####    ####   #####   ######
#     ##    # ## #  ##  ##  ##  ##  ##  ##    ##  
#     ##    # ## #  #####   ##  ##  #####     ##  
#     ##    #    #  ##      ##  ##  ## ##     ##  
#   ######  #    #  ##       ####   ##  ##    ##  

import pandas as pd
import numpy as np
import requests
import datetime
import concurrent.futures
#from tqdm import tqdm

#   #####     ##     ####   ######       #####   ####   #####   ######
#   ##  ##   #  #   ##   #  ##          ##      ##  ##  ##  ##  ##
#   #####   ######    ##    #####       ##      ##  ##  ##  ##  #####
#   ##  ##  ##  ##  #   ##  ##          ##      ##  ##  ##  ##  ##
#   #####   ##  ##   ####   ######       #####   ####   #####   ######

class base_code:
    def __init__():
        pass

    def collect_data_json(input_json: str) -> pd.DataFrame:
        """
        This function would scrape the website from the json data and output it in a DataFrame format.
        """
        temp_json = input_json['results']
        output_df = pd.json_normalize(temp_json)

        return output_df

    def get_max_search_json() -> list:
        """
        This function is use to generate the list of number of new job page, and the return list would be use in threadpoolexecutor
        """
        url = 'https://api.mycareersfuture.gov.sg/v2/jobs?limit=20&page=0&sortBy=min_monthly_salary'
        result_temp = requests.get(url).json()
        web_total_page = result_temp['total']//20

        web_total_page_list = []
        for each in range(web_total_page):
            web_total_page_list.append(each)
        
        return web_total_page_list

    def get_new_meta_data(input_df: pd.DataFrame) -> pd.DataFrame:
        """
        
        """
        output_meta_df = input_df[[
            'metadata.jobPostId','metadata.deletedAt','metadata.createdBy',
            'metadata.createdAt','metadata.updatedAt','metadata.editCount',
            'metadata.repostCount','metadata.totalNumberOfView','metadata.newPostingDate',
            'metadata.originalPostingDate','metadata.expiryDate',
            'metadata.totalNumberJobApplication','metadata.isPostedOnBehalf',
            'metadata.isHideSalary','metadata.isHideCompanyAddress',
            'metadata.isHideHiringEmployerName','metadata.isHideEmployerName',
            'metadata.jobDetailsUrl','metadata.matchedSkillsScore'
        ]]
        return output_meta_df
    
    def get_new_job_data(input: pd.DataFrame, jobid: list) -> pd.DataFrame:
        """
        
        """
        output_scrapped_df = input[~input['metadata.jobPostId'].isin(jobid)]
        return output_scrapped_df

    def logging(message):
        """
        
        """
        timestamp_format = '%Y-%h-%d-%H:%M:%S'
        now = datetime.now()
        timestamp = now.strftime(timestamp_format)
        print(timestamp + ', ' + message)

#   ######  ##  ##  #####   ######    ##    #####        ####   ######  ######  ##  ##  #####
#     ##    ##  ##  ##  ##  ##       #  #   ##  ##      ##   #  ##        ##    ##  ##  ##  ##
#     ##    ######  #####   #####   ######  ##  ##        ##    #####     ##    ##  ##  #####
#     ##    ##  ##  ## ##   ##      ##  ##  ##  ##      #   ##  ##        ##    ##  ##  ##
#     ##    ##  ##  ##  ##  ######  ##  ##  #####        ####   ######    ##     ####   ##

class thread_setup:
    def __init__():
        pass

    def setup_threaded_workers_search(main_list: list) -> pd.DataFrame:
        """
        Multi-thread function to collect the jobs website link.
        The collected link would be use in subsequent function 'setup_threaded_workers_scraper', which would use the website link to scrap the data.
        """
        #threaded_df = pd.DataFrame
        partitions = 4
        splitted_list = np.array_split(main_list,partitions)
        with concurrent.futures.ThreadPoolExecutor(max_workers= partitions) as executor:        
            threaded_scrapped_df = executor.map(thread_setup.get_handles_new_link, splitted_list)
        
        output_scrapped_df = thread_setup.threaded_df_output(threaded_scrapped_df)

        return output_scrapped_df

    def get_handles_new_link(splitted_list: list) -> pd.DataFrame:
        """
        The function that would run in multi-threaded function 'setup_threaded_workers_search'
        """
        output_scrapped_df = pd.DataFrame()
        for each_page in splitted_list:
            try:
                url = ('https://api.mycareersfuture.gov.sg/v2/jobs?limit=20&page=' + str(each_page) + '&sortBy=new_posting_date')
                json_temp = requests.get(url).json()
                temp_scrapped_df = base_code.collect_data_json(json_temp)
                frames = [output_scrapped_df, temp_scrapped_df]
                output_scrapped_df = pd.concat(frames)
            except:
                pass
        
        return output_scrapped_df

    def threaded_df_output(input_gen):
        """
        This function is use to convert the generator output from the multi-threaded function into a single Dataframe.
        """
        output_df = pd.DataFrame()
        for each_gen_df in input_gen:
            frames = [output_df, each_gen_df]
            #output_df = pd.concat(frames)
            output_df = pd.concat(
                        frames,
                        axis=0,
                        join="outer",
                        ignore_index=True,
                        keys=None,
                        levels=None,
                        names=None,
                        verify_integrity=False,
                        copy=True,
                    )
        return output_df

#    ####   #####       #####   ##  ##   #####  ##  ##  ######  ######
#   ##   #      ##      ##  ##  ##  ##  ##      ## ##   ##        ##  
#    ####   #####       #####   ##  ##  ##      ####    #####     ##  
#   #   ##      ##      ##  ##  ##  ##  ##      ## ##   ##        ##  
#    ####   #####       #####    ####    #####  ##  ##  ######    ##  

class s3_bucket:
    def __init__():
        pass

    def s3_save_instance_df(input_df: pd.DataFrame):
        """
        aws s3 saving of scrapped data base on individual run time
        this approach would create single csv file every day or append to existing file
        """
        # setting the filler for the naming convention
        now = datetime.datetime.now() + datetime.timedelta(hours=8)
        year = now.year
        month = now.month
        day = now.day
        # setting the S3 bucket name
        #aws_s3_bucket = 'mycareerfuture-scraper'
        aws_s3_bucket = 'mycareerfuture-scraper/raw'
        #aws_s3_bucket = 'mcf-scraped-data'
        # setting the S3 object name / file name
        key = f'mycareersfuture_scrapped_api_{month}_{day}_{year}.csv'

        # try to check if S3 have existing file with the same key naming convention, if so load the csv and concat it with input_df before saving back to S3
        # if except FileNotFoundError is raise, it would save as per key naming convention.
        try:
            # reading of csv directly from s3 via pandas, achieved via usage of s3fs
            exist_df = pd.read_csv(
                f"s3://{aws_s3_bucket}/{key}",
            )

            # Setting the list of dataframe
            frames = [exist_df, input_df]
            # Combining the website_new and website_old dataframe into one dataframe 
            input_df = pd.concat(
                                frames,
                                axis=0,
                                join="outer",
                                ignore_index=True,
                                keys=None,
                                levels=None,
                                names=None,
                                verify_integrity=False,
                                copy=True,
                            )
            print(f'Per Instance File Found, updating {key} into S3')
        except FileNotFoundError:
            print(f'Per Instance File Not Found, saving new file into S3 as {key}')
        
        # uploading of csv directly to s3 via pandas, achieved via usage of s3fs
        input_df.to_csv(
            f"s3://{aws_s3_bucket}/{key}",
            index=False,
        )

    def s3_save_meta_df(input_df: pd.DataFrame):
        """
        aws s3 saving of scrapped meta data base on individual run time
        this approach would create single csv file every day
        """
        # setting the filler for the naming convention
        now = datetime.datetime.now() + datetime.timedelta(hours=8)
        year = now.year
        month = now.month
        day = now.day
        # setting the S3 bucket name
        aws_s3_bucket = 'mycareerfuture-scraper/meta'
        #aws_s3_bucket = 'mcf-scraped-metadata'
        # setting the S3 object name / file name
        key = f'mycareersfuture_scrapped_meta_{month}_{day}_{year}.csv'

        # try to check if S3 have existing file with the same key naming convention, if so load the csv and concat it with input_df before saving back to S3
        # if except FileNotFoundError is raise, it would save as per key naming convention.
        try:
            # reading of csv directly from s3 via pandas, achieved via usage of s3fs
            exist_df = pd.read_csv(
                f"s3://{aws_s3_bucket}/{key}",
            )

            # Setting the list of dataframe
            frames = [exist_df, input_df]
            # Combining the website_new and website_old dataframe into one dataframe 
            input_df = pd.concat(
                                frames,
                                axis=0,
                                join="outer",
                                ignore_index=True,
                                keys=None,
                                levels=None,
                                names=None,
                                verify_integrity=False,
                                copy=True,
                            )
            print(f'Meta File Found, Updating file {key} into S3')
        except FileNotFoundError:
            print(f'Meta File Not Found, Saving new file into S3 as {key}')
        
        # uploading of csv directly to s3 via pandas, achieved via usage of s3fs
        input_df.to_csv(
            f"s3://{aws_s3_bucket}/{key}",
            index=False,
        )

    def s3_load_jobid_list() -> list:
        """
        aws s3 loading of website link csv file in the s3 bucket
        """
        # importing the full scrapped data from s3 bucket
        # setting the S3 bucket name
        aws_s3_bucket = 'mycareerfuture-scraper/joblist'
        # setting the S3 object name / file name
        key = 'mycareersfuture_scrapped_jobid.csv'

        try:
            # reading of csv directly from s3 via pandas, achieved via usage of s3fs
            output_df = pd.read_csv(
                f"s3://{aws_s3_bucket}/{key}",
            )
            output_list = list(output_df['metadata.jobPostId'])

        except FileNotFoundError:
            output_list = []
            print('JobID File Not Found')

        return output_list

    def s3_save_jobid(input_df: pd.DataFrame):
        """
        aws s3 saving of website link that was scrapped
        this would continous update the csv file to store all website link that was scrapped
        these website link form as a core reference to identify scrapped or new links
        """
        # filter out the website link from scrapped data
        input_jobid_df = input_df[['metadata.jobPostId']]

        # importing the full scrapped data from s3 bucket
        # setting the S3 bucket name
        aws_s3_bucket = 'mycareerfuture-scraper/joblist'
        # setting the S3 object name / file name
        key = 'mycareersfuture_scrapped_jobid.csv'

        try:
            # reading of csv directly from s3 via pandas, achieved via usage of s3fs
            exist_df = pd.read_csv(
                f"s3://{aws_s3_bucket}/{key}",
            )
            print('JobID File Found, the existing JobID file will be updated')
        except FileNotFoundError:
            exist_df = pd.DataFrame
            print('JobID File Not Found, a new JobID file will be saved')

        frames = [exist_df, input_jobid_df]
        # Combining the website_new and website_old dataframe into one dataframe 
        output_df = pd.concat(
                            frames,
                            axis=0,
                            join="outer",
                            ignore_index=True,
                            keys=None,
                            levels=None,
                            names=None,
                            verify_integrity=False,
                            copy=True,
                        )

        # uploading of csv directly to s3 via pandas, achieved via usage of s3fs
        output_df.to_parquet(
            f"s3://{aws_s3_bucket}/{key}", 
            engine='pyarrow', 
            compression='gzip', 
            index=None, 
            partition_cols=None
        )


#   ##  ##    ##    ######  ##   #
#   # ## #   #  #     ##    ###  #
#   # ## #  ######    ##    # ## #
#   #    #  ##  ##    ##    #  ###
#   #    #  ##  ##  ######  #   ##

def main():
    """
    this is the main function of this web scraper for mycareerfuture
    """
    #logging starting ETLT process
    base_code.log('Starting EtLT process')
    total_page_list = base_code.get_max_search_json() # firstly to identify the number of pages of jobs that in mycareerfuture website
    #web_total_page_list = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20] # debug input to limit search parameter

    #logging starting extract process
    base_code.log('Starting extract process')
    # load up scrapped jobid list
    # this list would be use to filter all scrapped information before saving it to S3
    jobid_list = s3_bucket.s3_load_jobid_list()

    df_scrapped_output = thread_setup.setup_threaded_workers_search(total_page_list) # running the multi-threaded process to scrape the href in each page
    #logging completed extract process
    base_code.log('Completed extract process')

    #logging starting transform process
    base_code.log('Starting transform process')
    df_scrapped_new_job = base_code.get_new_job_data(df_scrapped_output, jobid_list)

    df_meta_df = base_code.get_new_meta_data(df_scrapped_output)
    #logging completed transform process
    base_code.log('Completed transform process')

    #logging starting loading process
    base_code.log('Starting loading process')
    s3_bucket.s3_save_instance_df(df_scrapped_new_job) # run the function to save the scrapped data into s3 bucket
    print(f'{df_scrapped_new_job.shape[0]} new jobs scrapped, and S3 Bucket Per Instance DataFrame Saved')

    s3_bucket.s3_save_meta_df(df_meta_df) # run the function to save the scrapped meta data into s3 bucket
    print(f'{df_meta_df.shape[0]} meta data scrapped, and S3 Bucket Per Instance DataFrame Saved')

    s3_bucket.s3_save_jobid(df_scrapped_new_job) # run the function to update the website link that scrapped into s3 bucket
    print('S3 Bucket Website Link Updated')
    #logging completed loading process
    base_code.log('Completed loading process')

#                   ##   #    ##     ####   ######
#                   ###  #   #  #   # ## #  ##
#                   # ## #  ######  # ## #  #####
#                   #  ###  ##  ##  #    #  ##
#   ######  ######  #   ##  ##  ##  #    #  ######  ######  ######

if __name__ == '__main__':
    main()