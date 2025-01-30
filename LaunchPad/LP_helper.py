import pypostmaster
import getpass
from queryrunner_client import *
from queryrunner_client import QueryRunnerException
import numpy as np
import pandas as pd
from pandas.io import gbq
import pytz
from datetime import datetime, timedelta, tzinfo
import time
import warnings
import matplotlib.pyplot as plt
import seaborn as sns
from dateutil.relativedelta import relativedelta
import docx
from docx import Document
from docxtpl import DocxTemplate, InlineImage
from docx.shared import Inches
import jinja2
import math
import yaml
import os
import signal


import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import uuid
import json
from google.cloud import bigquery
import base64
import nbformat
from IPython import get_ipython
import shutil
import subprocess
import random
from croniter import croniter
import re

import mysql.connector
from mysql.connector import Error

import json
import html

ipython = get_ipython()


# uSecrets 
secret='launchpad-gi'

def uSecret(secret_name):
    secret_json = os.environ['SECRETS_PATH'] +'/'+ secret +'/'+ 'client_secret_launchpad'
    # for GCP service file it returns the json filepath, for rest it returns a dict from uSecret
    return secret_json


# Path to your service account key file
address=uSecret(secret)
service_account_path = address=uSecret(secret)
# service_account_path = "creds.json"



# Set the environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_path

# Initialize the BigQuery client
bqclient = bigquery.Client()

username = getpass.getuser()
try:
    user_email = os.getenv('USER_EMAIL_ID')
except:
    user_email = username+"@ext.uber.com"
logger_id = None


db_config = {
    'user': 's_launchpad_te_launchpad_gss',
    'password': 'gjDZ7HP45ef4Iw3zuWX7Iw75BHIOdSNq',
    'host': '127.5.14.180',
    'port': 17025,
    'database': 'launchpad_test_1_mysql_db',
    'autocommit': True,
    'sql_mode': 'STRICT_TRANS_TABLES'
}




def ymlParser(file='config.yaml'):
    with open(file, "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            return 'YamlParserError'

masterConfig = ymlParser(file='config.yaml')

def Fail_mail(error, mailID=None):
    from_addr = user_email
    to_addr =user_email
    cc=['mnizam1@ext.uber.com'] 
    if mailID:
        if type(mailID)==list:
            if len(mailID)>1:
                to_addr = mailID[0]
            cc =mailID.extend(cc)
        else:
            to_addr = mailID
    print(to_addr)
       
    bcc = []
    subject = 'LaunchPad fail on '+username
    html1 = """\
            <!DOCTYPE html>
            <html>
            <head></head>
            <body>
            <p style="margin : 0; padding-top:0;">Hi, </p>
            <p style="margin : 0; padding-top:0;">The LaunchPad Task has failed on {} </p>
            <p><b> Error:</p></b>
            <p> {} </p>
            <br>   """.format(username,error)         

    
    body = html1 
    helper = pypostmaster.MailHelper()
    print(from_addr, to_addr, cc, bcc)
    print(helper.sendmail(from_addr, to_addr, subject, body, cc, bcc))

def completed_mail(mailID=None):
    from_addr = user_email
    to_addr =user_email
    cc=['mnizam1@ext.uber.com'] 
    if mailID:
        if type(mailID)==list:
            if len(mailID)>1:
                to_addr = mailID[0]
            cc =mailID.extend(cc)
        else:
            to_addr = mailID
    print(to_addr)
       
    bcc = []
    subject = 'LaunchPad Completed'
    html1 = """\
            <!DOCTYPE html>
            <html>
            <head></head>
            <body>
            <p style="margin : 0; padding-top:0;">Hi, </p>
            <p style="margin : 0; padding-top:0;">The LaunchPad Task has Completed on {} </p>
            <br>   """.format(username)         

    
    body = html1 
    helper = pypostmaster.MailHelper()
    print(from_addr, to_addr, cc, bcc)
    print(helper.sendmail(from_addr, to_addr, subject, body, cc, bcc))




def validateDriveAccess(isSheetAccess = False, isDriveAccess=False, sheetId = '1z8GP0BNfD3t9tWaqZe1WWtee7SIg0A_zDPbkjs387cM' ):
# def validateDriveAccess(isSheetAccess = False, isDriveAccess=False, serviceFile = "daasu4b-9f48d5a6e540.json", sheetId = '1z8GP0BNfD3t9tWaqZe1WWtee7SIg0A_zDPbkjs387cM' ):
    # Scope for accessing Google Sheets
    # scope = ['https://spreadsheets.google.com/feeds',
    #          'https://www.googleapis.com/auth/drive']
    address=uSecret(secret)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

    
    if isSheetAccess:
        # Authenticate using the service account credentials
        # credentials = ServiceAccountCredentials.from_json_keyfile_name(serviceFile, scope)
        credentials = ServiceAccountCredentials.from_json_keyfile_name(address, scope)
        client = gspread.authorize(credentials)

        # google Sheet Validation
        sheet = client.open_by_key(sheetId)
    else:
        sheet = None
    
    if isDriveAccess:
        # google Drive validation
        gauth = GoogleAuth()           
        drive = GoogleDrive(gauth) 
    else:
        drive = None
    
    return sheet,drive


def runBQuery(query, fetch_results=True):
    """
    Executes the given SQL query on the MySQL database.

    :param query: The SQL query string to be executed.
    :param fetch_results: If True, fetches and returns the results as a DataFrame (for SELECT queries).
    :return: The results as a DataFrame if fetch_results is True, otherwise None.
    """
    try:
        # print("try block")
        # Connect to the MySQL database
        connection = mysql.connector.connect(**db_config)

        if connection.is_connected():
            # print("connect")
            cursor = connection.cursor()

            # Execute the query
            cursor.execute(query)

            # If fetch_results is True, fetch the results
            if fetch_results:
                # print("fetch true")
                columns = [desc[0] for desc in cursor.description]  # Get column names
                results = cursor.fetchall()
                return pd.DataFrame(results, columns=columns)  # Return as DataFrame

            # If fetch_results is False, fetch and discard any remaining results
            else:
                # print("fetch False")
                # Fetch and discard any results to avoid "Unread result found" error
                if cursor.with_rows:
                    cursor.fetchall()  # Fetch all results and discard

            # Commit the transaction (for INSERT, UPDATE, DELETE, etc.)
            connection.commit()
            print("Query executed successfully.")

    except Error as e:
        print(f"Error: {e}")

    # finally:
    #     if connection.is_connected():
    #         cursor.close()
    #         connection.close()
    #         print("MySQL connection is closed")

    return None





def runBQuery_json(query, params=None, fetch_results=True):
    """
    Executes the given SQL query on the MySQL database.

    :param query: The SQL query string to be executed with placeholders for parameters.
    :param params: Tuple of parameters to safely insert into the query.
    :param fetch_results: If True, fetches and returns the results as a DataFrame (for SELECT queries).
    :return: The results as a DataFrame if fetch_results is True, otherwise None.
    """
    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(**db_config)

        if connection.is_connected():
            cursor = connection.cursor()

            # Execute the query with parameters
            cursor.execute(query, params)

            # If fetch_results is True, fetch the results
            if fetch_results:
                columns = [desc[0] for desc in cursor.description]  # Get column names
                results = cursor.fetchall()
                return pd.DataFrame(results, columns=columns)  # Return as DataFrame

            # Commit the transaction (for INSERT, UPDATE, DELETE, etc.)
            connection.commit()
            print("Query executed successfully.")

    except Error as e:
        print(f"Error: {e}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

    return None



# def runBQuery(query, toDataFrame=True):
#     query_job = bqclient.query(query)

#     if toDataFrame:
#         return query_job.to_dataframe()
#     else:
#         return query_job

def getNextID(tableName, col=0):
    idColumn = {
        'launchpad_dqr_requests_dev': ['task_id']
        , 'launchpad_historical_query_runs_dev': ['query_run_id']
        , 'launchpad_logs_dev': ['log_id']
        , 'launchpad_runs_dev': ['run_id']
        , 'launchpad_data_push_requests_dev': ['data_push_request_id', 'data_push_id']
        , 'launchpad_schedular_request_dev': ['scheduler_id']
        , 'launchpad_schedular_on_pre_check_dev': ['pre_check_id']
    }

    query = f"""
        SELECT MAX({idColumn[tableName][col]}) as max_value 
        FROM {tableName}
    """
    query_job = runBQuery(query, True)
    max_value = query_job['max_value'].iloc[0] if not query_job.empty else 0
#     query_job = runBQuery(query, False)
#     max_ID = query_job.result()
#     max_value = [row.max_value for row in max_ID][0] or 0

    # Increment the max value
    return max_value + 1

def getDateFromShortName(shortName, isDate = False, isStr=False):
    timeIST = datetime.now()+timedelta(minutes=30, hours=5)
        
    if shortName[0] in ('d','D'):
        requireDate = timeIST-timedelta(days=int(shortName[1:]))
    elif shortName[0] in ('w','W'):
        Mdate = timeIST - timedelta(timeIST.weekday())
        requireDate = Mdate-timedelta(days=(int(shortName[1:]))*7)
    
    requireDate = requireDate.date() if isDate else requireDate
    
    requireDate = str(requireDate) if isStr else requireDate
    
    return requireDate


def changeTF(tf):
    if tf == 'TRUE':
        return True
    else:
        return False

# def getConfig(Key):
#     df = get_sheet_data(sheetName='Config')
#     val = df.loc[df['Key']=='IsQuery'].values[0][1]
#     if val in ('TRUE','FALSE'):
#         val = changeTF(val)
#     return val


def getDqrRequest(config):
    timeIST = datetime.now()+timedelta(minutes=30, hours=5)
    
    query = f"""
    SELECT *
    FROM launchpad_dqr_requests_dev
    WHERE run_on in ('{username}', 'Any', 'All')
    AND run_status not in('Completed','Disabled')
    AND NOT (run_condition='Once' and run_status != '')
    ORDER BY task_id
    """
    df = runBQuery(query)

    if len(df[df['run_status'].isin(['', 'Modified'])]) != 0:
    # if len(df[df['run_status']==''])!=0: 
        # request = df[df['run_status']==''].iloc[0].to_dict()
        request = df[df['run_status'].isin(['', 'Modified'])].iloc[0].to_dict()
        return True, request,request['task_id']
    
    failed = df[(df['run_status']=='Failed')
        & (timeIST >= (pd.to_datetime(df['updated_at'])+timedelta(minutes=config['reRunFailTaskInMinuts'])))]

    if len(failed)!=0:
        request = failed.iloc[0].to_dict()
        print(True, request, request['task_id'])
        return True, request,request['task_id']

    return False, '',0   

def isDqrRequestAvailable(status,task_id):
    timeIST = datetime.now()+timedelta(minutes=30, hours=5)
    timeIST = timeIST.strftime("%Y-%m-%d %H:%M:%S")
    print(status)
    current_pid = os.getpid()
    if status == False:
        print("into this status =false")
        # postDqrScriptFail(task_id, None)
        os.kill(current_pid, signal.SIGKILL)
    else:
        print("into query")
        query = f"""
            UPDATE launchpad_dqr_requests_dev
            SET updated_at = '{timeIST}'
                ,run_status = 'Running'
            WHERE task_id = {task_id};
        """
        runBQuery(query,False)
#         runBQuery(query)


def executeNotebookCell(notebook_name, cell_index):
    output = ''
    nb = nbformat.read(open(notebook_name), as_version=4)
    cell = nb.cells[cell_index]
        
    # Check if it's a code cell
    if cell.cell_type == 'code':
        # Execute the code in the cell
        output = ipython.run_cell(cell.source)
        
    return output


def postDqrCompleted(request,link, logger_id):
    timeIST = datetime.now()+timedelta(minutes=30, hours=5)
    timeIST = timeIST.strftime("%Y-%m-%d %H:%M:%S")
    
    query = f"""
                UPDATE launchpad_dqr_requests_dev
                SET updated_at = '{timeIST}'
                , run_status = 'Completed'
                , dqr_link = '{link}'
                WHERE task_id = {request['task_id']};
            """
    runBQuery(query,False)
#     runBQuery(query)

    query = f"""
            UPDATE launchpad_runs_dev
            SET updated_at = '{timeIST}'
            ,status = 'Completed'
            WHERE run_id = {logger_id};
        """
    runBQuery(query,False)
#     runBQuery(query)
    
    return True


def sendDqrMail(uploadFilePath,requestConfig, reportConfig , link,runConfig):
    from_addr = reportConfig['poc'][0]
    to_addr =requestConfig['email_address']
    cc = reportConfig['poc']  + ['kshiva19@ext.uber.com']
    bcc = []
    date = datetime.today().strftime('%Y-%m-%d')
    subject = f"LaunchPad Update: Successful Execution of {requestConfig['dqr_name']} (Task ID: {requestConfig['task_id']})"
    # subject = uploadFilePath.split('/')[-1]

    task_link = f"https://michelangelo-studio.uberinternal.com/session/110180d5-f2dc-46a0-9d43-b402e0cc634f/phoenix/dashboard/be175d71-779e-44eb-832e-b6cc1051ff24/task/{requestConfig['task_id']}"
    log_link = f"https://michelangelo-studio.uberinternal.com/session/110180d5-f2dc-46a0-9d43-b402e0cc634f/phoenix/dashboard/be175d71-779e-44eb-832e-b6cc1051ff24/run/{runConfig['logger_id']}"
    slack_link = f"https://uber.slack.com/app_redirect?channel=launchpad_support"

    html1 = f"""\
            <!DOCTYPE html>
            <html>
            <head></head>
            <body>
            <p style="margin: 0; padding-top: 0;">Hi,</p>
            <p style="margin: 0; padding-top: 0;">Please note that the script <strong>{requestConfig['dqr_name']}</strong> has been executed successfully.</p>
            <p>Details of the execution are as follows:</p>
            <ul>
                <li><strong>Task ID:</strong> {requestConfig['task_id']}</li>
                <li><strong>Run By:</strong> {requestConfig['run_on']}</li>
                <li><strong>Run Condition:</strong> {requestConfig['query_run_condition']}</li>
                <li><strong>Dashboard:</strong> {requestConfig['dashboard']}</li>
                <li><strong>Execution Time:</strong> {requestConfig['created_at']} - {requestConfig['updated_at']}</li>
            </ul>
            <p>Additional links for your reference:</p>
            <ul>
                <li><strong>Report Details:</strong> <a href="{link}" target="_blank">Report Link</a></li>
                <li><strong>Task Details:</strong> <a href="{task_link}" target="_blank">View Task</a></li>
                <li><strong>Logs:</strong> <a href="{log_link}" target="_blank">View Logs</a></li>
            </ul>
            <br>
            <p>If there are any issues or queries, please reach out to the <a href="{slack_link}" style="font-weight:500;" target="_blank"><strong>LaunchPad-Support</strong></a> channel for assistance.</p>
            <br>
            <p>Best regards,</p>
            <p>LaunchPad Team</p>
            </body>
            </html>
            """      

    
    body = html1 
    
    helper = pypostmaster.MailHelper()
    return helper.sendmail(from_addr, to_addr, subject, body, cc, bcc)



def sendDqrMail_completion(requestConfig, reportConfig,runConfig):
    from_addr = reportConfig['poc'][0]
    to_addr =requestConfig['email_address']
    cc = reportConfig['poc']  + ['kshiva19@ext.uber.com']
    bcc = []
    date = datetime.today().strftime('%Y-%m-%d')
    subject = f'LaunchPad Completion for the Script - {requestConfig["dqr_name"]} (Task id : {requestConfig["task_id"]})'
    # subject = f"LaunchPad Update: Successful Execution of {requestConfig['dqr_name']} (Task ID: {requestConfig['task_id']})"

    task_link = f"https://michelangelo-studio.uberinternal.com/session/110180d5-f2dc-46a0-9d43-b402e0cc634f/phoenix/dashboard/be175d71-779e-44eb-832e-b6cc1051ff24/task/{requestConfig['task_id']}"
    log_link = f"https://michelangelo-studio.uberinternal.com/session/110180d5-f2dc-46a0-9d43-b402e0cc634f/phoenix/dashboard/be175d71-779e-44eb-832e-b6cc1051ff24/run/{runConfig['logger_id']}"
    slack_link = f"https://uber.slack.com/app_redirect?channel=launchpad_support"


    html1 = f"""\
            <!DOCTYPE html>
            <html>
            <head></head>
            <body>
            <p style="margin: 0; padding-top: 0;">Hi,</p>
            <p style="margin: 0; padding-top: 0;">Please note that the script <strong>{requestConfig['dqr_name']}</strong> has been executed successfully.</p>
            <p>Details of the execution are as follows:</p>
            <ul>
                <li><strong>Task ID:</strong> {requestConfig['task_id']}</li>
                <li><strong>Run By:</strong> {requestConfig['run_on']}</li>
                <li><strong>Run Condition:</strong> {requestConfig['query_run_condition']}</li>
                <li><strong>Dashboard:</strong> {requestConfig['dashboard']}</li>
                <li><strong>Execution Time:</strong> {requestConfig['created_at']} - {requestConfig['updated_at']}</li>
            </ul>
            <p>Additional links for your reference:</p>
            <ul>
                <li><strong>Task Details:</strong> <a href="{task_link}" target="_blank">View Task</a></li>
                <li><strong>Logs:</strong> <a href="{log_link}" target="_blank">View Logs</a></li>
            </ul>
            <br>
            <p>If there are any issues or queries, please reach out to the <a href="{slack_link}" style="font-weight:500;" target="_blank"><strong>LaunchPad-Support</strong></a> channel for assistance.</p>
            <br>
            
            <p>Best regards,</p>
            <p>LaunchPad Team</p>
            </body>
            </html>
            """

    body = html1 
    helper = pypostmaster.MailHelper()
    return helper.sendmail(from_addr, to_addr, subject, body, cc, bcc)


# def sendDqrMail_completion(requestConfig, reportConfig):
#     from_addr = reportConfig['poc'][0]
#     to_addr =requestConfig['email_address']
#     cc = reportConfig['poc']   
#     bcc = []
#     date = datetime.today().strftime('%Y-%m-%d')
#     subject = f'LaunchPad Completion for the task id - {requestConfig["task_id"]}'
#     html1 = """\
#             <!DOCTYPE html>
#             <html>
#             <head></head>
#             <body>
#             <p style="margin : 0; padding-top:0;">Hi, </p>
#             <p style="margin : 0; padding-top:0;">Please note that the script {} has been executed successfully. </p>            
#             <br>   """.format(requestConfig['dqr_name'])         

    
#     body = html1 
# #     attachments = ["bar_plot.png"]
# #     attachments = [path]
    
#     helper = pypostmaster.MailHelper()

# #     print(helper.sendmail(from_addr, to_addr, subject, body, cc, bcc,attachments))
#     return helper.sendmail(from_addr, to_addr, subject, body, cc, bcc)

def runTComand(query, isListFormat=False, cwd=None, returncode=False):
    if not isListFormat:
        query = [query]

    if cwd:
        result = subprocess.run(query, capture_output=True, text=True, shell=True, cwd=cwd)
    else:
        result = subprocess.run(query, capture_output=True, text=True, shell=True, check=True)
    # print(result.stdout)
    # print(result.stderr)
    if returncode:
        return result.stdout, result.stderr, result.returncode
    else:
        return result.stdout, result.stderr


def getModifiedAndUntrackedFilesGit(repo_root):
    import re
    # Run 'git status --porcelain'
    stdout, stderr, returncode = runTComand('git status --porcelain', cwd=repo_root, returncode=True)
    if returncode != 0:
        error_message = stderr.strip() if stderr else stdout.strip()
        # Fail_mail(f"Error getting git status: {error_message}", mailID='123@example.com')
        return [], []
    
    modified_files = []
    untracked_files = []
    
    # Regex pattern to match the status code and file path
    pattern = re.compile(r'^(\s?[MADRCU?!]{1,2})\s+(.*)$')
    
    for line in stdout.strip().split('\n'):
        if not line:
            continue
        match = pattern.match(line)
        if not match:
            continue  # Skip lines that don't match the pattern
        
        status_code = match.group(1).strip()
        file_path = match.group(2)
        
        # Remove surrounding quotes if present
        file_path = file_path.strip('"')
        
        absolute_path = os.path.join(repo_root, file_path)
        relative_path = os.path.relpath(absolute_path, repo_root)
        
        # Skip files in .git directory
        if '.git' in relative_path.split(os.path.sep):
            continue
        
        if status_code in ['M', 'MM', 'AM', 'A', 'D', 'R', 'C', 'UU']:
            # Modified files
            modified_files.append(relative_path)
        elif status_code == '??':
            # Untracked files
            untracked_files.append(relative_path)
        # Handle other status codes if needed
    
    return modified_files, untracked_files


def ensureOnMainBranchGit(repo_root):
    # Check the current branch and switch to 'main' if necessary
    stdout, _ = runTComand('git rev-parse --abbrev-ref HEAD', cwd=repo_root)
    current_branch = stdout.strip()

    if current_branch != 'main':
        print(f"Switching to 'main' branch from '{current_branch}'")
        stdout, stderr = runTComand('git stash', cwd=repo_root)
        if stdout or stderr:
            print("Stashing changes:", stdout, stderr)

        stdout, stderr = runTComand('git checkout main', cwd=repo_root)
        if stderr:
            print("Error switching branches:", stderr)
            error_message = f"Error switching branches: {stderr.strip()}"
            # Fail_mail(error_message, mailID='mnizam1@ext.uber.com')

        stdout, stderr = runTComand('git stash pop', cwd=repo_root)
        if stdout or stderr:
            print("Applying stashed changes:", stdout, stderr)

def filterFilesBySize(file_list, repo_root, size_limit=100 * 1024 * 1024):
    filtered_files = []
    large_files = []
    for relative_path in file_list:
        absolute_path = os.path.join(repo_root, relative_path)
        try:
            size = os.path.getsize(absolute_path)
            if size <= size_limit:
                filtered_files.append(relative_path)
            else:
                large_files.append(relative_path)
        except OSError as e:
            print(f"Could not access file {absolute_path}: {e}")
            # Fail_mail(f"Could not access file {absolute_path}: {e}", mailID='123@example.com')
    return filtered_files, large_files


def checkGitRepoState(repo_root):
    """ Checks the state of the repository for inconsistencies. """
    stdout, stderr, returncode = runTComand('git status --porcelain', cwd=repo_root, returncode=True)
    if returncode != 0 or stdout.strip():
        # If git status shows any issues, handle them (e.g., uncommitted changes or unmerged files)
        print("Repository is in an inconsistent state.")
        # Fail_mail(f"Repository is in an inconsistent state. Status output: {stdout.strip()}", mailID='mnizam1@ext.uber.com')
        return False
    return True



def attemptGitPull(repo_root, retries=3):
    """ Attempts to pull changes with retries if there are merge conflicts. """
    for attempt in range(retries):
        stdout, stderr, returncode = runTComand('git pull -X theirs origin main', cwd=repo_root, returncode=True)
        if returncode == 0:
            print("Pull successful!")
            return True
        else:
            if "conflict" in stderr.lower():
                print(f"Merge conflict on attempt {attempt + 1}, retrying...")
                # Optionally, reset or re-fetch if needed
                runTComand('git reset --hard origin/main', cwd=repo_root)  # This will discard all local changes
            else:
                error_message = stderr.strip() if stderr else stdout.strip()
                return False
    Fail_mail("Pull failed after multiple attempts", mailID='mnizam1@ext.uber.com')
    return False

def attemptGitPush(repo_root, retries=3):
    """ Attempts to push changes with retries. Re-fetches changes if push is rejected. """
    for attempt in range(retries):
        stdout, stderr, returncode = runTComand('git push --force origin main', cwd=repo_root, returncode=True)
        if returncode == 0:
            print("Push successful!")
            return True
        else:
            if "rejected" in stderr.lower():
                print(f"Push rejected on attempt {attempt + 1}, retrying after fetching latest changes...")
                runTComand('git fetch origin', cwd=repo_root)
            else:
                error_message = stderr.strip() if stderr else stdout.strip()
                return False
    Fail_mail("Push failed after multiple attempts", mailID='mnizam1@ext.uber.com')
    return False


def autoGit(action, comment="Automated update"):
    script_dir = os.getcwd()

    repo_root = os.path.abspath(os.path.join(script_dir, '..'))

    # Ensure we are on the 'main' branch
    ensureOnMainBranchGit(repo_root)

    Auto_git_commands = {
        'pull': [
            # Avoid using 'git reset --hard'
            'git fetch origin',
            'git merge --strategy=recursive -X theirs origin/main'
            # 'git merge --strategy=ours origin/main'
        ],
        'push': [
            # handle 'git add' by getModifiedAndUntrackedFilesGit and filterFilesBySize
            f'git commit -m "{comment}"',
            'git pull --strategy=ours origin main',
            'git push --force-with-lease origin main'
        ],
        'error_push': [
            'git push origin main:error --force'
        ]
    }
    

    if action == 'push':
        # Get modified and untracked files
        modified_files, untracked_files = getModifiedAndUntrackedFilesGit(repo_root)
        
        # Combine the lists
        all_files = modified_files + untracked_files
        
        if all_files:
            # Filter files by size
            small_files, large_files = filterFilesBySize(all_files, repo_root)
            
            if small_files:
                # Stage small files for commit
                print(f"Adding {len(small_files)} files smaller than 100MB.")
                batch_size = 100  # Adjust as needed
                for i in range(0, len(small_files), batch_size):
                    batch = small_files[i:i+batch_size]
                    files_str = ' '.join(f'"{file}"' for file in batch)
                    add_command = f'git add {files_str}'
                    stdout, stderr, returncode = runTComand(add_command, cwd=repo_root, returncode=True)
                    if returncode != 0:
                        error_message = stderr.strip() if stderr else stdout.strip()
                        # Fail_mail(f"Error adding files: {error_message}", mailID='123@example.com')
                        return
                    else:
                        print(f"Added batch {i//batch_size + 1}: {len(batch)} files.")
            else:
                print("No files smaller than 100MB to add.")
                return  # No changes to commit, exit early
    
            if large_files:
                print("The following large files (>100MB) were skipped:")
                for file in large_files:
                    print(file)
        else:
            print("No modified or untracked files detected.")
            return  # No changes to commit, exit early
    
    # Execute Git commands from the repository root directory
    for query in Auto_git_commands.get(action, []):
        print(f"Executing: {query}")
        stdout, stderr, returncode = runTComand(query, cwd=repo_root, returncode=True)
        if stdout:
            print(f"Output: {stdout}")
        if returncode != 0:
            error_message = stderr.strip() if stderr else stdout.strip()
            print(f"Error executing '{query}': {error_message}")
            # Fail_mail(f"Error executing '{query}': {error_message}", mailID='123@example.com')
        else:
            # Command succeeded; handle outputs
            combined_output = (stdout + stderr).strip()
            if combined_output:
                benign_messages = [
                    'From github.com:',
                    'To github.com:',
                    'Already up to date.',
                    'Fast-forward',
                    '(forced update)'
                ]
                if any(msg in combined_output for msg in benign_messages):
                    print(f"Non-error message from '{query}': {combined_output}")
                else:
                    print(f"Output from '{query}': {combined_output}")

        time.sleep(2) 

    if not checkGitRepoState(repo_root):
        if action == 'push':
            attemptGitPush(repo_root, retries=3)
        elif action == 'pull':
            attemptGitPull(repo_root, retries=3)
        
        
        



def postDqrScriptFail(task_id, logger_id):
    timeIST = datetime.now()+timedelta(minutes=30, hours=5)
    timeIST = timeIST.strftime("%Y-%m-%d %H:%M:%S")
    current_pid = os.getpid()
    query = f"""
                UPDATE launchpad_dqr_requests_dev
                SET updated_at = '{timeIST}'
                ,run_status = 'Failed'
                WHERE task_id = {task_id};
            """
    runBQuery(query,False)
#     runBQuery(query)
    
    if logger_id:
        query = f"""
                UPDATE launchpad_runs_dev
                SET updated_at = '{timeIST}'
                ,status = 'Failed'
                WHERE run_id = {logger_id};
            """
        runBQuery(query,False)
#         runBQuery(query)
    
    # autoGit('error_push')   
    os.kill(current_pid, signal.SIGKILL)


def CreateDqrLogger(request,username):
    # run_id = getNextID('launchpad_runs')
    task_id = request['task_id']
    timeIST = datetime.now()+timedelta(minutes=30, hours=5)
    timeIST = timeIST.strftime("%Y-%m-%d %H:%M:%S")
    query = f"""
    INSERT INTO launchpad_runs_dev (task_id,running_on, status)
    VALUES ({task_id}, '{username}', 'Running')
    """
    # query = f"""
    # INSERT INTO launchpad_runs (run_id, task_id, created_at, updated_at, running_on, status)
    # VALUES ({run_id}, {task_id}, '{timeIST}', '{timeIST}', '{username}', 'Running')
    # """
    runBQuery(query,False)
    
    query_select = f"""
        SELECT run_id FROM launchpad_runs_dev
        WHERE task_id = {task_id} AND running_on = '{username}' AND status = 'Running' 
    """

    run_id = runBQuery(query_select)
    # run_id = run_id_df.iloc[0]['run_id']
    print(run_id.iloc[0]['run_id'])
    return run_id.iloc[0]['run_id']


def resetDqrLongRunningTask():
    resetMin = masterConfig['resetLongRunningTaskInMinuts']
    query = f"""
        SELECT * FROM launchpad_runs_dev
        WHERE TIMESTAMPDIFF(MINUTE, 
                            TIMESTAMP(updated_at, 'Asia/Kolkata'), 
                            CURRENT_TIMESTAMP()) >= {resetMin}
        AND status = 'Running'
        ORDER BY run_id DESC
    """
    # query = f"""
    #             SELECT * FROM launchpad_runs
    #             WHERE DATEDIFF(
    #                     CURRENT_TIMESTAMP(), 
    #                     TIMESTAMP(updated_at, "Asia/Kolkata"), 
    #                     MINUTE
    #                   ) >= {resetMin}
    #             AND status = 'Running'
    #             order by run_id desc
    #         """

    # query = f"""
    #             SELECT * FROM launchpad_runs
    #             WHERE TIMESTAMP_DIFF(
    #                     CURRENT_TIMESTAMP(), 
    #                     TIMESTAMP(updated_at, "Asia/Kolkata"), 
    #                     MINUTE
    #                   ) >= {resetMin}
    #             AND status = 'Running'
    #             order by run_id desc
    #         """
    runs =runBQuery(query)
    # print(runs)
    
    if runs is None:
        print("Query returned no results.")
        return True
    
    if len(runs)==0:
        return True
    elif len(runs['task_id'].unique())==1:
        task_ids = tuple(runs['task_id'].unique())
        task_ids = f"({task_ids[0]})"
    else: 
        task_ids = tuple(runs['task_id'].unique())

    query = f"""
                UPDATE launchpad_dqr_requests_dev
                SET run_status = 'Failed'
                WHERE task_id in {task_ids}
                AND run_status in ('Running', '',null)
            """
    runBQuery(query,False)
#     runBQuery(query)

    if len(runs)==1:
        run_ids = tuple(runs['run_id'].unique())
        run_ids = f"({run_ids[0]})"
    else: 
        run_ids = tuple(runs['run_id'].unique())
        
    query = f"""
                UPDATE launchpad_runs_dev
                SET status = 'Failed'
                WHERE run_id in {run_ids}
                AND status in ('Running', '',null)
            """
    runBQuery(query,False)
#     runBQuery(query)
    

    return True

def maxDqrTaskChecker(username):
    maxTask = masterConfig['maxTaskPerSession']
    query = f"""
                SELECT COUNT(*) FROM launchpad_runs_dev
                WHERE running_on = '{username}'
                AND status = 'Running'
            """
    x = runBQuery(query)
    if x.iloc[0][0] <maxTask:
        return True
    else:
        current_pid = os.getpid()
        os.kill(current_pid, signal.SIGKILL)
        
def resetDqrForTesting(task_id):
    timeIST = datetime.now()+timedelta(minutes=30, hours=5)
    timeIST = timeIST.strftime("%Y-%m-%d %H:%M:%S")

    query = f"""
                UPDATE launchpad_dqr_requests_dev
                SET updated_at = '{timeIST}'
                ,run_status = ''
                WHERE task_id = {task_id};
            """
    runBQuery(query,False)
#     runBQuery(query)


def postDqrAutoLog(output, cellNumber, notebook_name, reportConfig, requestConfig, postLog):
    if output=='':
        print('non code cell')
    elif not output.success:
        rawCellContent = str(output.info.raw_cell).replace('\n','<br>')
        
        if output.error_before_exec!=None:
            # print('send mail to the POC that the script has some syntax error')
            Fail_mail(error = f'''The Script has error before execution <br>
                    Cell Number: {cellNumber} of {notebook_name}<br>
                    Line Number: {output.error_before_exec.lineno} <br>
                    Error Has Occurred In This Line Of Code: {output.error_before_exec.text} <br>
                    Raw Code Of The Cell : <br>
                    <p>{rawCellContent}</p>''', mailID= reportConfig['poc'])

            
            postLog(f'''Cell Number: {cellNumber} of {notebook_name}<br>
                    Line Number: {output.error_before_exec.lineno} <br>
                    Error Has Occurred In This Line Of Code: {output.error_before_exec.text} <br>
                    Raw Code Of The Cell : <br>
                    <p>{rawCellContent}</p>''', LogType='ERROR')
            
        elif output.error_in_exec!=None:
            # print('send mail to the POC that the task that there is some execution error')
            Fail_mail(error = f'''The Script has error during execution <br>
                    Cell Number: {cellNumber} of {notebook_name} <br>
                        Error: {str(output.error_in_exec)} <br>
                        Raw Code Of The Cell : <br>
                        <p>{rawCellContent}</p>''', mailID= requestConfig['email_address'])

            
            postLog(f'''Cell Number: {cellNumber} of {notebook_name} <br>
                        Error: {str(output.error_in_exec)} <br>
                        Raw Code Of The Cell : <br>
                        <p>{rawCellContent}</p>''', LogType='ERROR')
    elif output.success:
        print(output)
        result = str(output.result).replace('\n','<br>').replace('\\','')
        print("Results\n")
        print(result)
        if (result is not None) and (result!='None'):
            if len(result)< 512:
                postLog(f'AutoLog Of Cell {cellNumber} : <p>{result}</p>')
            else:
                postLog(f'AutoLog Of Cell {cellNumber} : <p style="color:red"> output of the cell is too long</p>')
        






# def uploadFileToDrive(uploadFilePath, fileName, folderId,convert=True):
#     sheetAccess,drive = validateDriveAccess(isSheetAccess = False, isDriveAccess=True)
#     gfile = drive.CreateFile({'parents' : [{'id' : folderId}], 'title' : fileName})
#     # Read file and set it as the content of this instance.
#     gfile.SetContentFile(uploadFilePath)
#     gfile.Upload({'convert': convert}) # Upload the file.
    
    
#     filesID =  gfile.values().mapping['id']
#     link = gfile.values().mapping['alternateLink']
    
#     return gfile.uploaded,filesID,link


def uploadFileToDrive(uploadFilePath, fileName, folderId,drive,convert=True):
    gfile = drive.CreateFile({'parents' : [{'id' : folderId}], 'title' : fileName})
    # Read file and set it as the content of this instance.
    gfile.SetContentFile(uploadFilePath)
    gfile.Upload({'convert': convert}) # Upload the file.
    
    
    filesID =  gfile.values().mapping['id']
    link = gfile.values().mapping['alternateLink']
    
    return gfile.uploaded,filesID,link



def getQueryUUID(reportID,parameters,QueryRunCondition, postLog):
    
    try:
        QRC = masterConfig['QueryRunCondition'][QueryRunCondition]
    except Exception as e:
        postLog('Query Run Condition is in correct : '+str(e) , LogType='ERROR')
        return False, ''
    
    if QRC in ('F'):
        return False, ''
    elif QRC in ('D'):
        runAfter = datetime(2023, 8, 23)
    else:
        runAfter = getDateFromShortName(QRC)

    runAfter = runAfter.strftime("%Y-%m-%d %H:%M:%S")
    query = f"""
        SELECT uuid FROM launchpad_historical_query_runs_dev
        WHERE report_id = '{reportID}'
        AND parameters = '''{parameters}'''
        AND created_at >= '{runAfter}' 
        order by created_at desc
        limit 1;

    """
    uuid = runBQuery(query)

    if len(uuid)>0:
        uuid = uuid['uuid'][0]
        postLog('Using given query run : '+str(uuid),LogType='INFO')
        return True, uuid
        
    else:
        return False, ''




def getEncodedImg(filePath):

    # Read the image and encode it to base64
    with open(filePath, 'rb') as image_file:
        encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
    return encoded_string


def getDpvQueryReportID(geo):
    query_link_mapping = {
                            "US Rides": ["rMwMNbwR5","xSNvmrQJB"],
                            "US Eats": ["GnHZs8S4z","uRPSgOTC3"],
                            "Brazil Rides": ["dfl3jc7T5", "yGClmw2I3"],
                            "Mexico Rides": ["C0TtQuxrB", "EimCJgxuH"],
                            "Canada Eats": ["822mVtZfB", "4ZomLCjR1"],
                            "Japan Eats": ["IdEMzOKIz", "v42nxgNph"],
                            "Taiwan Eats": ["5XGBaYvez", "pLFfZJFbt"],
                            "Argentina Rides" : ["ARhOgQKO7", "wsozy4OpR"]
                         }
    if geo in query_link_mapping:
        return query_link_mapping[geo]
    else:
        return None






def getDataPushRequest():
    timeIST = datetime.now()+timedelta(minutes=30, hours=5)
    
    query = f"""
    SELECT 
        * 
    FROM 
        launchpad_data_push_requests_dev
    WHERE 
      current_status not in ('Completed','Disabled','Trigger', 'DPV in Progress')
    Order by data_push_id asc
    """
    df = runBQuery(query)
    requests = []
    if len(df[df['current_status']==''])!=0:
        request = df[df['current_status']==''].to_dict('records')
        requests.extend(request)
    
    failed = df[(df['current_status']=='Failed')
        & (timeIST >= (pd.to_datetime(df['updated_at'])+timedelta(minutes=2)))]
    if len(failed)!=0:
        request = failed.to_dict('records')
        requests.extend(request)
    
    if len(requests)!=0: 
        return True, requests
    else:
        return False, ''



def triggerDataPush(username, flow, run_id, data_push_id):
    print('data_push_id: ', data_push_id)
    timeIST = datetime.now()+timedelta(minutes=30, hours=5)
    timeIST = timeIST.strftime("%Y-%m-%d %H:%M:%S")
    
    tcQuery = """yab -s steering-wheel --caller yab-{username} --grpc-max-response-size 20971520 --request '{{"flow_type":"{flow}","run_id":"{run_id}","rollback_version":false}}' --procedure 'uber.marketplace.global_intelligence.steeringwheel.VersionControl/PublishVersionControl' --header 'x-uber-source:studio' --header 'studio-caller:{username}' --header 'x-uber-uuid:8fbdc0a3-505b-4e59-a513-3039933078dc' --header 'jaeger-debug-id:api-explorer-{username}' --header 'uberctx-tenancy:uber/testing/api-explorer/41ad-4b52-8c26-7bed844d59a7' --peer '127.0.0.1:5435' --timeout 30000ms""".format(username=username,flow=flow, run_id=run_id)
    print(tcQuery)
    stdout, stderr = runTComand(str(tcQuery))
    # stdout, stderr = runTComand(str('ls'))
    run = {'flow': flow
           , 'run_id': run_id
           , 'stdout' : stdout
           , 'stderr' : stderr
          }
    print(len(stderr))
    if len(stderr)==0:
        query = f"""
                UPDATE launchpad_data_push_requests_dev
                SET updated_at = '{timeIST}'
                    , data_push_trigger_at = '{timeIST}'
                    , current_status = 'Trigger'
                    , data_push_stdout = '''{stdout}'''
                    , data_push_stderr = '''{stderr}'''
                WHERE data_push_id = {data_push_id};
            """
    else:
        query = f"""
                UPDATE launchpad_data_push_requests_dev
                SET updated_at = '{timeIST}'
                    , data_push_trigger_at = '{timeIST}'
                    , current_status = 'Failed'
                    , data_push_stdout = '''{stdout}'''
                    , data_push_stderr = '''{stderr}'''
                WHERE data_push_id = {data_push_id};
            """
    
    runBQuery(query,False)
#     runBQuery(query)
    
    time.sleep(random.randint(1, 5))
    return True


def getDataPushReportRequest():
    timeIST = datetime.now()+timedelta(minutes=30, hours=5)
    
    query = f"""
    SELECT 
        * 
    FROM 
        launchpad_data_push_requests_dev
    WHERE 
         current_status = 'Trigger'
    Order by data_push_request_id asc
    """
    df = runBQuery(query)
    df['data_push_trigger_at'] = pd.to_datetime(df['data_push_trigger_at'])
    data_push_trigger_at = df.groupby('data_push_request_id')['data_push_trigger_at'].max()
    for id in df['data_push_request_id'].unique():
        if timeIST >= data_push_trigger_at[id]+timedelta(minutes=25):
            query = f"""
                UPDATE launchpad_data_push_requests_dev
                SET updated_at = '{timeIST}'
                    , current_status = 'DPV in Progress'
                WHERE data_push_request_id = {id};
            """
            runBQuery(query,False)
#             runBQuery(query)
            return True, df[df['data_push_request_id']==id].to_dict('records')
    return False, ''


def sendDpvMail(uploadFilePath, link, user_email, to_addr, geo):
    from_addr = user_email
    to_addr = to_addr
    cc = ['mnizam1@ext.uber.com']
    bcc = []
    date = datetime.today().strftime('%Y-%m-%d')
    subject = uploadFilePath.split('/')[-1]
    html1 = """\
            <!DOCTYPE html>
            <html>
            <head></head>
            <body>
            <p style="margin : 0; padding-top:0;">Hi, </p>
            <p style="margin : 0; padding-top:0;">Please find the latest data push validation report of {} and you can find it here {} </p>            
            <br>   """.format(geo,link)         

    
    body = html1 
#     attachments = ["bar_plot.png"]
#     attachments = [path]
    
    helper = pypostmaster.MailHelper()

#     print(helper.sendmail(from_addr, to_addr, subject, body, cc, bcc,attachments))
    return helper.sendmail(from_addr, to_addr, subject, body, cc, bcc)

def postDpvCompleted(data_push_request_id, link):
    timeIST = datetime.now()+timedelta(minutes=30, hours=5)
    timeIST = timeIST.strftime("%Y-%m-%d %H:%M:%S")
    
    query = f"""
                UPDATE launchpad_data_push_requests_dev
                SET updated_at = '{timeIST}'
                , current_status = 'Completed'
                , dqr_link = '{link}'
                WHERE data_push_request_id = {data_push_request_id};
            """
    runBQuery(query,False)
#     runBQuery(query)
    return True

def getData(report,parameters ={}):
    try:
        user_email = os.getenv('USER_EMAIL_ID')
    except:
        user_email = username+"@ext.uber.com"
    qr = Client(user_email=user_email)
    
    cursor = qr.execute_report(report, parameters=parameters)#, datacenter='phx2')   
    uuid = cursor.execution_uuid
    data = cursor.to_pandas()

    return data


def getHistoricalScheduler():
    query = '''
        SELECT * FROM launchpad_schedular_request_dev
        WHERE is_active in (1, '1')
        order by scheduler_id 
    '''
    df = runBQuery(query)

    # df['report_status'] = df['dqr_link'].apply(lambda x: '' if x in (None, '') else 'Completed')
    df['cronEcpresssion'] = df[['cron_minute', 'cron_hour', 'cron_day', 'cron_month', 'cron_weekday']].apply(
        lambda x: ' '.join(x), axis=1)
    df = df.to_dict('records')
    return df


def isScheduled(last_executed_at, cronEcpresssion):
    last_executed_at = pd.to_datetime(last_executed_at)
    ist_now = datetime.utcnow() + timedelta(hours=5, minutes=30)
    iter = croniter(cronEcpresssion, ist_now)
    next_execution = iter.get_next(datetime)
    prev_execution = iter.get_prev(datetime)

    if (last_executed_at <prev_execution):
        return True
    else:
        return False

def nextRunin(cronEcpresssion):
    ist_now = datetime.utcnow() + timedelta(hours=5, minutes=30)

    iter = croniter(cronEcpresssion, ist_now)
    next_execution = iter.get_next(datetime)
    nextIn = next_execution-ist_now
    print(nextIn)
    
    seconds = nextIn.seconds
    days = nextIn.days
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    runNextiN = ''
    if days!=0:
        runNextiN += str(days)+'days '
    if hours!=0:
        runNextiN += str(hours)+'h '
    if minutes!=0:
        runNextiN += str(minutes)+'min'
    if runNextiN=='':
        runNextiN = '0min'
    return runNextiN


def SchedulesToPrecheck(schedule):
    utc_now = datetime.utcnow() + timedelta(hours=5, minutes=30)
    currentTime = utc_now.strftime("%Y-%m-%d %H:%M:%S")
    # pre_check_id = getNextID('launchpad_schedular_on_pre_check')
    
    scheduler_id = schedule['scheduler_id']
    reportId = schedule['getRunId_querybuilder']
    print(schedule['parameters'])
    parameters_json = schedule['parameters']
    # query = f'''
    # INSERT INTO launchpad_schedular_on_pre_check_dev (scheduler_id, getRunId_querybuilder, run_status, runIds,past_checked_at,parameters)
    # VALUES ({scheduler_id},'{reportId}', '', '',NULL,'{parameters_json}');
    # '''

    # # query = f'''
    # # INSERT INTO launchpad_schedular_on_pre_check (pre_check_id, scheduler_id, created_at, updated_at, getRunId_querybuilder, run_status, runIds, past_checked_at)
    # # VALUES ({pre_check_id},{scheduler_id}, '{currentTime}','{currentTime}','{reportId}', '', '', '');
    # # '''
    # print(query)
    # runBQuery(query,False)
    query = '''
        INSERT INTO launchpad_schedular_on_pre_check_dev 
        (scheduler_id, getRunId_querybuilder, run_status, runIds, past_checked_at, parameters)
        VALUES (%s, %s, %s, %s, %s, %s);
    '''
    
    # Parameters for the query
    params = (
        scheduler_id,      # scheduler_id
        reportId,          # getRunId_querybuilder
        '',                # run_status
        '',                # runIds
        None,              # past_checked_at (NULL)
        parameters_json    # parameters (as JSON string)
    )
    
    # Execute the query
    runBQuery_json(query, params, fetch_results=False)


    query = f'''
            UPDATE launchpad_schedular_request_dev SET last_executed_at = '{currentTime}'
                WHERE scheduler_id = {scheduler_id}
        '''
    print(query)
    runBQuery(query,False)
    return True

def getThisWeekStartEnd():
    ist_now = datetime.utcnow() + timedelta(hours=5, minutes=30) #- timedelta(days=7)
    weekDay = ist_now.weekday()
    start = ist_now - timedelta(days = weekDay)
    end = start + timedelta(days = 6)
    start = str(start.date())
    end = str(end.date())
    return start, end


def getSchedulerForPreCheck():
    utc_now = datetime.utcnow() + timedelta(hours=5, minutes=30)
    currentTime = utc_now.strftime("%Y-%m-%d %H:%M:%S")
    
    checkBy = utc_now-timedelta(minutes=20)
    checkBy_str = checkBy.strftime("%Y-%m-%d %H:%M:%S") 
    query = f'''
            SELECT * FROM launchpad_schedular_on_pre_check_dev
            WHERE run_status != 'Completed'
            AND (
                past_checked_at IS NULL OR 
                past_checked_at < '{checkBy_str}'
            )
        '''
    # query = f'''
    #     SELECT * FROM launchpad_schedular_on_pre_check
    #     WHERE run_status != 'Completed'
    #     and ((past_checked_at = '' )or (DATETIME(past_checked_at)<DATETIME('{checkBy}')) or (past_checked_at is NULL ))
    # '''
    print(query)
    df = runBQuery(query)
    print(df)
    return df.to_dict('records')

def postReportRequest(dqr_name, system, run_con, query_run,
                      email, runids,dashboard,parameters):
    utc_now = datetime.utcnow() + timedelta(hours=5, minutes=30)
    currentTime = utc_now.strftime("%Y-%m-%d %H:%M:%S")
    # task_id = getNextID('launchpad_dqr_requests')

    # query = f'''
    #     INSERT INTO launchpad_dqr_requests (task_id, created_at, dqr_name, run_on, run_condition, run_status, updated_at, dqr_link, email_address, query_run_condition, run_id, dashboard)
    #     VALUES ({task_id}, '{currentTime}', '{dqr_name}', '{system}', '{run_con}', '', '{currentTime}', '', '{email}', '{query_run}', '{runids}', 'gi_gss');
    # '''
    # parameters_json = json.dumps(parameters).replace("'", "''")
    # query = f'''
    #     INSERT INTO launchpad_dqr_requests_dev (dqr_name, run_on, run_condition, run_status,dqr_link, email_address, query_run_condition, run_id, dashboard,parameters)
    #     VALUES ('{dqr_name}', '{system}', '{run_con}', '', '', '{email}', '{query_run}', '{runids}', '{dashboard}','{parameters}');
    # '''
    
    # print(query)
    # runBQuery(query,False)
    parameters = cleanMarkup(parameters)
    # parameters = sanitize_parameters(parameters)
    parameters_json = html.unescape(json.dumps(parameters))
    query = '''
        INSERT INTO launchpad_dqr_requests_dev 
        (created_at, dqr_name, run_on, run_condition, run_status, updated_at, dqr_link, 
         email_address, query_run_condition, run_id, dashboard, parameters)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    '''
    
    # Parameters for the query
    params = (
        currentTime,  # created_at
        dqr_name,     # dqr_name
        system,       # run_on
        run_con,      # run_condition
        '',           # run_status
        currentTime,  # updated_at
        '',           # dqr_link
        email,        # email_address
        query_run,    # query_run_condition
        runids,        # run_id
        dashboard,  # dashboard
        parameters_json  # parameters (as JSON string)
    )
    
    # Execute the query
    runBQuery_json(query, params, fetch_results=False)

#     runBQuery(query)
    return True



def cleanMarkup(input_dict):
    cleaned_dict = {}
    for key, value in input_dict.items():
        if isinstance(value, str):
            pattern = r"Markup\('([^']*)'\)"
            match = re.search(pattern, value)
            if match:
                cleaned_value = match.group(1)
                cleaned_dict[key] = cleaned_value
            else:
                cleaned_dict[key] = value
        else:
            cleaned_dict[key] = value
    return cleaned_dict

def postSchedulerToDqrRequest(preCheck, dashboard):
    utc_now = datetime.utcnow() + timedelta(hours=5, minutes=30)
    current_time = utc_now.strftime("%Y-%m-%d %H:%M:%S")
    partition_id_start, partition_id_end = getThisWeekStartEnd()

    # parameters = json.loads(preCheck['parameters'])
    if preCheck['parameters'] is None:
        parameters = {}  # Assign an empty dictionary to avoid errors
    else:
        parameters = json.loads(preCheck['parameters'])

    dashboard_name = dashboard.upper().replace('_', '-') if dashboard in ('gi_gss', 'mi_gss') else dashboard
    
    # Get dashboard ID
    query = f"""
        SELECT dashboard_id
        FROM launchpad_dim_dashboard_dev
        WHERE dashboard_name = '{dashboard_name}'
    """
    dashboard_id = runBQuery(query, True).iloc[0, 0]

    # Get parameter details
    param_keys = list(parameters.keys())
    print(f'param keys: {param_keys}')
    if param_keys:
        query = f"""
            SELECT variable_name, exec_type, column_name
            FROM launchpad_dim_parameter_dev
            WHERE variable_name IN ({', '.join(f"'{key}'" for key in param_keys)}) AND dashboard_id = {dashboard_id} AND form_name='script_scheduler'
        """
    
        query_result = runBQuery(query,True)
        print(query_result)
        param_type = {
            row.variable_name: [row.exec_type, row.column_name]
            for row in runBQuery(query, True).itertuples(index=False)
        }
        # param_type = {
        #     row.short_name: [row.exec_type, row.variable_name if row.variable_name else None, row.column_name]
        #     for row in runBQuery(query, True).itertuples(index=False)
        # }
        # Separate parameters by exec_type
        text_params = {k: v for k, v in parameters.items() if param_type.get(k, [None])[0] == 'text'}
        qb_report_params = {k: v for k, v in parameters.items() if param_type.get(k, [None])[0] == 'qb_report'}
        print(f'text_params = {text_params}')
        print(f'qb_report_params = {qb_report_params}')
        # Process text parameters with error handling
        text_dict = {}
        for k, v in text_params.items():
            try:
                # Since the value is plain text, unescape it directly
                text_dict[k] = html.unescape(v)
            except Exception as e:
                print(f"Error processing key '{k}': {v}. Error: {e}")
        print("Decoded Text Parameters:", text_dict)

    runId=''

    # Process QB report parameters
    to_dqr = True
    if param_keys and qb_report_params:
        for param_key, value in qb_report_params.items():
            if value not in ('', 'None', None):
                # Update pre-check status
                update_query = f"""
                    UPDATE launchpad_schedular_on_pre_check_dev
                    SET updated_at = '{current_time}', run_status = 'Running'
                    WHERE pre_check_id = {preCheck['pre_check_id']}
                """
                runBQuery(update_query, False)
    
                variable_name = param_key
                store = param_key
                if dashboard_id == 1:
                    parameters_qb = {'partition_id_start': partition_id_start, 'partition_id_end': partition_id_end}
                    # for now keeping this as the runId column exister=d
                    runIds = getData(preCheck['getRunId_querybuilder'], parameters =parameters_qb)
                    runId = runIds['partition_id'].to_list()
                    if len(runId)!=0:
                        runId = ', '.join(runId)
                    else:
                        toDqr = False
                        runId = ''
                    # we can use the below one to get the parameters format 
                    variable_data = getData(value, parameters=parameters_qb)
                    print(variable_data)
                else:
                    runId=''
                    variable_data = getData(value)
    
                # Convert the first column to a list and join as comma-separated values
                column_name = param_type.get(param_key, [None])[1]
                first_column_values = variable_data[column_name].to_list()
                variable_name = ', '.join(map(str, first_column_values)) if first_column_values else ''
    
                # Check if there are any values to proceed
                if not first_column_values:
                    to_dqr = False
    
                # Store results in parameters dictionary
                parameters[store] = variable_name


    # Update the pre-check status based on success or failure
    status = 'Completed' if to_dqr else 'Failed'
    print(f'status: {status}')
    # parameters_json = html.unescape(json.dumps(parameters))
    parameters_json = html.escape(json.dumps(parameters))
    # parameters_json = html.escape(parameters)
    print("\n")
    print(f'json param:{parameters_json}')
    update_query = f"""
        UPDATE launchpad_schedular_on_pre_check_dev
        SET updated_at = '{current_time}', past_checked_at = '{current_time}',
        run_status = '{status}'
        WHERE pre_check_id = {preCheck['pre_check_id']}
    """
    runBQuery(update_query, False)
    # Proceed with DQR if necessary
    if to_dqr:
        schedular_query = f"""
            SELECT * FROM launchpad_schedular_request_dev
            WHERE scheduler_id = {preCheck['scheduler_id']}
        """
        schedular_task = runBQuery(schedular_query).to_dict('records')[0]
        print(parameters)
        postReportRequest(
            schedular_task['dqr_name'], schedular_task['run_on'], schedular_task['run_condition'],
            schedular_task['query_run_condition'], schedular_task['email_address'], runId, dashboard, parameters
        )

    return to_dqr


    


# def postSchedulerToDqrRequest(preCheck,dashboard):
#     utc_now = datetime.utcnow() + timedelta(hours=5, minutes=30)
#     currentTime = utc_now.strftime("%Y-%m-%d %H:%M:%S")
#     partition_id_start, partition_id_end = getThisWeekStartEnd()

#     toDqr = True
#     runId = ''
#     if preCheck['getRunId_querybuilder'] not in ('', 'None', None):
#         query = f'''
#                 UPDATE launchpad_schedular_on_pre_check_dev SET updated_at = '{currentTime}',run_status = 'Running'
#                     WHERE pre_check_id = {preCheck['pre_check_id']}
#             '''
#         print(query)
#         runBQuery(query,False)
# #         runBQuery(query)
        
#         parameters = {'partition_id_start':partition_id_start, 'partition_id_end':partition_id_end}
#         runIds = getData(preCheck['getRunId_querybuilder'], parameters =parameters)
#         runId = runIds['partition_id'].to_list()
#         if len(runId)!=0:
#             runId = ', '.join(runId)
#         else:
#             toDqr = False
#             runId = ''

#     if toDqr:
#         query = f'''
#                 SELECT * from launchpad_schedular_request_dev Where scheduler_id = {preCheck['scheduler_id']}
#             '''
#         print(query)
#         schedularTask = runBQuery(query)
#         schedularTask = schedularTask.to_dict('records')[0]
#         dqr_name = schedularTask['dqr_name']
#         system = schedularTask['run_on']
#         run_con = schedularTask['run_condition']
#         query_run = schedularTask['query_run_condition']
#         email = schedularTask['email_address']
#         runids = runId
#         print(dashboard)
#         postReportRequest(dqr_name, system, run_con, query_run, email, runids,dashboard)
        
#         query = f'''
#                 UPDATE launchpad_schedular_on_pre_check_dev SET updated_at = '{currentTime}', past_checked_at = '{currentTime}'
#                     ,run_status = 'Completed', runIds = '{runId}'
#                     WHERE pre_check_id = {preCheck['pre_check_id']}
#             '''
#         print(query)
#         runBQuery(query,False)
# #         runBQuery(query)
#         return True

#     else:
#         query = f'''
#                 UPDATE launchpad_schedular_on_pre_check_dev SET updated_at = '{currentTime}', past_checked_at = '{currentTime}'
#                     ,run_status = 'Failed'
#                     WHERE pre_check_id = {preCheck['pre_check_id']}
#             '''
#         print(query)
#         runBQuery(query,False)
# #         runBQuery(query)
#         return False 


timestamp_file = 'timestamps.json'
def load_timestamps_file():
    if not os.path.exists(timestamp_file):
        return {}
    with open(timestamp_file, 'r') as file:
        return json.load(file)

def save_timestamps_file(timestamps):
    with open(timestamp_file, 'w') as file:
        json.dump(timestamps, file)

def should_run_task(task_name, frequency='daily'):
    if frequency is None:
        return True
    timestamps = load_timestamps_file()
    last_run_date = timestamps.get(task_name, None)
    current_date = datetime.now().strftime('%Y-%m-%d')
    
    if last_run_date is None:
        return True

    last_run_datetime = datetime.strptime(last_run_date, '%Y-%m-%d')

    if frequency == 'daily':
        return last_run_datetime.date() < datetime.now().date()
    elif frequency == 'weekly':
        return datetime.now() - last_run_datetime >= timedelta(weeks=1)
    elif frequency == 'monthly':
        last_run_month = last_run_datetime.year * 12 + last_run_datetime.month
        current_month = datetime.now().year * 12 + datetime.now().month
        return current_month > last_run_month
    else:
        raise ValueError("Unsupported frequency. Use 'daily', 'weekly', or 'monthly'.")

def update_timestamp_file(task_name):
    timestamps = load_timestamps_file()
    current_date = datetime.now().strftime('%Y-%m-%d')
    timestamps[task_name] = current_date
    save_timestamps_file(timestamps)


def calculate_priority(task_id,dqr_name, pocs):
    # Convert the list of pocs to a string for SQL IN clause for requesttask
    pocs_str = "', '".join(pocs)
    query = f"""
    WITH FilteredData AS (
        SELECT
            created_at,
            dqr_name,
            run_status,
            run_on,
            updated_at
        FROM launchpad_dqr_requests_dev
        WHERE run_status = 'Completed'
          AND DATEDIFF(
                CURRENT_TIMESTAMP(),
                STR_TO_DATE(created_at, '%Y-%m-%d %H:%i:%s')
            ) < 40
          AND run_on IN ('{pocs_str}')
          AND dqr_name = '{dqr_name}'
    ),
    RankedData AS (
        SELECT
            run_on,
            dqr_name,
            DENSE_RANK() OVER (
                PARTITION BY dqr_name
                ORDER BY COUNT(*) DESC
            ) AS rank_value
        FROM FilteredData
        GROUP BY run_on, dqr_name
    )
    SELECT *
    FROM RankedData
    WHERE run_on IN ('{pocs_str}')
    ORDER BY dqr_name, rank_value;
    """
    df = runBQuery(query)
    print("df")
    print(df)
    df['rank'] = df['rank_value']
    if df is None:
        print("None here")
        df = pd.DataFrame()
    if df.empty:
        print("empty here")
        # If empty, assign all POCs a rank of 1
        df = pd.DataFrame({
            'run_on': pocs,
            'dqr_name': dqr_name,
            'rank': 1
        })
        return df
    else:
        print("no empty here")
        # Identify existing POCs
        existing_pocs = df['run_on'].tolist()
        missing_pocs = [poc for poc in pocs if poc not in existing_pocs]
        # If there are missing POCs, add them with the next priority rank
        if missing_pocs:
            print("existace of miss pocs")
            next_priority_rank = df['rank'].max() + 1  # Next rank after the last one
            # Create a DataFrame for the missing POCs with the next rank
            missing_df = pd.DataFrame({
                'run_on': missing_pocs,
                'dqr_name': dqr_name,
                'rank': next_priority_rank
            })
            df = pd.concat([df, missing_df], ignore_index=True)
            print(df)
        return df




# def calculate_priority(task_id,dqr_name, pocs):
#     # Convert the list of pocs to a string for SQL IN clause for requesttask
#     pocs_str = "', '".join(pocs)
    
#     query = f"""
#     WITH FilteredData AS (
#         SELECT
#             created_at,
#             dqr_name,
#             run_status,
#             run_on,
#             updated_at
#         FROM launchpad_dqr_requests_test
#         WHERE run_status = 'Completed'
#           AND TIMESTAMP_DIFF(
#                 CURRENT_TIMESTAMP(), 
#                 TIMESTAMP(created_at, "Asia/Kolkata"), 
#                 DAY
#               ) < 40
#           AND run_on IN ('{pocs_str}')
#           AND dqr_name = '{dqr_name}'
#     ),
#     RankedData AS (
#         SELECT
#             run_on,
#             dqr_name,
#             DENSE_RANK() OVER (
#                 PARTITION BY dqr_name
#                 ORDER BY COUNT(*) DESC
#             ) AS rank
#         FROM FilteredData
#         GROUP BY run_on, dqr_name
#     )
#     SELECT *
#     FROM RankedData
#     WHERE run_on IN ('{pocs_str}')
#     ORDER BY dqr_name, rank;
#     """

#     df = runBQuery(query)
#     if df is None:
#         df = pd.DataFrame()
#     if df.empty:
#         # If empty, assign all POCs a rank of 1
#         df = pd.DataFrame({
#             'run_on': pocs,
#             'dqr_name': dqr_name,
#             'rank': 1
#         })
#         return df
#     else:
#         # Identify existing POCs
#         existing_pocs = df['run_on'].tolist()
#         missing_pocs = [poc for poc in pocs if poc not in existing_pocs]

#         # If there are missing POCs, add them with the next priority rank
#         if missing_pocs:
#             next_priority_rank = df['rank'].max() + 1  # Next rank after the last one
#             # Create a DataFrame for the missing POCs with the next rank
#             missing_df = pd.DataFrame({
#                 'run_on': missing_pocs,
#                 'dqr_name': dqr_name,
#                 'rank': next_priority_rank
#             })
#             df = pd.concat([df, missing_df], ignore_index=True)
#             return df


def is_failed(username, task_id, priority,requestConfig,config):
    timeIST = datetime.now() + timedelta(minutes=30, hours=5)

    query = f"""
    SELECT *
    FROM launchpad_runs_dev
    WHERE running_on IN ('{username}')
    AND status IN ('Failed')
    AND task_id = {task_id};
    """

    df = runBQuery(query)

    if df.empty:
        threshold_time = (priority-1) * timedelta(minutes=config['reRunFailTaskInMinuts'])
        failed = (timeIST >= (pd.to_datetime(requestConfig['created_at']) + threshold_time))
        
        return failed

    else:
        max_created_row = df.loc[df['created_at'] == df['created_at'].max()]
        threshold_time = (priority-1) * timedelta(minutes=config['reRunFailTaskInMinuts'])
        failed = (timeIST >= (pd.to_datetime(max_created_row['updated_at'].iloc[0]) + threshold_time))
        return failed


def canPickUpTask(config):
    timeIST = datetime.now()+timedelta(minutes=30, hours=5)
    
    try:
        print("try")
        status, requestConfig, task_id =  getDqrRequest(config)
        print(status, requestConfig, task_id)
        mapping = {
        'mi_gss': 'MI-GSS',
        'gi_gss': 'GI-GSS'
        }
        # print(status,requestConfig,task_id)
        dash = requestConfig['dashboard']
        
        if dash in ["gi_gss","mi_gss"]:
            dashboard = mapping.get(dash, None)
        else:
            dashboard=dash
        config = ymlParser(file='config.yaml')
        pocs = config['dashboard'][dashboard]
        # print(pocs)
        if dashboard in ('GI-GSS'):
            pocs.append('kshiva19')
        print(pocs)
        print(task_id)
        print(requestConfig['dqr_name'])
        priority_list = calculate_priority(task_id,requestConfig['dqr_name'],pocs)
        print(priority_list)
        my_priority = priority_list.loc[priority_list['run_on'] == username, 'rank'].iloc[0]
        
        print(my_priority)
        
        if requestConfig['run_on']== username:
            return True, requestConfig, task_id
        
        else:
            failed = is_failed(username,task_id,my_priority,requestConfig,config)
            return failed, requestConfig, task_id
                  
    except Exception as e:
#     Fail_mail('Getting the Request for the DQR sheet : '+str(e))
        print(e)
        status, requestConfig, task_id = False, '',0
    return status, requestConfig, task_id
    


