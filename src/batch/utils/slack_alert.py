import os
import requests
from airflow.models import Variable
from dotenv import load_dotenv

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK")
load_dotenv()

def task_fail_slack_alert(context):
    dag_id = context.get('task_instance').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    exception = context.get('exception')
    log_url = context.get('task_instance').log_url
    
    slack_msg = {
        "text": "🚨 *ETL Job Failed!*",
        "attachments": [
            {
                "color": "#FF0000",
                "fields": [
                    {"title": "DAG", "value": dag_id, "short": True},
                    {"title": "Task", "value": task_id, "short": True},
                    {"title": "Time", "value": str(execution_date), "short": False},
                    {"title": "Error", "value": str(exception), "short": False},
                    {"title": "log URL", "value": log_url, "short": False}
                ]  
            }
        ]
    }
    
    try:
        requests.post(SLACK_WEBHOOK, json=slack_msg)
        print("Đã gửi cảnh báo trong Slack.")
    except Exception as e:
        print(f"Không gửi được Slack: {e}")