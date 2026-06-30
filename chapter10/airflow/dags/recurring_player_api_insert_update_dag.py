import datetime
import logging

# @dag = decorator that turns a function into an Airflow DAG (the pipeline)
from airflow.decorators import dag

# HttpOperator = task template for calling an API over HTTP
from airflow.providers.http.operators.http import HttpOperator

# PythonOperator = task template for running a Python function
from airflow.operators.python import PythonOperator

# upsert_player_data lives in shared_functions.py (same dags/ folder);
# upsert = UPDATE the row if it exists, else INSERT it
from shared_functions import upsert_player_data


# ---- Helper 1: verifies the API is alive (used by the health-check task) ----
def health_check_response(response):
    # Write the API's status code + body into Airflow's logs so you can inspect them
    logging.info(f"Response status code: {response.status_code}")
    logging.info(f"Response body: {response.text}")
    # Return True ONLY if status is 200 (OK) AND the body is the expected health message.
    # If this returns False, the task fails and the pipeline stops here.
    return response.status_code == 200 and response.json() == {
        "message": "API health check successful"
    }


# ---- Helper 2: the function the 3rd task runs (writes players to the DB) ----
def insert_update_player_data(**context):
    # XCom PULL: read the player data that the "api_player_query" task left behind.
    # ti = "task instance" (the running task); xcom_pull reads from the shared mailbox.
    player_json = context["ti"].xcom_pull(task_ids="api_player_query")
    if player_json:
        # Data came back -> write/update it in the SQLite database
        upsert_player_data(player_json)
    else:
        # No data -> log a warning instead of crashing
        logging.warning("No player data found.")


# schedule_interval=None -> no automatic schedule; you trigger this DAG manually
@dag(schedule_interval=None)
def recurring_player_api_insert_update_dag():

    # ---- Task 1: confirm the API is up before doing real work ----
    api_health_check_task = HttpOperator(
        task_id="check_api_health_check_endpoint",
        http_conn_id="sportsworldcentral_url",  # uses the saved Airflow connection (no hardcoded URL)
        endpoint="/",                            # hit the API root
        method="GET",
        headers={"Content-Type": "application/json"},
        response_check=health_check_response,    # pass/fail decided by Helper 1 above
    )

    # Hardcoded "only get changes on/after this date" (production would compute this dynamically)
    temp_min_last_change_date = "2024-04-01"

    # ---- Task 2: pull only CHANGED players (the delta) from the API ----
    api_player_query_task = HttpOperator(
        task_id="api_player_query",
        http_conn_id="sportsworldcentral_url",
        endpoint=(
            # query params: skip/limit for paging, minimum_last_changed_date = the delta filter
            f"/v0/players/?skip=0&limit=100000&minimum_last_changed_date="
            f"{temp_min_last_change_date}"
        ),
        method="GET",
        headers={"Content-Type": "application/json"},
    )
    # Note: Airflow auto-PUSHES this task's response into XCom, so Task 3 can pull it.

    # ---- Task 3: take the pulled data and upsert it into SQLite ----
    player_sqlite_upsert_task = PythonOperator(
        task_id="player_sqlite_upsert",
        python_callable=insert_update_player_data,  # the function defined above
        provide_context=True,                       # gives the function the **context bundle (needed for xcom_pull)
    )

    # Run order: check health -> THEN query players -> THEN upsert.
    # >> is Airflow's "then" operator (sets task dependencies).
    api_health_check_task >> api_player_query_task >> player_sqlite_upsert_task


# Required: calling the decorated function creates the actual DAG object Airflow registers.
# Without this line, Airflow won't see the DAG.
dag_instance = recurring_player_api_insert_update_dag()