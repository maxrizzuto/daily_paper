import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from pendulum import datetime

API = "https://www.boredapi.com/api/activity"

# create dag and define parameters
@dag(
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    tags=["activity"],
    catchup=False
)

# define function to run 
def find_activity():

    # @task allows us to execute python functions. name of this task is one task/node on our dag
    @task
    def get_activity():
        r = requests.get(API, timeout=10)
        return r.json()

    @task
    def write_activity_to_file(response):
        """
        takes activity from previous task and writes it to file
        """

        # in airflow we can create variables to pass between dags or tasks. 
        # don't put data that should be secret in variables, or at least title the key secret...
        # create variables in interface by going to admin, variables
        filepath = Variable.get("activity_file")
        with open(filepath, 'a') as f:
            f.write(f'Today you will: {response["activity"]}\n')

        # return filepath to share with last task, read activity from file
        return filepath
    
    @task
    def read_activity_from_file(filepath):
        with open(filepath, 'a') as f:
            print(f.read())


    # create dependencies between tasks
    # get_activity() >> write_activity_to_file() >> read_activity_from_file()

    # alternatively, we could manually pass responses:
    response = get_activity()
    filepath = write_activity_to_file(response)
    read_activity_from_file(filepath)


# call function at end of file
find_activity()