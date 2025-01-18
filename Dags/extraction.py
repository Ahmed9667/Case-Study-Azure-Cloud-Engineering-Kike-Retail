# Extract data
import pandas as pd
def run_extraction():
    try:
        df= pd.read_csv(r'https://github.com/Ahmed9667/Zipco_Food_Case_Study/blob/main/dags/extraction.py')

    except Exception as e :
        print(f'an error occured {e}')    