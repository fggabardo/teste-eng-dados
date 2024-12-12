
### Etapa 1
import pandas as pd

df_1 = pd.read_csv('transactions_file1.csv')
df_2 = pd.read_csv('transactions_file2.csv')

df_up = df_1.merge(df_2)

### Transformando o campo transaction_date para o tipo data
df_up['transaction_date'] = pd.to_datetime(df_up['transaction_date'])

### Etapa 2
import pandas_gbq
from google.oauth2 import service_account
import os
import json
from dotenv import load_dotenv
load_dotenv()

### Puxando credenciais da conta de serviço do .env
SA_PRIVATE_KEY = os.environ.get('SA_PRIVATE_KEY')
credentials_dict = json.loads(SA_PRIVATE_KEY)
credentials = service_account.Credentials.from_service_account_info(credentials_dict)

schema = [{'name' : 'transaction_id', 'type' : 'STRING'},
          {'name' : 'customer_id', 'type' : 'STRING'},
          {'name' : 'transaction_date', 'type' : 'DATE'},
          {'name' : 'transaction_amount', 'type' : 'FLOAT'},
          {'name' : 'transaction_status', 'type' : 'STRING'},
          {'name' : 'transaction_type', 'type' : 'STRING'},
          {'name' : 'qtty', 'type' : 'FLOAT'},
          {'name' : 'price', 'type' : 'FLOAT'}]

table_name = "dataset_teste.transactions"

pandas_gbq.to_gbq(df_up, table_name, 
                  project_id='teste-eng-dados',
                  if_exists='replace',
                  credentials = credentials,
                  table_schema=schema)
