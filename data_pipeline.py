
### Etapa 1
import pandas as pd

df_1 = pd.read_csv('transactions_file1.csv')
df_2 = pd.read_csv('transactions_file2.csv')
df_3 = pd.read_csv('customers_file3.csv')

df_transactions_up = df_1.merge(df_2)

### Transformando o campo transaction_date para o tipo data
df_transactions_up['transaction_date'] = pd.to_datetime(df_transactions_up['transaction_date'])


### Tratando os campos da tabela  customers para ter o mesmo padrão das transações
dict = {
    'C01' : 'C1',
    'C02' : 'C2',
    'C03' : 'C3',
    'C04' : 'C4',
    'C05' : 'C5',
    'C06' : 'C6',
    'C07' : 'C7',
    'C08' : 'C8',
    'C09' : 'C9'
}

df_3['customer_id'] = df_3['customer_id'].replace(dict)

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

table_transactions_name = "dataset_teste.transactions"

pandas_gbq.to_gbq(df_transactions_up, table_transactions_name, 
                  project_id='teste-eng-dados',
                  if_exists='replace',
                  credentials = credentials,
                  table_schema=schema)

table_customers_name = "dataset_teste.customers"

pandas_gbq.to_gbq(df_3, table_customers_name, 
                  project_id='teste-eng-dados',
                  if_exists='replace',
                  credentials = credentials)
