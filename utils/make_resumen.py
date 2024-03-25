# This script is used to make a resume of the data. 
# It is useful when the data is too big and we want to make some tests with a smaller dataset.

import pandas as pd


# Function to make a resume of the data
def make_data_resume(df, N, SAVE_PATH):
    df_aux = df[0:N]
    df_aux.to_csv(SAVE_PATH, index=False)
    return df_aux


if __name__ == '__main__':
    # Load the data
    DATABASE_PATH = 'data/model_fraud.csv'
    df = pd.read_csv(DATABASE_PATH)
    N=1000000
    make_data_resume(df, N, SAVE_PATH = 'data/model_fraud_resume.csv')
        
