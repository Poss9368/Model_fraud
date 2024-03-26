# Description: Load and save data functions


from colorama import Fore
import pandas as pd

def load_data(DATABASE_PATH,n_rows:int):
    # Load the data
    df = pd.read_csv(DATABASE_PATH)
    if n_rows > 0 and n_rows < len(df):
        df = df[0:n_rows]
    elif n_rows > len(df):
        print(f"  {Fore.YELLOW}Warning: The number of rows to load is greater than the number of rows in the dataset. The dataset has {len(df)} rows.{Fore.RESET}")
        print(f"  {Fore.YELLOW}The entire dataset will be loaded.{Fore.RESET}")
    else:
        print(f"  {Fore.YELLOW}Warning: The number of rows to load is not valid. The dataset has {len(df)} rows.{Fore.RESET}")
        print(f"  {Fore.YELLOW}The entire dataset will be loaded.{Fore.RESET}")

    #ENRICHMENT
    # Create a new column with the difference between the initial balance and the amount of the transaction
    df['deltaOrigen'] = (df['saldoInicialOrigen'] -df['monto']).abs()
    
    # new column to store the result of the algorithm
    df['marca_fraude_proyectada'] = 0
    
    return df

def save_data(df, SAVE_PATH):
    # Save the data
    df.to_csv(SAVE_PATH, index=False)
    return df