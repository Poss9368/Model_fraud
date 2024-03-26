# Description: This file contains the algorithm to detect fraud in the transactions of type 'Retiro'.

import pandas as pd
import multiprocessing
from multiprocessing import Pool

# ALGORITHM FOR RETIROS
# first rule: if the transaction is a retiro, check if there is a transfer with the same amount in the last 3 time units
def check_for_retiro(row, df_tra: pd.DataFrame):
    tiempo = row['unidadTiempo'] 
    tipo_transaccion=row['tipoTransaccion']
    monto_transaccion = row['monto']
    saldo_inicial_origen = row['saldoInicialOrigen']
    
    # if return 1, the transaction is a fraud
    if tipo_transaccion == 'Retiro':    
        df_tra = df_tra[(df_tra['unidadTiempo'] == tiempo) | (df_tra['unidadTiempo'] == tiempo-1) | (df_tra['unidadTiempo'] == tiempo-2)]
        df_tra = df_tra[df_tra['monto'] == monto_transaccion]
        
        if len(df_tra) > 0:
            return 1
        
    return 0

# Function to apply the check_for_transferencia function to a dataframe in parallel
def apply_check_for_retiro(dfs):
    df_retiro = dfs[0]
    df_transferencia = dfs[1]
    
    result = []
    for index, row in df_retiro.iterrows():
        marca = check_for_retiro(row, df_transferencia)
        if marca == 1:
            result.append(index)
    return result

def run_parallel_retiro(df_retiro: pd.DataFrame, df_transferencia: pd.DataFrame, n_cores: int, n_buckets: int):
    # Split the dataframe in buckets with n_buckets rows
    df_retiro_split = [df_retiro.iloc[i:i+n_buckets] for i in range(0, len(df_retiro), n_buckets)]
    df_transferencia_split = []
    
    for bucket in df_retiro_split:
        tmin = bucket['unidadTiempo'].min()-1
        tmax = bucket['unidadTiempo'].max()
        df_transferencia_aux = df_transferencia[(df_transferencia['unidadTiempo'] >= tmin) & (df_transferencia['unidadTiempo'] <= tmax)]
        df_transferencia_split.append(df_transferencia_aux)
        
    # Create a list of tuples with the dataframes to be processed
    dfs = [(df_retiro_split[i], df_transferencia_split[i]) for i in range(len(df_retiro_split))]
    
    with Pool(n_cores) as p:
        result = p.map(apply_check_for_retiro, dfs)
    
    result = [item for sublist in result for item in sublist]
    
    return result