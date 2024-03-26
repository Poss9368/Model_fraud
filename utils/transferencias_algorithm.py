# Description: This file contains the algorithm to detect fraud in the transactions of type 'Transferencia'.

import pandas as pd
import multiprocessing
from multiprocessing import Pool

# ALGORITHM FOR TRANSFERENCIAS
# first rule: if the transaction is a transferencia, check if the deltaOrigen is 0 
def check_for_transferencia(row):
    tipo_transaccion=row['tipoTransaccion']
    delta_origen = row['deltaOrigen']
    saldo_inicial_destinatario = row['saldoInicialDestinatario']
    saldo_final_destinatario = row['saldoFinalDestinatario']
    saldo_final_origen = row['saldoFinalOrigen']

    # if return 1, the transaction is a fraud 
    if tipo_transaccion == 'Transferencia':
        if delta_origen == 0:
            #if saldo_inicial_destinatario == 0 and saldo_final_destinatario == 0 and saldo_final_origen == 0:
                return 1
    
    return 0


# Function to apply the check_for_transferencia function to a dataframe in parallel
def apply_check_for_transferencia(df_transferencia: pd.DataFrame):
    result = []
    for index, row in df_transferencia.iterrows():
        marca = check_for_transferencia(row)
        if marca == 1:
            result.append(index)
    return result 


# Function to run the parallel process for the transferencias algorithm
def run_parallel_transferencias(df_transferencia: pd.DataFrame, n_cores: int, n_buckets: int):
    # Split the dataframe in buckets with n_buckets rows
    df_transferencia_split = [df_transferencia.iloc[i:i+n_buckets] for i in range(0, len(df_transferencia), n_buckets)]
    with Pool(n_cores) as p:
        result = p.map(apply_check_for_transferencia, df_transferencia_split)
    result = [item for sublist in result for item in sublist]
    
    return result
