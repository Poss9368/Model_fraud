import pandas as pd
import multiprocessing
from multiprocessing import Pool

from utils.retiro_algorithm import run_parallel_retiro
from utils.transferencias_algorithm import run_parallel_transferencias

def run_algorithm(df: pd.DataFrame, n_cores: int):
    # Data frame just for retiro and transferencia
    df_retiro = df[df['tipoTransaccion'] == 'Retiro']
    df_transferencia = df[df['tipoTransaccion'] == 'Transferencia']

    ############################################################
    # Run the algorithm for transferencias
    print('Running the algorithm for transferencias...')
    fraud_index_result_transferencias = run_parallel_transferencias(df_transferencia, n_cores, 10000)
    
    # Update the data frame with the result of the algorithm
    df.loc[fraud_index_result_transferencias, 'marca_fraude_proyectada'] = 1
    
    ############################################################
    #Run the algorithm for retiros
    print('Running the algorithm for retiros...')
    fraud_index_result_retiro = run_parallel_retiro(df_retiro, df_transferencia, n_cores, 10000)
    
    # Update the data frame with the result of the algorithm
    df.loc[fraud_index_result_retiro, 'marca_fraude_proyectada'] = 1
    
    return df