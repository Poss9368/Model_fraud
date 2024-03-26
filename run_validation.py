# Description: This script runs the validation of the model. 
# It compares the real frauds with the proyected frauds.
# The results are saved in a .csv file.

import pandas as pd
from colorama import Fore, Style

# Run the validation
def run_validation(df: pd.DataFrame):
    print('Running the validation...')
    
    #total trasactions in the dataset
    total_transactions = len(df)
    
    # total frauds in transferencias proyected
    total_frauds_transferencias_proyected = len(df[(df['tipoTransaccion'] == 'Transferencia') &  
                                                (df['marca_fraude_proyectada'] == 1)])
    # total frauds in transferencias real
    total_frauds_transferencias = len(df[(df['tipoTransaccion'] == 'Transferencia') &
                                         (df['marca_fraude'] == 1)])
    
    # total frauds in retiros proyected
    total_frauds_retiros_proyected = len(df[(df['tipoTransaccion'] == 'Retiro') & 
                                        (df['marca_fraude_proyectada'] == 1)]) 
    
    # total frauds in retiros real
    total_frauds_retiros = len(df[(df['tipoTransaccion'] == 'Retiro') &
                                  (df['marca_fraude'] == 1)])
    
    # total frauds in retiros correct
    total_frauds_retiros_correct = len(df[(df['tipoTransaccion'] == 'Retiro') &
                                          (df['marca_fraude_proyectada'] == 1) &
                                          (df['marca_fraude'] == 1)])
    
    # total frauds in transferencias correct
    total_frauds_transferencias_correct = len(df[(df['tipoTransaccion'] == 'Transferencia') &
                                                 (df['marca_fraude_proyectada'] == 1) &
                                                 (df['marca_fraude'] == 1)])
    
    # false positives in retiros
    false_positives_retiros = len(df[(df['tipoTransaccion'] == 'Retiro') & 
                                     (df['marca_fraude_proyectada'] == 1) & 
                                     (df['marca_fraude'] == 0)])
    
    # false positives in transferencias
    false_positives_transferencias = len(df[(df['tipoTransaccion'] == 'Transferencia') & 
                                            (df['marca_fraude_proyectada'] == 1) & 
                                            (df['marca_fraude'] == 0)])
    
    # Print the results
    print(f"  {Fore.BLUE}Total transactions:{Style.RESET_ALL} {total_transactions}\n")
    print(f"  {Fore.BLUE}Total frauds in 'Transferencias' - Proyected:{Style.RESET_ALL} {total_frauds_transferencias_proyected}, Real: {total_frauds_transferencias}")
    print(f"  {Fore.BLUE}Total frauds in 'Retiros' - Proyected:{Style.RESET_ALL} {total_frauds_retiros_proyected}, Real: {total_frauds_retiros}\n")

    print(f"  {Fore.BLUE}Total frauds in 'Transferencias' - Correct:{Style.RESET_ALL} {total_frauds_transferencias_correct} / {total_frauds_transferencias}  => {total_frauds_transferencias_correct/total_frauds_transferencias*100:.2f}%")
    print(f"  {Fore.BLUE}Total frauds in 'Retiros' - Correct:{Style.RESET_ALL} {total_frauds_retiros_correct} / {total_frauds_retiros}  => {total_frauds_retiros_correct/total_frauds_retiros*100:.2f}%\n")
    
    print(f"  {Fore.BLUE}False Positives in 'Transferencias':{Style.RESET_ALL} {false_positives_transferencias} / {total_frauds_transferencias_proyected}  => {false_positives_transferencias/total_frauds_transferencias_proyected*100:.2f}%")
    print(f"  {Fore.BLUE}False Positives in 'Retiros':{Style.RESET_ALL} {false_positives_retiros} / {total_frauds_retiros_proyected}  => {false_positives_retiros/total_frauds_retiros_proyected*100:.2f}%")
    
    ## save the results in .csv
    df_results = pd.DataFrame({'total_transactions': [total_transactions],
                               'total_frauds_transferencias_proyected': [total_frauds_transferencias_proyected],
                               'total_frauds_transferencias': [total_frauds_transferencias],
                               'total_frauds_retiros_proyected': [total_frauds_retiros_proyected],
                               'total_frauds_retiros': [total_frauds_retiros],
                               'total_frauds_retiros_correct': [total_frauds_retiros_correct],
                               'total_frauds_transferencias_correct': [total_frauds_transferencias_correct],
                               'false_positives_retiros': [false_positives_retiros],
                               'false_positives_transferencias': [false_positives_transferencias]})
    
    df_results.to_csv('data/result/results_logs.csv', index=False)
    
    return None
