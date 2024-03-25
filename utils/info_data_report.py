#This script is used to generate a report of the data.
#Showing the percentage of fraudulent transactions for each type of transaction

import pandas as pd


def data_report(df):
    tipoTransaccion = ["Pago", "Transferencia", "Retiro", "Debito","Deposito"]

    for tipo in tipoTransaccion:
        df_aux = df[df["tipoTransaccion"]==tipo]
        total_transacciones = len(df_aux)
        total_fraudes = len(df_aux[df_aux["marca_fraude"]==1])
        print(" {}:\n  Fraudulentas: {} -- Total: {} -- Porcentaje: {}%".format(tipo, total_fraudes, total_transacciones, total_fraudes/total_transacciones*100)) 
        print("\n")
        
    
if __name__ == '__main__':
    # Load the data
    DATABASE_PATH = 'data/model_fraud.csv'
    df = pd.read_csv(DATABASE_PATH)
    data_report(df)