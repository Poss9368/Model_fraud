import time
import pandas as pd
import multiprocessing
from multiprocessing import Pool

from run_algorithm import run_algorithm
from run_validation import run_validation
from utils.load_and_save_data import load_data, save_data

if __name__ == '__main__':
    tic = time.time()
    
    ############################################################
    # Load and prepare the data
    print('Loading and preparing the data...')
    DATABASE_PATH = 'data/model_fraud.csv'
    df = load_data(DATABASE_PATH, n_rows=100000)
    
    ############################################################
    # Run the algorithm
    n_cores = 12
    df = run_algorithm(df, n_cores)
    
    ############################################################
    # Run Validation
    run_validation(df)
    
    ############################################################
    # Save the data
    print('Saving the data...')
    SAVE_PATH = 'data/model_fraud_with_proyected_fraud.csv'
    save_data(df, SAVE_PATH)
    
    toc = time.time()
    print(f'Time elapsed: {toc-tic:.2f} seconds')
