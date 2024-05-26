import os
import pickle
import warnings
import logging

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

import mlflow
import mlflow.sklearn

# ignore warnings
warnings.filterwarnings('ignore')
mlflow.set_tracking_uri("sqlite:///mlflow.db")

# Set logging level to ERROR to suppress warnings
logging.getLogger("mlflow").setLevel(logging.ERROR)

def load_pickle(filename: str):
    with open(filename, "rb") as f_in:
        return pickle.load(f_in)

def run_train(data_path: str = "./output"):
    # start mlflow run
    with mlflow.start_run():
        X_train, y_train = load_pickle(os.path.join(data_path, "train.pkl"))
        X_val, y_val = load_pickle(os.path.join(data_path, "val.pkl"))

        # Set min_samples_split value
        min_samples_split = 2
        
        # Initialize and train RandomForestRegressor with min_samples_split
        rf = RandomForestRegressor(max_depth=10, random_state=0, min_samples_split=min_samples_split)
        rf.fit(X_train, y_train)
        y_pred = rf.predict(X_val)

        # Calculate RMSE
        rmse = mean_squared_error(y_val, y_pred, squared=False)

if __name__ == '__main__':
    # start the autologging
    mlflow.sklearn.autolog()

    # run the training
    run_train()