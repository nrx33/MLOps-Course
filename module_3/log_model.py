if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
import mlflow
import pickle
import os

@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
 
    model = data['model']  # linear regression model
    vectorizer = data['vectorizer']  # dict vectorizer

    # Set MLflow tracking URI
    mlflow.set_tracking_uri("http://mlflow:5000")

    # Set the experiment name
    experiment_name = "homework_03"
    mlflow.set_experiment(experiment_name)

    # Start an MLflow run
    with mlflow.start_run() as run:
        # Log the model
        mlflow.sklearn.log_model(model, "linear_regression_model")

        # Save and log the dict vectorizer as an artifact
        vectorizer_path = "output/dict_vectorizer.pkl"
        with open(vectorizer_path, "wb") as f:
            pickle.dump(vectorizer, f)
        mlflow.log_artifact(vectorizer_path)

    # Optionally return any object
    return {"run_id": run.info.run_id, "model": model, "vectorizer": vectorizer}
