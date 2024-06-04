if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd
from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import LinearRegression

@transformer
def transform(data, *args, **kwargs):
    """
    Train a linear regression model using DictVectorizer and return the vectorizer and model.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        dict: A dictionary containing the DictVectorizer and the trained model
    """
    # feature selection
    categorical_features = ['PULocationID', 'DOLocationID']

    # Convert the categorical features to string
    data[categorical_features] = data[categorical_features].astype(str)

    # Initialize the DictVectorizer
    dv = DictVectorizer()

    # Prepare the training data
    train_dict = data[categorical_features].to_dict(orient='records')
    X_train = dv.fit_transform(train_dict)

    # Target variable
    y_train = data['duration'].values

    # Initialize and train the linear regression model
    model = LinearRegression()
    model.fit(X_train, y_train)

    # Print the intercept of the model
    print(f"The intercept of the model is: {model.intercept_:.2f}")

    return {'vectorizer': dv, 'model': model}

@test
def test_output(output, *args) -> None:
    """
    Test the output of the transformation block.
    """
    assert output is not None, 'The output is undefined'
    assert isinstance(output, dict), 'The output is not a dictionary'
    assert 'vectorizer' in output, 'The output dictionary does not contain the "vectorizer" key'
    assert 'model' in output, 'The output dictionary does not contain the "model" key'
    assert isinstance(output['vectorizer'], DictVectorizer), 'The vectorizer is not a DictVectorizer instance'
    assert isinstance(output['model'], LinearRegression), 'The model is not a LinearRegression instance'
