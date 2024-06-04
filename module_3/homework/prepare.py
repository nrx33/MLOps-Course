import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@transformer
def transform_initial(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Args:
        df: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        DataFrame: Transformed DataFrame
    """
    # Transformation logic
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)

    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df.duration = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)]
    
    return df

@test
def test_output_initial(output, *args) -> None:
    """
    Template code for testing the output of the initial block.
    """
    assert output is not None, 'The output is undefined'
    assert isinstance(output, pd.DataFrame), 'The output is not a DataFrame'
    assert 'duration' in output.columns, 'The output DataFrame does not contain the "duration" column'
