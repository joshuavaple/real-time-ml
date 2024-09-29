import hopsworks
from src.config import hopsworks_config as config
from typing import List
import pandas as pd


def push_value_to_feature_group(
        value: List[dict],
        feature_group_name: str,
        feature_group_version: int,
        feature_group_primary_keys: List[str],
        feature_group_event_time: str,
        start_offline_materialization:bool,
):
    """ 
    Pushes a value to a given `feature_group_name` in the feature store.

    Args:
        value (List[dict]): Value to push to the feature store
        feature_group_name (str): Name of the feature group
        feature_group_version (int): Version of the feature group
        feature_group_primary_key (List[str]): List of primary key columns
        feature_group_event_time (str): Event time column
        start_offline_materialization (bool): Whether to store the data in offline storage as well when we save the `value` to the feature group

    Returns:
        None
    """
    
    project = hopsworks.login(
        project = config.hopsworks_project_name,
        api_key_value=config.hopsworks_api_key_value,
    )
    feature_store = project.get_feature_store()
    # Get or create the 'transactions_fraud_batch_fg' feature group
    feature_group = feature_store.get_or_create_feature_group(
        name=feature_group_name,
        version=feature_group_version,
        # description="Transaction data",
        primary_key=feature_group_primary_keys,
        event_time=feature_group_event_time,
        online_enabled=True,
        # TODO: add the test for feature data qualit, as it is important for downstream ML training
        # expectation_suite=expectation_suite_transactions,
    )
    # transform the value to a pandas df:
    value_df = pd.DataFrame(value)

    # breakpoint()
    # feature_group.insert(value_df) # this action adds to both online and offline storage, sot it will be slower
    feature_group.insert(
        value_df, 
        write_options={"start_offline_materialization" : start_offline_materialization}
        ) # this specification writes to online storage only

