from xreport.stages.base_stage import BaseStage


class ExpandStage(BaseStage):
    """
    ExpandStage is a processing stage in a data pipeline, responsible for applying a lambda function on a DataFrame column and then performing a join operation.

    :param name: The name of the stage.
    :type name: str
    :param description: Description of the stage.
    :type description: str
    :param join_columns: List of columns to join on.
    :type join_columns: list[str]
    :param lambda_func: A lambda function to be applied on the DataFrame.
    :type lambda_func: function
    :param join_mode: Mode of the join operation (default is 'left').
    :type join_mode: str

    _method _process_stage(df)
    :param df: Input DataFrame.
    :type df: pandas.DataFrame
    :return: Output DataFrame after applying lambda function and performing the join.
    :rtype: pandas.DataFrame
    """
    def __init__(self, name, description, join_columns, lambda_func, join_mode='left'):
        super().__init__(name, description)
        self.join_columns = join_columns
        self.lambda_func = lambda_func
        self.join_mode = join_mode

    def _process_stage(self, df):
        """
        :param df: pandas DataFrame to be processed, which includes the columns specified in `self.join_columns`.
        :return: DataFrame resulting from a left join between the original `df` and the DataFrame produced by applying `self.lambda_func` to the unique values in the join columns.
        """
        # Lambda function result
        lambda_result = self.lambda_func(df[self.join_columns].drop_duplicates())
        self.computation_df = lambda_result  # Only the result of the lambda

        # Perform left join
        self.output_df = df.merge(lambda_result, on=self.join_columns, how=self.join_mode)
        return self.output_df
