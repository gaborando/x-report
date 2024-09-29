from xreport.stages.base_stage import BaseStage


class ExpandStage(BaseStage):
    def __init__(self, stage_id, name, description, join_columns, lambda_func, join_mode='left'):
        """
        :param stage_id: Unique identifier for the stage.
        :param name: Name of the stage.
        :param description: Description of what the stage does.
        :param join_columns: Columns on which the join operations should be performed.
        :param lambda_func: A lambda function to apply during the join operation.
        :param join_mode: Specifies the type of join operation (default is 'left').
        """
        super().__init__(stage_id, name, description)
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
