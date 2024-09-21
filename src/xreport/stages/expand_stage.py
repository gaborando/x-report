from xreport.stages.base_stage import BaseStage


class ExpandStage(BaseStage):
    def __init__(self, name, description, join_columns, lambda_func):
        super().__init__(name, description)
        self.join_columns = join_columns
        self.lambda_func = lambda_func

    def _process_stage(self, df):
        # Lambda function result
        lambda_result = self.lambda_func(df[self.join_columns].drop_duplicates())
        self.computation_df = lambda_result  # Only the result of the lambda

        # Perform left join
        self.output_df = df.merge(lambda_result, on=self.join_columns, how='left')
        return self.output_df
