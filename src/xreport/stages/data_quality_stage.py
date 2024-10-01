import pandas as pd

from xreport.stages.base_stage import BaseStage


class RowLevelCheck:
    def __init__(self, name, check_func, warning_resolution):
        self.name = name
        self.check_func = check_func
        self.warning_resolution = warning_resolution

class DataFrameLevelCheck:
    def __init__(self, name, check_func, warning_resolution):
        self.name = name
        self.check_func = check_func
        self.warning_resolution = warning_resolution

class DataQualityStage(BaseStage):
    def __init__(self, stage_id, name, description, row_checks, df_checks):
        super().__init__(stage_id, name, description)
        self.row_checks = row_checks
        self.df_checks = df_checks
        self.computation_df = pd.DataFrame(columns=["Name", "Warning Message", "Resolution"])

    def execute(self, input_df):
        self.input_df = input_df.copy()
        computation = []
        # Execute row-level checks
        for check in self.row_checks:
            failed_rows = input_df[~input_df.apply(check.check_func, axis=1)]
            for index, row in failed_rows.iterrows():
                warning_msg, resolution_msg = check.warning_resolution(row)
                computation.append({
                    "Name": check.name,
                    "Warning Message": warning_msg,
                    "Resolution": resolution_msg
                })

        # Execute DataFrame-level checks
        for check in self.df_checks:
            if not check.check_func(input_df):
                warnings_resolutions = check.warning_resolution(input_df)
                for warning, resolution in warnings_resolutions:
                    computation.append({
                        "Name": check.name,
                        "Warning Message": warning,
                        "Resolution": resolution
                    })

        self.computation_df = pd.DataFrame(computation)
        self.output_df = input_df.copy()
        return self.output_df