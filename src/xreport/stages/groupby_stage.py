import pandas as pd
from .base_stage import BaseStage

class GroupByStage(BaseStage):
    """
    class GroupByStage(BaseStage):

    :param name: Stage name
    :param description: Stage description
    :param group_by_columns: List of columns to group by
    :param agg_funcs: Dictionary where the key is the column name and the value is the aggregation function

    def execute(self, input_df):

    :param input_df: DataFrame to be processed

    :returns: Aggregated DataFrame after performing group by and aggregation operations

    This method performs the following steps:
    1. Sets the input DataFrame.
    2. Groups the input DataFrame by the specified columns.
    3. Aggregates the grouped data using the specified aggregation functions.
    4. Sorts the input DataFrame by the group-by columns.
    5. Constructs a computation DataFrame by appending aggregation rows to each group's data.
    6. Sets the computation and output DataFrames.
    """
    def __init__(self, name, description, group_by_columns, agg_funcs):
        """
        :param name: Stage name
        :param description: Stage description
        :param group_by_columns: List of columns to group by
        :param agg_funcs: Dictionary where the key is the column name and the value is the aggregation function
        """
        super().__init__(name, description)
        self.group_by_columns = group_by_columns
        self.agg_funcs = agg_funcs

    def execute(self, input_df):
        """
        :param input_df: A pandas DataFrame that needs to be processed.
        :return: A new DataFrame containing the aggregated results.
        """
        # Set input DataFrame
        self.input_df = input_df.copy()

        # Perform the group by operation
        grouped = self.input_df.groupby(self.group_by_columns)

        # Store aggregation results
        aggregation_result = grouped.agg(self.agg_funcs).reset_index()

        # Sorting by the group-by columns
        sorted_df = self.input_df.sort_values(by=self.group_by_columns)

        # Build the computation DataFrame
        computation_list = []
        for group_values, group_data in grouped:
            computation_list.append(group_data)

            # Build a boolean mask for each grouping key
            mask = pd.Series(True, index=aggregation_result.index)
            for col, val in zip(self.group_by_columns, group_values if isinstance(group_values, tuple) else [group_values]):
                mask &= aggregation_result[col] == val

            # Get the corresponding aggregation row for the group
            agg_row = aggregation_result[mask].copy()

            # Create a label for grouping in the first column (e.g., 'Agg for group (A)')
            agg_row.loc[:, self.group_by_columns] = f'Aggregation for group ({", ".join(map(str, group_values))})'

            # Append the aggregated row after each group's data
            computation_list.append(agg_row)

        # Concatenate the sorted data with the aggregation summary at the end of each group
        computation_df = pd.concat(computation_list)

        # Set the computation DataFrame
        self.computation_df = computation_df

        # Set the output DataFrame to the aggregated result
        self.output_df = aggregation_result

        return self.output_df
