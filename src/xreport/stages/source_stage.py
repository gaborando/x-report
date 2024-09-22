import pandas as pd

from xreport.stages.base_stage import BaseStage


class SourceStage(BaseStage):
    """
        SourceStage class inherits from BaseStage and represents a stage in a data processing pipeline where the input DataFrame is passed unchanged as the output. It also initializes an empty DataFrame for computations.

        :param name: Name of the stage.
        :type name: str
        :param description: Description of the stage.
        :type description: str
        :param input_df: Input DataFrame to be processed.
        :type input_df: pandas.DataFrame
    """
    def __init__(self, name, description, input_df):
        """
        :param name: Name of the object.
        :param description: Description of the object.
        :param input_df: Input DataFrame. This will also serve as the initial output DataFrame.
        """
        super().__init__(name, description)
        self.input_df = input_df  # Set the input DataFrame
        self.output_df = input_df  # The output is the same as the input
        self.computation_df = pd.DataFrame()  # Empty computation DataFrame

    def _process_stage(self, df):
        """
        :param df: The input DataFrame that needs to be processed.
        :return: The processed output DataFrame.
        """
        return self.output_df  # Return the output DataFrame