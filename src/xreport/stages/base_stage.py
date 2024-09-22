class BaseStage:
    """
    BaseStage represents a base class for data processing stages in a pipeline.

    Methods
    -------
    __init__(self, name, description)
        Initializes the base stage with a name and description.

    execute(self, df)
        Executes the stage by copying the input DataFrame and invoking the stage-specific processing method.

    _process_stage(self, df)
        Placeholder method for stage-specific processing logic to be implemented by subclasses.
    """
    def __init__(self, name, description):
        """
        :param name: The name of the instance.
        :param description: A brief description of the instance.
        """
        self.name = name
        self.description = description
        self.input_df = None
        self.output_df = None
        self.computation_df = None

    def execute(self, df):
        self.input_df = df.copy()
        return self._process_stage(df)

    def _process_stage(self, df):
        raise NotImplementedError("Each stage must implement its own processing logic.")
