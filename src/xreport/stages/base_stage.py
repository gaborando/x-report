class BaseStage:
    def __init__(self, stage_id, name, description):
        """
        :param stage_id: An identifier for the stage.
        :param name: The name of the stage.
        :param description: A textual description of the stage.
        """
        self.stage_id = stage_id
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
