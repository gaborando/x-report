class BaseStage:
    def __init__(self, name, description):
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
