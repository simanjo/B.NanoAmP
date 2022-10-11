class PipelineStepError(Exception):

    def __init__(self, step, *args: object) -> None:
        super().__init__(*args)
        self.step = step

    def __str__(self) -> str:
        msg = [
            f"Executing the pipeline failed at {self.step}.",
            "For more information consult the logfiles."
        ]
        return "\n".join(msg)