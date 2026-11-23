class PipelineError(Exception):
    pass


class RetryableError(PipelineError):
    pass


class QuarantineError(PipelineError):
    pass


class FatalError(PipelineError):
    pass


class ConfigError(PipelineError):
    pass
