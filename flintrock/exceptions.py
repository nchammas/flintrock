class UsageError(Exception):
    pass


class UnsupportedProviderError(UsageError):
    def __init__(self, provider: str):
        super().__init__(
            "This provider is not supported: {p}".format(p=provider))
        self.provider = provider


class NothingToDo(Exception):
    pass


class ClusterNotFound(Exception):
    pass


class ClusterAlreadyExists(Exception):
    pass


class ClusterInvalidState(Exception):
    def __init__(self, *, attempted_command: str, state: str):
        super().__init__(
            "Cluster is in state '{s}'. Cannot execute {c}.".format(
                c=attempted_command,
                s=state))
        self.attempted_command = attempted_command
        self.state = state


class NodeError(Exception):
    def __init__(self, error: str):
        super().__init__(
            "At least one node raised an error: " + error)
        self.error = error
