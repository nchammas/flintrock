class UsageError(Exception):
    pass


class UnsupportedProviderError(UsageError):
    def __init__(self, provider):
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
    def __init__(self, *, attempted_command, state):
        super().__init__(
            "Cluster is in state '{s}'. Cannot call {c}.".format(
                c=attempted_command,
                s=state))
        self.attempted_command = attempted_command
        self.state = state
