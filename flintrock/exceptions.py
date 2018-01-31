class NothingToDo(Exception):
    pass


class UsageError(Exception):
    pass


class UnsupportedProviderError(UsageError):
    def __init__(self, provider: str):
        super().__init__(
            "This provider is not supported: {p}".format(p=provider))
        self.provider = provider


class Error(Exception):
    pass


class ClusterNotFound(Error):
    pass


class ClusterAlreadyExists(Error):
    pass


class ClusterInvalidState(Error):
    def __init__(self, *, attempted_command: str, state: str):
        super().__init__(
            "Cluster is in state '{s}'. Cannot execute {c}.".format(
                c=attempted_command,
                s=state))
        self.attempted_command = attempted_command
        self.state = state


class SSHError(Error):
    def __init__(self, *, host: str, message: str):
        super().__init__(
            "[{h}] {m}".format(h=host, m=message))
        self.host = host
        self.message = message


class InterruptedEC2Operation(Error):
    def __init__(self, *, instances: list):
        super().__init__(
            "Operation aborted."
        )
        self.instances = instances
