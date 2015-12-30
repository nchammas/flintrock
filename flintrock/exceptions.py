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


class UsageError(Exception):
    pass


class NothingToDo(Exception):
    pass
