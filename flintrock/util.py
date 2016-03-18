import asyncio
import functools
import os
import sys

# Flintrock modules
from .exceptions import SSHError, NodeError


def flintrock_is_in_development_mode() -> bool:
    """
    Check if Flintrock was installed in development mode.

    Use this function to toggle behavior that only Flintrock developers should
    see.
    """
    # This esoteric technique was pulled from pip.
    # See: https://github.com/pypa/pip/pull/3258/files#diff-ab583908279e865537dec218246edcfcR310
    for path_item in sys.path:
        egg_link = os.path.join(path_item, 'Flintrock.egg-link')
        if os.path.isfile(egg_link):
            return True
    else:
        return False


def run_against_hosts(*, async_partial_func: functools.partial, hosts: list):
    """
    Run an asynchronous function against a group of hosts.

    async_partial_func must accept `host` as a keyword argument.
    """
    loop = asyncio.get_event_loop()

    if flintrock_is_in_development_mode():
        loop.set_debug(True)

    tasks = []
    for host in hosts:
        task = asyncio.ensure_future(async_partial_func(host=host))
        tasks.append(task)

    # TODO: Let KeyboardInterrupt cleanly cancel hung commands.
    try:
        loop.run_until_complete(asyncio.gather(*tasks))
        # done, _ = loop.run_until_complete(asyncio.wait(tasks))
        # # Is this the right way to make sure no coroutine failed?
        # for future in done:
        #     future.result()
    except SSHError as e:
        raise NodeError(str(e))


def sync_run(coro_or_future):
    """
    Run a coroutine or future synchronously and return the result.
    """
    return asyncio.get_event_loop().run_until_complete(coro_or_future)
