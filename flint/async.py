def async_execute(async_fns):
    # TODO: Use parameter names for run_in_executor() once Python 3.4.4 is released.
    #       Until then, we leave them out to maintain compatibility across Python 3.4
    #       and 3.5.
    # See: http://stackoverflow.com/q/32873974/

    import asyncio
    loop = asyncio.get_event_loop()
    tasks = []
    for fn in async_fns:
        task = loop.run_in_executor(executor=None, callback=fn)
        tasks.append(task)
    loop.run_until_complete(asyncio.wait(tasks))

