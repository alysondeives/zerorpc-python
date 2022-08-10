def wait_and_ignore(thread):
    """Wait for a green thread to finish execute and ignore the return value and any 
    raised exception.
    """
    try:
        thread.wait()
    except:
        pass # ignore any exception raised in the thread