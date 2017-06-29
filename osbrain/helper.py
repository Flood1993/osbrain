"""
Helper functions for osbrain (usually for testing purposes).
"""
import re
import time
from uuid import uuid4


def regex_count_in_list(regex, strings):
    """
    Returns
    -------
    int
        The number of times a regular expression was found in a list of
        strings.
    """
    matches = 0
    for candidate in strings:
        if re.findall(regex, candidate, re.DOTALL):
            matches += 1
    return matches


def logger_received(logger, message, log_name='log_history_info',
                    position=None, timeout=1.):
    """
    Check if a logger receives a message.

    Parameters
    ----------
    logger : Proxy
        Proxy to the logger.
    log_name : str, default is `'log_history_info'`
        The name of the attribue to look for in the logger.
    message : anything
        Message to look for in the log. Can be a partial match. Regular
        expressions are allowed.
    position : int, default is None
        Where to look for the message in the log. If not set, the message
        will be searched for through all the log.
    timeout : float
        After this number of seconds the function will return `False`.

    Returns
    -------
    int
        The number of times the logger received a message that matched the
        expression. Can be higher than 1 if `position` is not set.
    """
    t0 = time.time()
    while True:
        time.sleep(0.01)
        log_history = logger.get_attr(log_name)
        if position is not None:
            log_history = [log_history[position]]
        matches = regex_count_in_list(message, log_history)
        if matches:
            return matches
        if timeout and time.time() - t0 > timeout:
            return 0


def sync_agent_logger(agent, logger):
    """
    Make sure and agent and a logger are synchronized.

    An agent is synchronized with its logger when we make sure the logger has
    started receiving messages from the agent.

    Parameters
    ----------
    agent : Proxy
        Proxy to the agent.
    logger : Proxy
        Proxy to the logger.
    """
    message = str(uuid4())
    delay = 0.01
    while not len(logger.get_attr('log_history_info')) or \
            message not in logger.get_attr('log_history_info')[-1]:
        message = str(uuid4())
        agent.log_info(message)
        time.sleep(delay)
        delay *= 2


def agent_dies(agent, nsproxy, timeout=1.):
    """
    Check if an agent dies within a given period.

    Parameters
    ----------
    agent : str
        Name of the agent, as registered in the name server.
    nsproxy : NSProxy
        Proxy to the name server.
    timeout : float
        After this number of seconds the function will return `False`.

    Returns
    -------
    bool
        Whether the agent died (was unregistered from the name server) within
        the given period.
    """
    t0 = time.time()
    while True:
        time.sleep(0.01)
        if agent not in nsproxy.agents():
            break
        if timeout and time.time() - t0 > timeout:
            return False
    return True


def attribute_match_all(attribute, length=None, data=None, value=None):
    """
    Check if an attribute matches all of the following specified conditions:

    - Minimum length.
    - Contains an item.
    - Is exactly a value.

    Note that only those conditions explicitly passed will be checked.

    Parameters
    ----------
    attribute : anything
        The attribute to match against.
    length : int, default is None
        If specified, the attribute must reach this length (or higher).
    data : anything, default is None
        If specified, the attribute must contain this value.
    value : anything, default is None
        If specified, the attribute must be this value.

    Returns
    -------
    bool
        Whether the attribute matches any condition.
    """
    assert length is not None or data is not None or value is not None, \
        'No condition passed! Will return True always...'
    if length is not None and len(attribute) < length:
        return False
    if data is not None and data not in attribute:
        return False
    if value is not None and attribute != value:
        return False
    return True


def wait_agent_attr(agent, name='received', length=None, data=None, value=None,
                    timeout=3):
    """
    Wait for an agent's attribute to match all of the following specified
    conditions:

    - Reach a minimum length.
    - Contain a particular item.
    - Become a given value.

    Note that only those conditions explicitly passed will be checked.

    Parameters
    ----------
    agent : Proxy
        A proxy to the agent.
    name : str, default is `'received'`
        Name of the agent's attribute to look for (should be a list).
    length : int, default is None
        If specified, wait until the attribute reaches this length or higher.
    data : anything, default is None
        If specified, wait until the attribute contains this element.
    value : anything, default is None
        If specified, wait until the attribute becomes this value.
    timeout : float, default is 3
        After this number of seconds the function will return `False`.

    Returns
    -------
    bool
        Whether the specified conditions where met within the given time.
    """
    t0 = time.time()
    while True:
        attribute = agent.get_attr(name)
        if attribute_match_all(attribute, length=length, data=data,
                               value=value):
            return True
        if time.time() - t0 > timeout:
            break
        time.sleep(0.01)
    return False


def synchronize_sync_pub(server, server_alias, client, client_alias):
    '''
    Create a SYNC_PUB/SYNC_SUB channel and connect both agents.

    Make sure they have stablished the PUB/SUB communication within the
    SYNC_PUB/SYNC_SUB channel before returning. This will guarantee that
    no PUB messages are lost.
    '''
    def assert_receive(agent, message, topic=None):
        try:
            agent.get_attr('_tmp_attr')
            agent.set_attr(_tmp_attr=True)
        except AttributeError:  # Attribute already deleted
            pass

    uuid = client.get_uuid_used_as_alias_for_sub_in_sync_pub(client_alias)

    # Set a temporary custom handler
    client.set_attr(_tmp_attr=False)
    original_handler = client.get_handler(uuid)
    client.subscribe(uuid, assert_receive)

    # Send messages through the PUB socket until the client receives them
    server.each(0.1, 'send', server_alias, 'Synchronize', alias='_tmp_timer')
    assert wait_agent_attr(client, name='_tmp_attr', value=True, timeout=5)
    server.stop_timer('_tmp_timer')

    # Restore the original handler, now that the connection is guaranteed
    client.subscribe(uuid, original_handler)

    client.del_attr('_tmp_attr')
