"""
Helper functions to transform the data from a tidy count of unique tuples
to a user level aggregation of counts of resourceType occurrences both duplicated
de-duplicated
"""




def setColumnValues(keyValuePair):
    """
    A helper function to buildRows, this function appropriately sets the index for
    insertion of counts due to corresponding resourceType
    :param keyValuePair: A tuple of extracted information and the corresponding count of duplication
    :return: A dictionary of index to value
    """


    resourceType = keyValuePair[0][1]
    N = keyValuePair[1]

    # define index values for resourceTypes
    idx = [0, 4]
    v1, v2 = N, 1

    # set the index for each resource type
    if resourceType == 'Condition':
        pass
    elif resourceType == 'Procedure':
        idx = [x + 1 for x in idx]
    elif resourceType == 'Observation':
        idx = [x + 2 for x in idx]
    elif resourceType == 'MedicationOrder' or resourceType == 'MedicationRequest':
        idx = [x + 3 for x in idx]

    return {idx[0]: N, idx[1]: 1}



def csvToTuples(keyValuePair):
    """
    take a string representation of key value pair and count of existance
    and return actual tuple representation
    :param keyValuePair: '('x', 'y','z', 'a'), 1'
    :return: (('x','y','z','z'),1)
    """

    keys = keyValuePair.split(', ')
    n = len(keys)
    value = int(keys.pop(n - 1))

    return (tuple(keys),value)



def buildRows(keyValuePair):
    """
    Taking a key value pair where the key is a tuple of all relevant information and value is the
    count of the count of duplication for the tuple we return a single ordered tuple.
    If our resultant rows are ordered Condition, Procedure, Observation, MedicineOrder and a given tuple
    is 'Procedure', then the resulting row would be as follows
    :param keyValuePair: ((id, 'Procedure', date, system, code), N)
    :return: (id, [0, N, 0, 0, 0, 1, 0, 0])
    """

    # define the rows and get the correct index to insert quanities
    id_ = keyValuePair[0][0]
    idx = setColumnValues(keyValuePair)

    # create and return row
    return (id_, [idx.get(i, 0) for i in range(8)])



def flatten(keyValuePair):
    """
    Silly function to make flat. Sure there is a better way
    :param keyValuePair: (id (row of resourceType Counts))
    :return: (id, row of resourceTypes Counts)
    """

    key = keyValuePair[0]
    value = keyValuePair[1]

    # create a flat tuple
    tupes = tuple([key] + list(value))

    return tupes