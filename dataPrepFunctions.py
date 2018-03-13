"""
Helper functions for FHIRdataPrep.py. Most of the functions directly
relate to extracting specific resources from a FHIR bundle
"""



from dsfunction.resources.maps import SYSTEM_URL_TO_SHORT_NAME_MAP as URL_SHORT


def generateTuples(fhirs):
    """
    Take a list of JSON objects (FHIR), extract information
    and yeild tuples to distributed datasets
    :param fhirs: A list of FHIR formatted objects
    :return: tuples containing (id, resourceType, date, system, code)
    """

    # subset the list
    if type(fhirs) == dict:
        fhirs = fhirs['entry']

    # extract a list of tuples
    extractions = extractFromEvents(fhirs)

    for tup in extractions: yield tup



def extractFromEvents(entryList):
    """
    Given a list of entries from a FHIR object, serially operate and extract
    the relevant entries 'Condition', 'Observation', 'Procedure', 'MedicationOrder'
    :param eventList: A list of entries from fire object
    :return: A list of tuples from relevant events
    """

    entryInformation = []  # list to return
    count = 0  # counter for error identification

    # ID errors
    errorIndex = []
    errorKey = []

    for x in entryList:

        # easy typing
        resourceType = x['resource']['resourceType']
        extraction = False

        try:

            # match to entries that we are interested in
            if resourceType == 'Condition':
                extraction = conditionExtract(x)
            elif resourceType == 'Observation':
                extraction = observationExtract(x)
            elif resourceType == 'Procedure':
                extraction = procedureExtract(x)
            elif resourceType == 'MedicationOrder':
                extraction = medicationExtract(x)

        except IndexError:
            extraction = ('IndexError')
        except KeyError:
            extraction = ('KeyError')
        except TypeError:
            extraction = ('TypeError')

        # append the information that we are interested in
        if extraction: entryInformation.append(extraction)

        # reference point for errors
        count += 1

    return entryInformation  # error checking -> errorIndex, errorKey



def conditionExtract(conditionEvent):
    """
    This function expects a top level event object
    extracted directly from an event list
    :param conditionEvent: An event object with 'resourceType' equal to 'Condition'
    :return: a tuple with (id, date, system, code)
    id -
    """

    # easy typing/readability
    entry = conditionEvent['resource']

    # extract patient ID - last eight numbers seem to be the same across
    patientID = entry['patient']['reference'].split('/')[1]

    # extract date - do I need validation on this? Are date structures the same always?
    date = entry['dateRecorded']

    # extract system - match this to the appropriate code string
    system = URL_SHORT[entry['code']['coding'][0]['system']]

    # extract code
    code = entry['code']['coding'][0]['code']

    return (patientID, 'Condition', date, system, code)



def observationExtract(observationEntry):
    """
    This function expects a top level event object
    extracted directly from an event list
    :param observationEvent: An event object with 'resourceType' equal to 'Observation'
    :return: a tuple with (id, date, system, code)
    id -
    """

    # easy typing/readability
    entry = observationEntry['resource']

    # extract patient ID - last eight numbers seem to be the same across
    patientID = entry['subject']['reference'].split('/')[1]

    # extract date - note that there are two types of time objects
    # 'effectiveDateTime', 'issued' As per Rohun, 'effectiveDateTime' is chosen
    date = entry['effectiveDateTime']

    # extract system - match this to the appropriate code string
    system = URL_SHORT[entry['code']['coding'][0]['system']]

    # extract code
    code = entry['code']['coding'][0]['code']

    return (patientID, 'Observation', date, system, code)



def procedureExtract(procedureEntry):
    """
    This function expects a top level event object
    extracted directly from an event list
    :param procedureEvent: An event object with 'resourceType' equal to 'Procedure'
    :return: a tuple with (id, date, system, code)
    id -
    """

    # easy typing/readability
    entry = procedureEntry['resource']

    # extract patient ID - last eight numbers seem to be the same across
    patientID = entry['subject']['reference'].split('/')[1]

    # extract date - note that there are two types of time objects
    # 'effectiveDateTime', 'issued' As per Rohun, 'effectiveDateTime' is chosen
    date = entry['performedDateTime'][:10]

    # extract system - match this to the appropriate code string
    system = URL_SHORT[entry['code']['coding'][0]['system']]

    # extract code
    code = entry['code']['coding'][0]['code']

    return (patientID, 'Procedure', date, system, code)



def medicationExtract(medicationEntry):
    """
    This function expects a top level event object
    extracted directly from an event list
    :param procedureEvent: An event object with 'resourceType' equal to 'MedicationOrder'
    :return: a tuple with (id, date, system, code)
    id -
    """

    # easy typing/readability
    entry = medicationEntry['resource']

    # extract patient ID - last eight numbers seem to be the same across
    patientID = entry['patient']['reference'].split('/')[1]

    # extract date - note that in medication orders there is a date range
    # the dateWritten is assumed to be appropriate
    date = entry['dateWritten']

    # extract stem for code related information
    codeStem = entry['medicationCodeableConcept']['coding'][0]

    # extract system - match this to the appropriate code string
    system = URL_SHORT[codeStem['system']]

    # extract code
    code = codeStem['code']

    return (patientID, 'MedicationOrder', date, system, code)