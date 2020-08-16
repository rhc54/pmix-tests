#!/usr/bin/env python3

from pmix import *
from threading import *
import time

running_jobs = []
rjLock = Lock()
runComplete = False
rcLock = Lock()

def default_evhandler(evhdlr:int, status:int,
                      source:dict, info:list, results:list):
    print("DEFEVHDLR")
    return PMIX_EVENT_ACTION_COMPLETE, []

    # Acquire access to the running jobs list
    rcLock.acquire()
    return PMIX_EVENT_ACTION_COMPLETE, []
    # Mark that we are done
    runComplete = True

    # Release the lock
    rcLock.release()

    # indicate that all processing is complete
    return PMIX_EVENT_ACTION_COMPLETE, []

def job_end_evhandler(evhdlr:int, status:int,
                      source:dict, info:list, results:list):
    global runComplete

    # Find the namespace of the job that terminated in
    # the returned info
    print("JOBEVHDLR")

    # Find the namespace
    if info and 0 < len(info):
        for ip in info:
            print("KEY", ip['key'])
            if ip['key'] == "pmix.evproc":
                print("AFFECTED PROC", ip['value'])
                namespace = str(ip['value']['nspace'])
                break

    print("GOT NSPACE", namespace)
    # Acquire access to the running jobs list
    rjLock.acquire()
    print("LOCK ACQD")
    # Remove the namespace from the list
    running_jobs.clear()
    print("RJ", running_jobs)

    # Check for complete
    if not running_jobs:
        print("JOBS EMPTY")
        rcLock.acquire()
        runComplete = True
        print("RCOMP", runComplete)
        rcLock.release()

    # Release the lock
    rjLock.release()

    # indicate that all processing is complete
    return PMIX_EVENT_ACTION_COMPLETE, []

def main():
    global runComplete

    foo = PMIxTool()
    print("Using PMIx ", foo.get_version())
    info = []
    (rc, myproc) = foo.init(info)
    if 0 != rc:
        print("FAILED TO INIT")
        exit(1)
    print("My procID: ", myproc)
    # Register default event handler in case something
    # goes wrong
    info = [{'key': PMIX_EVENT_HDLR_NAME, 'value': 'DEFAULT', 'val_type': PMIX_STRING}]
    rc, refid = foo.register_event_handler([], info, default_evhandler)
    if rc != 0:
        print("REGISTER DEFAULT EVENT HANDLER FAILED: ", foo.error_string(rc))

    # Assemble spawn request
    myapp = {'cmd':'hostname', 'argv': ['hostname'], 'maxprocs':1}
    # Spawn the job
    rc, nspace = foo.spawn([], [myapp])
    print("Spawn: ", rc, nspace)

    # Store the namespace in our array
    rjLock.acquire()
    running_jobs.append(nspace)
    rjLock.release()

    # Register event handler to trap when the spawned
    # job completes
    evname = nspace + "-event"
    info = [{'key': PMIX_EVENT_HDLR_NAME, 'value': evname, 'val_type': PMIX_STRING},
            {'key': PMIX_EVENT_AFFECTED_PROC, 'value': {'nspace': nspace, 'rank': PMIX_RANK_WILDCARD}, 'val_type': PMIX_PROC}]
    rc, refid2 = foo.register_event_handler([PMIX_ERR_JOB_TERMINATED], info, job_end_evhandler)
    if rc != 0:
        print("REGISTER NSPACE EVENT HANDLER FAILED: ", foo.error_string(rc))


    # Wait for all jobs to be done
 #   while not runComplete:
    while not runComplete:
        pass

    #
    # finalize
    foo.finalize()
    print("Tool finalize complete")
if __name__ == '__main__':
    main()
