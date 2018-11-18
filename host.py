#!/usr/bin/env python
from __future__ import print_function

import sys
import threading
import weakref
import time
sys.path.append("../")
from pysyncobj import SyncObj, SyncObjConf, replicated

STORAGE = None
VALUE = 'important_key'
G_STEPS = 'global_steps'
LOCK = 'lockPath'
N_OPERATIONS = 10

REPORT_KEY = 'report_key_global_'

class Host(SyncObj):
    def __init__(self,selfAddress, operation, operand, partners):
        cfg = SyncObjConf(dynamicMembershipChange = True, commandsWaitLeader=True, raftMaxTimeout=2.0, connectionTimeout=3.0)
        new_address = selfAddress[:-1] + str(int(selfAddress[-1]) + 1)
        print('Host:{}'.format(new_address))
        super(Host, self).__init__(new_address, partners, cfg)
        self.__data = {}
        self.operation = operation
        self.operand = float(operand)

    @replicated
    def set(self, key, value):
        self.__data[key] = value

    @replicated
    def pop(self, key):
        self.__data.pop(key, None)

    def get(self, key):
        return self.__data.get(key, None)

    def apply_operation(self, data):
        return {
            '+' : data +  self.operand,
            '-' : data -  self.operand,
            '*' : data *  self.operand,
            '/' : data /  self.operand,
        }[self.operation]

    def update_data(self, key):
        data = self.get(key)
        last_data = float(data)
        data = self.apply_operation(last_data)
        print('Operation applied!\n{} {} {} = {}'.format(last_data, self.operation, self.operand, data))
        return data

class LockImpl(SyncObj):
    def __init__(self, selfAddress, partnerAddrs, autoUnlockTime):
        cfg = SyncObjConf(dynamicMembershipChange = True, raftMaxTimeout=10.0, connectionTimeout=12.0)
        super(LockImpl, self).__init__(selfAddress, partnerAddrs, cfg)
        self.__locks = {}
        self.__autoUnlockTime = autoUnlockTime

    @replicated
    def acquire(self, lockPath, clientID, currentTime):
        existingLock = self.__locks.get(lockPath, None)
        # Auto-unlock old lock
        if existingLock is not None:
            if currentTime - existingLock[1] > self.__autoUnlockTime:
                existingLock = None
        # Acquire lock if possible
        if existingLock is None or existingLock[0] == clientID:
            self.__locks[lockPath] = (clientID, currentTime)
            return True
        # Lock already acquired by someone else
        return False

    @replicated
    def ping(self, clientID, currentTime):
        for lockPath in self.__locks.keys():
            lockClientID, lockTime = self.__locks[lockPath]

            if currentTime - lockTime > self.__autoUnlockTime:
                del self.__locks[lockPath]
                continue

            if lockClientID == clientID:
                self.__locks[lockPath] = (clientID, currentTime)

    @replicated
    def release(self, lockPath, clientID):
        existingLock = self.__locks.get(lockPath, None)
        if existingLock is not None and existingLock[0] == clientID:
            del self.__locks[lockPath]

    def isAcquired(self, lockPath, clientID, currentTime):
        existingLock = self.__locks.get(lockPath, None)
        if existingLock is not None:
            if existingLock[0] == clientID:
                if currentTime - existingLock[1] < self.__autoUnlockTime:
                    return True
        return False


class Lock(object):
    def __init__(self, selfAddress, partnerAddrs, autoUnlockTime):
        print('Lock: {}'.format(selfAddress))
        self.__lockImpl = LockImpl(selfAddress, partnerAddrs, autoUnlockTime)
        self.__selfID = selfAddress
        self.__autoUnlockTime = autoUnlockTime
        self.__mainThread = threading.current_thread()
        self.__initialised = threading.Event()
        self.__thread = threading.Thread(target=Lock._autoAcquireThread, args=(weakref.proxy(self),))
        self.__thread.start()
        while not self.__initialised.is_set():
            pass

    def _autoAcquireThread(self):
        self.__initialised.set()
        try:
            while True:
                if not self.__mainThread.is_alive():
                    break
                time.sleep(float(self.__autoUnlockTime) / 4.0)
                if self.__lockImpl._getLeader() is not None:
                    self.__lockImpl.ping(self.__selfID, time.time())
        except ReferenceError:
            pass

    def tryAcquireLock(self, path):
        self.__lockImpl.acquire(path, self.__selfID, time.time())

    def isAcquired(self, path):
        return self.__lockImpl.isAcquired(path, self.__selfID, time.time())

    def release(self, path):
        self.__lockImpl.release(path, self.__selfID)

    def printStatus(self):
        self.__lockImpl._printStatus()

def main():
    
    # Setup arguments
    p = sys.argv[1]
    ip = sys.argv[2]
    aux_address = ip.split(':')
    aux_address.append(p)
    selfAddress = ":".join(aux_address)
    operation = sys.argv[3]
    operand   = sys.argv[4]
    partners  = sys.argv[5:]
    partners_aux = []
    for part in partners:
        partners_aux.append(part[:-1] + str(int(part[-1]) - 1))

    # Setup data
    global STORAGE, VALUE, N_OPERATIONS, G_STEPS, LOCK
    lock = Lock(selfAddress, partners_aux, 10.0)
    STORAGE = Host(selfAddress, operation, operand, partners)
    
    # Setup Initialization
    if p == '50000':
        # For distributed hosts
        if STORAGE.get(G_STEPS) == None :  
            STORAGE.set(G_STEPS,0)
            STORAGE.set(VALUE,1)
        time.sleep(1.5)
        print('Global Step: {}'.format(STORAGE.get(G_STEPS)))

    input('Press enter to start operations.')

    # Manage operations
    for operation_i in range(N_OPERATIONS):

        # Wait host sync...
        while not STORAGE.isReady():
            time.sleep(0.2)
            print('Wating host to be ready!')
            pass

        # Get lock...
        while not lock.isAcquired(LOCK):
            lock.tryAcquireLock(LOCK)
            time.sleep(0.5)
            # Check for leader...
            if(STORAGE.getStatus()['leader'] == None):
                print('No leader! Waiting for leader...')
        
        print('\nGlobal Step: {}'.format(STORAGE.get(G_STEPS)))
        print("####################################\n Controller: acquired lock.")
        print('Leader: {}'.format(STORAGE.getStatus()['leader']))

        # Apply operations
        aux_old_data = STORAGE.get(VALUE)
        new_data = STORAGE.update_data(VALUE)
        STORAGE.set(VALUE, str(new_data))
        
        # Update global step
        if STORAGE.getStatus()['leader'] == None:
            # Losing the leader makes the counter stop increasing, this is a workaround.
            STORAGE.set(G_STEPS, int(STORAGE.get(G_STEPS)) + 1)
        current_step = int(STORAGE.get(G_STEPS))
        STORAGE.set(G_STEPS, str(current_step + 1)) 
        time.sleep(.5)
        print('Controller: updated global steps to {}'.format(STORAGE.get(G_STEPS)))

        # Update Report 
        last_step = "Step {}: Applied {} {} {} = {} on IP: {} Port:{}\n".format((STORAGE.get(G_STEPS)), aux_old_data, operation, operand, new_data, ip, p)
        with open("local_report.txt", "a") as report:
            report.write(last_step)
        print('Controller: Updated report.')

        # Update Report (for distributed hosts)
        dude = REPORT_KEY + STORAGE.get(G_STEPS)
        STORAGE.set(REPORT_KEY + STORAGE.get(G_STEPS), last_step)

        # Release lock
        lock.release(LOCK)
        print('Controller: released lock.\n{} operations remaining'.format(N_OPERATIONS - operation_i - 1 ))
        print("####################################\n")
        time.sleep(.8)

    print('Finished Operations.')
    if p == '50000':
        input('Wait all operations end, then press enter to receive distributed report.')
        with open("distributed_report.txt", "a") as report:
            for i in range(int(STORAGE.get(G_STEPS))):
                global_step_report = STORAGE.get(REPORT_KEY + str(i+1))
                report.write(global_step_report)
        print('Controller: Distributed report done!')
    else:
        input('Press anything to quit. If there are other hosts applying operations, it is advised to keep this host alive.')
        STORAGE.destroy()

if __name__ == '__main__':
    main()
