from common.input import Mode, Reader
from common.output import Schedule
from common.units import Process
from typing import Callable, List, Tuple
from common.units import Unit
from itertools import takewhile 
import sys 
from copy import deepcopy
import os

class SchedulingAlgorithm():

    def schedule(self,units : List[Unit]) -> Schedule:

        # simulate process flow
        arriving_queue = units.copy()
        ready_queue = []
        finished_queue = []
        scheduling_list = []
        curr_time = 0
        while len(arriving_queue) + len(ready_queue) > 0:

            # add arriving processes at arrial time
            new_arriving_queue =[]
            for u in arriving_queue:
                if u.arrival_time <= curr_time:
                    ready_queue.append(u)
                else:
                    new_arriving_queue.append(u)

            arriving_queue = new_arriving_queue

            # do fictional work if ready queue non empty
            if len(ready_queue) > 0:
                next_unit = self.choose_next(ready_queue)
                scheduling_list.append(next_unit)
                next_unit.do_work()

                if next_unit.finished():
                    finished_queue.append(next_unit)
                    ready_queue.remove(next_unit)
            curr_time += 1

        return Schedule(scheduling_list)


    def choose_next(self, units : List[Unit]) -> Unit:
        """ override this for specific behaviour """
        raise NotImplementedError()

    def order_ready(self,units : List[Unit], chosen : Unit) -> List[Unit]:
        """ override this for specific behaviour """ 
        return units

class ProcessSchedulingAlgorithm(SchedulingAlgorithm):
    pass 

class NonPreemptiveFCFS(SchedulingAlgorithm):
    def __init__(self) -> None:
        self.last : Unit = None 

    def choose_next(self, units: List[Unit]) -> Unit:
        # commit to first choice
        if self.last and not self.last.finished():
                return self.last

        # sort units by arrival time
        sorted_units = sorted(units,key=lambda u : u.arrival_time)
        self.last = sorted_units[0]
        return sorted_units[0]


class PreEmptiveSJF(ProcessSchedulingAlgorithm):
    def choose_next(self, units: List[Process]) -> Unit:
        # sort units by cpu time
        sorted_units = sorted(units,key=lambda u : u.cpu_time_left) 
        return sorted_units[0]
        
class NonPreemptiveSJF(PreEmptiveSJF):
    def __init__(self) -> None:
        self.last : Process = None 

    def choose_next(self, units: List[Process]) -> Unit:
        # commit to first choice
        if self.last and not self.last.finished():
                return self.last 

        self.last = super().choose_next(units)
        return self.last

class RoundRobin(ProcessSchedulingAlgorithm):
    def __init__(self, quantum : int, on_preempt : Callable[[Process],None] = None) -> None:
        self.quantum = quantum
        self.schedules_left = quantum
        self.current : Process = None
        self.on_preempt = on_preempt

    def choose_next(self, units: List[Process]) -> Unit:
        # pre timestep

        if self.current and self.current.finished():
            self.schedules_left = self.quantum
            self.current = None

        if not self.current:
            self.current = units[0]
            self.schedules_left = self.quantum


        # timestep
        next = self.current
        if self.schedules_left - 1 <= 0:
            if self.on_preempt:
                self.on_preempt(self.current)
            self.order_ready(units,self.current)
            self.current = None

        # post timestep
        self.schedules_left -= 1
        
        return next

    def reset(self):
        self.current = None 
        self.schedules_left = self.quantum

    def order_ready(self, units: List[Unit], chosen: Unit) -> List[Unit]:
        units.remove(chosen)
        units.append(chosen)

class MultipleQueues(ProcessSchedulingAlgorithm):
    def __init__(self, quantum : int) -> None:
        self.queues = {}
        self.quantum = quantum

    def choose_next(self, units: List[Process]) -> Unit:
        
        # sort by priority, sort is stable, so arrival order for processes with equal 
        # priority 
        units.sort(key=lambda u:u.priority)

        # if priority hasn't been scheduled before, initialize the RR scheduler 
        highest_priority = units[0].priority
        rr : RoundRobin = self.queues.setdefault(highest_priority,RoundRobin(self.quantum))
        
        # pick subset of the queue corresponding to current priority
        highest_queue = list(takewhile(lambda u: u.priority == highest_priority, units))
        
        # perform round robin scheduling on highest priority,
        # this will possibly have side effects on the queue
        # feed those back into aggregated queue
        next_unit = rr.choose_next(highest_queue)
        units[0:len(highest_queue)] = highest_queue

        return next_unit

class MultilevelFeedbackQueue(ProcessSchedulingAlgorithm):
    def __init__(self, quantum_function : Callable[[int],int]) -> None:
        self.queues = {}
        self.quantum_function = quantum_function
        self.preempted = None 

    def on_preempt(self,p : Process):
        self.preempted = p

    def choose_next(self, units: List[Process]) -> Unit:
        # sort by priority, sort is stable, so arrival order for processes with equal 
        # priority 
        units.sort(key=lambda u:u.priority)

        # if priority hasn't been scheduled before, initialize the RR scheduler 
        highest_priority = units[0].priority
        rr : RoundRobin = self.queues.setdefault(highest_priority,
            RoundRobin(self.quantum_function(highest_priority),on_preempt=self.on_preempt))

        # pick subset of the queue corresponding to current priority
        highest_queue = list(takewhile(lambda u: u.priority == highest_priority, units))
        
        # perform round robin scheduling on highest priority,
        # this will possibly have side effects on the queue
        # feed those back into aggregated queue
        self.preempted = None 
        next_unit = rr.choose_next(highest_queue)


        units[0:len(highest_queue)] = highest_queue

        # check if the last process needs to get booted to the next queue
        if self.preempted:
            # if so, boot it
            self.preempted.priority += 1
            # re-sort, since it was higher up in the list, and relative ordering gets preserved,
            # it will be placed at the head of the lower priority queue, but we want it at the tail, so add infinity when sorting 
            # the last element
            units.sort(key=lambda u:u.priority if u != self.preempted else float('inf'))
            rr.reset()


        return next_unit

class Priority(ProcessSchedulingAlgorithm):
    def __init__(self) -> None:
        self.last : Process = None 

    def choose_next(self, units: List[Unit]) -> Unit:
        # commit to first choice
        if self.last and not self.last.finished():
                return self.last

        # sort units by arrival time
        sorted_units = sorted(units,key=lambda u : u.priority)
        self.last = sorted_units[0]
        return sorted_units[0]


if __name__ == "__main__":
    
    path = None
    mode = None
    try:
        path = sys.argv[1]
        mode = sys.argv[2] 
    except Exception:
        pass 

    quantum = 1
    if len(sys.argv) == 4:
        quantum = int(sys.argv[3])

    vals = ["process","disk","page"] # corresponds to Mode enum indexes
    
    if mode not in vals or not path or not mode:
        print("first argument must be the path to the csv file containing scheduling units")
        print("second argument must be one of: {}".format(vals))
        print("third argument can be either ommitted or set to the time quantum for rr (default 1)")
        print("the input file needs to be a csv file with each line corresponding to a scheduling unit, in one of the following formats:")
        print("process scheduling: pname,arrival time, cpu burst time, priority (lower is better)")
        sys.exit(0)
    else:
        reader = Reader()
        eMode = Mode(vals.index(mode))
        units = reader.read(eMode,path)    

        if eMode == Mode.PROCESS:

            alg_filenames : List[Tuple[str,SchedulingAlgorithm]] = [
                ("FirstComeFirstServed", NonPreemptiveFCFS()),
                ("ShortestJobFirst", NonPreemptiveSJF()),
                ("ShortestRemainingTimeFirst", PreEmptiveSJF()),
                ("RoundRobin", RoundRobin(quantum)),
                ("Priority",  Priority()),
                ("MultipleQueues", MultipleQueues(quantum)),
                ("MultiLevelFeedbackQueue",  MultilevelFeedbackQueue(lambda p : 2 **(p-1)))
            ]

            for (f,a) in alg_filenames:
                uCopy = deepcopy(units)
                schedule = a.schedule(uCopy)
                schedule.save(os.path.dirname(path),"{}.csv".format(f))

        elif eMode == Mode.DISK: 
            pass   
        elif eMode == Mode.PAGE: 
            pass

        print("Saved scheduling data to {}".format(os.path.dirname(path)))