from .utils import to_csv_string
from .units import Unit
from typing import List

from os.path import join 

class Schedule():
    def __init__(self, schedule_list : List[Unit]) -> None:
        """
            Args:
                schedule_list(`List[int]`): list such that the i'th elem is the unit scheduled at the i'th time unit
            
        """
        self.schedule_list = schedule_list

        # find all units present in schedule and sort them 
        self.units = sorted(list(set(self.schedule_list)),key=lambda u: str(u))
        
        # create 2d array, where [i],[j]th entry represents process i at time j, 
        # a 1 entry meaning the process was on the cpu at that time and 0 otherwise 

        self.array = []
        for u in self.units:
            row = []
            for (t,p) in enumerate(self.schedule_list):
                row.append(int(u == p))

            self.array.append(row)
        # work out interesting intervals (i.e. we shrink the table horizontally, by concatenating columns which have 1's in the same position and 
        # are directly adjacent) 

        curr_time = 0
        curr_unit_idx = self._unit_idx_at_time(curr_time)
        interval_start = 0 

        self.intervals = []
        while curr_time < len(self.schedule_list):
            
            if curr_time + 1 >= len(self.schedule_list):
                self.intervals.append((interval_start,curr_time,curr_unit_idx))
                break 

            next_unit_idx = self._unit_idx_at_time(curr_time + 1)
            if next_unit_idx != curr_unit_idx:
                self.intervals.append((interval_start,curr_time,curr_unit_idx))
                curr_unit_idx = next_unit_idx
                interval_start = curr_time + 1
            
            curr_time += 1

    def _unit_idx_at_time(self,time : int):
        """ returns the unit index in self.array which was scheduled at the given time """
        for (i,r) in enumerate(self.array):
            if r[time]:
                return i

    def _unit_idx(self,unit : Unit):
        self.units.index(unit)

    def save(self,dir : str, file_name : str):
        
        with open(join(dir,file_name),'w') as f:

            # write interval columns
            columns = ["{}-{}({})".format(str(a),str(b),b-a+1) for (a,b,_) in self.intervals]
            columns += ["turnaround time","wait time"]

            header = "unit,{}".format(to_csv_string(columns)) 
            f.write(header + "\n")

            # write rows
            avg_turnaround_time = 0
            avg_wait_time = 0

            for u in self.units:
                row = [str(u)]
                for (a,b,ui) in self.intervals:
                    row.append(str(int(u == self.units[ui])))

                # turnaround time 
                completion_time = max([b for (_,b,ui) in self.intervals if u == self.units[ui]])
                submission_time = u.arrival_time 
                turnaround_time = completion_time - submission_time + 1
                avg_turnaround_time += turnaround_time
                row.append(turnaround_time)

                # wait time 
                burst_time = sum([b - a + 1 for (a,b,ui) in self.intervals if u == self.units[ui]])
                wait_time = turnaround_time - burst_time
                avg_wait_time += wait_time
                row.append(wait_time)

                f.write(to_csv_string(row) + "\n")

            # averages 
            avg_turnaround_time /= len(self.units)
            avg_wait_time /= len(self.units)

            averages = ["averages"] + (['_'] * len(self.intervals)) + [avg_turnaround_time,avg_wait_time]
            f.write(to_csv_string(averages))
        # close file

class TrackSchedule(Schedule):
    def save(self, dir: str, file_name: str):
        with open(join(dir,file_name),'w') as f:
            for t in self.schedule_list:
                f.write(str(t)+",")

            sum_tracks = 0
            for i in range(len(self.schedule_list) - 1):
                sum_tracks += abs(self.schedule_list[i+1].track_number - self.schedule_list[i].track_number)

            f.write("head movements: {}".format(sum_tracks))