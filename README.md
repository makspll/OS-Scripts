# OS-Scripts
Helper scripts to help with the Operating Systems course at the UoE

# Usage

`python3 scheduling.py <input csv file path> <process/disk/page> <optional time quantum, default 1>`

the input csv file contains a scheduling unit per line with the following formats:

process scheduling:
```csv
process name, arrival time (int), cpu burst time (int), priority (int, lower is better)
```



the output will be a number of different csv files containing the minified gantt charts for each relevant scheduling algorithm like so:
```csv
unit,0-0(1),1-2(2),3-5(3),turnaround time,wait time
p1,1,0,0,1,0
p2,0,1,0,3,1
p3,0,0,1,6,3
averages,_,_,_,3.3333333333333335,1.3333333333333333
```

TBC
