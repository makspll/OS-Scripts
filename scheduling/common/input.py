

from typing import List
from enum import Enum
from .units import Process, Unit, Track


class Mode(Enum):
    PROCESS = 0
    DISK = 1
    PAGE = 2

class Reader():
    def __init__(self) -> None:
        pass

    def read(self,mode : Mode, path : str ) -> List[Unit]:
        with open(path,"r") as f:
            creator = None 

            if mode == Mode.PROCESS:
                creator = lambda str : Process.parse(str)
            elif mode == Mode.DISK:
                creator = lambda str : Track.parse(str)
            units = []
            for l in f.readlines():
                units.append(creator(l))

            return units

