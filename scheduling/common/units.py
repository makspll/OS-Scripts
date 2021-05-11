

class Unit():
    def __init__(self, arrival_time : int, name : str) -> None:
        self.arrival_time = arrival_time
        self.name = name 

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return self.__str__()

    def do_work(self)-> None:
        """ override """
        pass

    def finished(self) -> bool:
        """ override """
        pass

    def parse(csvLine :str):
        """ override """
        pass

class Process(Unit):
    def __init__(self, arrival_time: int, name: str, cpu_time : int, priority : int = None) -> None:
        super().__init__(arrival_time, name)

        self.cpu_time = cpu_time
        self.priority = priority
        self.cpu_time_left = cpu_time

    def do_work(self):
        self.cpu_time_left -= 1

    def finished(self):
        return self.cpu_time_left <= 0
    
    @staticmethod
    def parse(csvLine : str) -> Unit :
        params = csvLine.split(",")

        name = params[0]
        arrival = int(params[1])
        cpu_time = int(params[2])
        priority = int(params[3])
        return Process(arrival,name,cpu_time,priority=priority)

class Track(Unit):
    def __init__(self, arrival_time: int, name: str, track_number : int) -> None:
        super().__init__(arrival_time, name)

        self.track_number = track_number
        self.read = False
    
    def do_work(self) -> None:
        self.read = True 

    def finished(self) -> bool:
        return self.read

    @staticmethod
    def parse(csvLine : str) -> Unit :
        params = csvLine.split(",")

        name = params[0]
        arrival = int(params[1])
        track = int(params[2])
        return Track(arrival,name,track)