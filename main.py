import utils.schedule_parser as schedule_parser
from two_phase_lock.two_pl import TwoPL
from occ.occ import OCC

if __name__ == "__main__":
    schedule = schedule_parser.ScheduleParser("./test/deadlock.txt").schedule
    twopl = TwoPL(schedule)
    twopl.run()

    # schedule = schedule_parser.ScheduleParser("./test/occ1.txt").schedule
    # occ = OCC(schedule)
    # occ.run()
