from mvcc.mvcc import MVCC
import utils.schedule_parser as schedule_parser
from two_phase_lock.two_pl import TwoPL
from occ.occ import OCC

if __name__ == "__main__":
    print("Choose a protocol:\n1. Two Phase Lock\n2. OCC\n3. MVCC\n")
    protocol = input("Enter your choice: ")
    filename = input("Enter the input filename: ")
    output_filename = input("Enter the output filename: ")
    schedule = schedule_parser.ScheduleParser(filename).schedule

    if protocol == "1": 
        twopl = TwoPL(schedule)
        twopl.run()
        schedule_parser.output_schedule(twopl.final_schedule, output_filename)
    elif protocol == "2":
        occ = OCC(schedule)
        occ.run()
        schedule_parser.output_schedule(occ.out, output_filename)
    elif protocol == "3":
        mvcc = MVCC(schedule)
        mvcc.run()
        schedule_parser.output_schedule(mvcc.schedule, output_filename)
    else:
        print("Invalid choice")