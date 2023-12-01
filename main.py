from two_phase_lock.two_pl import twoPL
import utils.schedule_parser as schedule_parser


if __name__ == "__main__":
    schedule = schedule_parser.parse_data("./test/input.txt")
    twoPL(schedule)