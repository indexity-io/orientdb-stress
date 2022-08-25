import argparse
import os
import time

from orientdb_stress.record import PropertyType
from orientdb_stress.scenarios import OrientDBScenarioConfig, Scenarios

ORIENTDB_USER = os.getenv("ORIENTDB_USER", "root")
ORIENTDB_PASSWD = os.getenv("ORIENTDB_PASSWD", "password")
ORIENTDB_HOST = os.getenv("ORIENTDB_HOST", "localhost")
ORIENTDB_BASE_NAME = os.getenv("ORIENTDB_BASE_NAME", "dorientdb")
ORIENTDB_BASE_PORT = int(os.getenv("ORIENTDB_BASE_PORT", "2480"))
ORIENTDB_SERVER_COUNT = int(os.getenv("ORIENTDB_SERVER_COUNT", "3"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Stress Tester for OrientDB")

    def list_scenarios(_: argparse.Namespace) -> None:
        print("Available scenarios:")
        for scen in Scenarios.ALL_SCENARIOS:
            print(f"\t{scen.SCENARIO_NAME:20} : {scen.__doc__}")

    def execute_scenario(args: argparse.Namespace) -> None:
        scenario_constructor = next(
            (scen for scen in Scenarios.ALL_SCENARIOS if scen.SCENARIO_NAME == args.scenario_name),
            None,
        )
        if not scenario_constructor:
            print(f"Unknown scenario {args.scenario_name}")
            parser.print_help()
            return

        odb_config = OrientDBScenarioConfig(
            ORIENTDB_BASE_NAME, ORIENTDB_HOST, ORIENTDB_BASE_PORT, ORIENTDB_USER, ORIENTDB_PASSWD, ORIENTDB_SERVER_COUNT
        )
        config = vars(args)
        del config["func"]
        del config["scenario_name"]
        # print(config)
        # print(scenario_constructor)
        run_count = 1 if not args.scenario_count else args.scenario_count
        for c in range(1, run_count + 1):
            if run_count > 1:
                print(f"Run {c}/{run_count}")
            executable_scenario = scenario_constructor(*[odb_config], **config)
            executable_scenario.run(config)
            print()
            if c != run_count:
                time.sleep(5)

    subs = parser.add_subparsers(title="commands", description="valid commands")

    list_parser = subs.add_parser("list", help="List available scenarios.")
    list_parser.set_defaults(func=list_scenarios)

    run_parser = subs.add_parser(
        "run",
        help="Execute scenario",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    run_parser.add_argument("scenario_name", help="Name of the scenario to execute")
    run_parser.add_argument("-c", "--scenario_count", help="Number of times to run the scenario", type=int)
    run_parser.add_argument(
        "--scenario_length",
        help="Duration of the scenario in seconds",
        type=int,
        default="60",
    )
    run_parser.add_argument(
        "--dead_time",
        help="Fractional number of seconds that a node stays dead during a restart",
        type=float,
        default=0,
    )
    run_parser.add_argument(
        "--restart_interval",
        help="Fractional number of seconds between node restarts in restart scenarios",
        type=float,
        default=10,
    )
    run_parser.add_argument(
        "--enable_workload",
        help="Enables client workload during scenario",
        action="store_true",
    )
    run_parser.add_argument("--workload_threads", help="Number of workload threads", type=int, default=1)
    run_parser.add_argument(
        "--workload_rate",
        help="Rate to run workload at in fractional operations/second.",
        type=float,
        default=10,
    )
    run_parser.add_argument(
        "--workload_record_count",
        help="Number of records to use in query workload",
        type=int,
        default=100,
    )
    run_parser.add_argument(
        "--workload_readonly",
        help="Perform only read operations in background workload",
        action="store_true",
    )
    run_parser.add_argument(
        "--workload_validation_readonly",
        help="Perform only read operations in validation workload",
        action="store_true",
    )
    run_parser.add_argument(
        "--workload_type",
        help="The type of record updates to apply during update workloads",
        type=PropertyType.type_for,
        choices=list(PropertyType),
        default=PropertyType.NOT_UNIQUE,
    )
    run_parser.add_argument(
        "--alternating_reset_server",
        help="Reset data directory of stopped node in alternate stop/start scenarios",
        action="store_true",
    )
    run_parser.add_argument(
        "--alternating_kill_server",
        help="Kill server unclearnly in alternate stop/start scenarios",
        action="store_true",
    )

    run_parser.set_defaults(func=execute_scenario)

    args = parser.parse_args()
    if "func" not in args:
        parser.print_help()
        return
    args.func(args)

    return

    # TODO: Separate the non-unique/unique/fulltext
    # index requiring data workloads in case they're object level, not field level


# def funk():
# 	for th in threading.enumerate():
# 	    print(th)
# 	    traceback.print_stack(sys._current_frames()[th.ident])
# 	    print()


# def debug(_: int, frame: Optional[FrameType]) -> None:
#     """Interrupt running process, and provide a python prompt for
#     interactive debugging."""
#     assert frame is not None
#     d = {"_frame": frame}  # Allow access to frame object.
#     d.update(frame.f_globals)  # Unlegss shadowed by global
#     d.update(frame.f_locals)

#     i = code.InteractiveConsole(d)
#     message = "Signal received : entering python shell.\nTraceback:\n"
#     message += "".join(traceback.format_stack(frame))
#     i.interact(message)


# def listen() -> None:
#     signal.signal(signal.SIGUSR1, debug)  # Register handler


# listen()
