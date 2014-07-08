from flowy.boilerplate import workflow_starter
import argparse
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("domain")
    parser.add_argument("name")
    parser.add_argument("version")
    parser.add_argument("--task-list")
    parser.add_argument("--decision-duration", type=int, default=None)
    parser.add_argument("--workflow-duration", type=int, default=None)
    parser.add_argument('args', nargs=argparse.REMAINDER)

    args = parser.parse_args()

    wf = workflow_starter(args.domain, args.name, args.version, args.task_list,
                          args.decision_duration, args.workflow_duration)
    return not wf.start(*args.args)  # 0 is success


if __name__ == '__main__':
    sys.exit(main())
