import argparse
import sys

from flowy import SWFWorkflowStarter


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("domain")
    parser.add_argument("name")
    parser.add_argument("version")
    parser.add_argument("--task-list")
    parser.add_argument("--decision-duration", type=int, default=None)
    parser.add_argument("--workflow-duration", type=int, default=None)
    parser.add_argument("--child-policy", type=str, default=None)
    parser.add_argument('args', nargs=argparse.REMAINDER)

    args = parser.parse_args()

    starter = SWFWorkflowStarter(args.domain, args.name, args.version,
                                 args.task_list, args.decision_duration,
                                 args.workflow_duration,
                                 child_policy=args.child_policy)
    return not starter(*args.args)  # 0 is success


if __name__ == '__main__':
    sys.exit(main())
