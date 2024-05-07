import argparse

config = {
    "allowed_prefixes": [
        "feat",
        "fix",
        "docs",
        "style",
        "model",
        "refactor",
        "test",
        "build",
        "chore",
        "revert",
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("title", help="Pull request title")
    return parser.parse_args()


def lint(title: str) -> list[str]:
    print(f"Linting PR title: `{title}`")
    issues = []
    if not title:
        issues.append("PR title must not be empty")
    if not any([title.startswith(prefix) for prefix in config["allowed_prefixes"]]):
        issues.append(f"PR title must start with one of: {config['allowed_prefixes']}")
    return issues


def main() -> None:
    args = parse_args()
    issues = lint(args.title)
    if len(issues) > 0:
        print(f"Detected {len(issues)} issue(s):")
        print("\n".join(issues))
        exit(1)
    else:
        print("🎉 PR title followes all guidelines!")


if __name__ == "__main__":
    main()
