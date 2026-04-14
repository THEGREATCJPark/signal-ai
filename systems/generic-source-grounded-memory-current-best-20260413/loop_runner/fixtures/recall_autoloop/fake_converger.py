#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--round", type=int, required=True)
    args = parser.parse_args()

    workspace = Path(args.workspace)
    feedback_dir = workspace / "critic_feedback"
    critic_names = sorted(path.stem for path in feedback_dir.glob("*.md") if path.name != "index.md")
    feedback_text = "\n\n".join(path.read_text(encoding="utf-8") for path in sorted(feedback_dir.glob("*.md")) if path.name != "index.md")
    common_line = (
        "top60 recall is still too low"
        if feedback_text.count("top60 recall is still too low") >= 2
        else "no stable overlap"
    )
    (workspace / "converged_feedback.md").write_text(
        "\n".join(
            [
                "# Converged Feedback",
                "",
                f"- Round: {args.round}",
                f"- Critics read: {', '.join(critic_names)}",
                f"- Common issue: {common_line}",
                "- Feed this exact duplicate signal to the next solver; do not include one-off critic disagreements.",
                "",
            ]
        ),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
