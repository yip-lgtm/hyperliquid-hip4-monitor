#!/usr/bin/env python3
"""
HIP4 Monitor Auto Backup Script
Auto-commits code + important data to GitHub.
Usage: python3 backup.py ["optional commit message"]
"""
import subprocess
import datetime
import sys
from pathlib import Path


def run_command(cmd: list, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, capture_output=True, text=True)


def auto_backup(commit_message: str = None, push: bool = True):
    project_root = Path(__file__).parent.resolve()

    if commit_message is None:
        commit_message = f"Auto backup - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

    try:
        subprocess.run(
            ["git", "rev-parse", "--is-inside-work-tree"],
            cwd=project_root, check=True, capture_output=True
        )
    except subprocess.CalledProcessError:
        print("Not a git repo, skipping backup")
        return

    files_to_add = [
        "monitor.py",
        "skills/",
        "hip4_events.jsonl",
        "hip4_performance.json",
        "last_backtest.json",
        "*.txt",
        "backup.py",
        ".gitignore",
    ]

    for pattern in files_to_add:
        run_command(["git", "-C", str(project_root), "add", pattern], check=False)

    diff_result = run_command(
        ["git", "-C", str(project_root), "diff", "--cached", "--quiet"],
        check=False
    )

    if diff_result.returncode == 0:
        print("No changes, skipping backup")
        return

    run_command(["git", "-C", str(project_root), "commit", "-m", commit_message])
    print(f"Committed: {commit_message}")

    if push:
        try:
            run_command(["git", "-C", str(project_root), "push"])
            print("Pushed to remote")
        except subprocess.CalledProcessError as e:
            print(f"Push failed: {e}")


if __name__ == "__main__":
    message = sys.argv[1] if len(sys.argv) > 1 else None
    auto_backup(commit_message=message)
