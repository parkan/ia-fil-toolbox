#!/usr/bin/env python3
"""Interactive ia-fil shell using the cmd module."""

import cmd
import os
import re
import shlex
import subprocess
import sys

from ia_fil import cli

os.chdir("/work")


def _parse_storacha_subcommands():
    """Parse storacha --help to discover available subcommands."""
    try:
        result = subprocess.run(
            ["storacha", "--help"], capture_output=True, text=True, timeout=5
        )
        cmds = []
        for m in re.finditer(r"^\s{4}(\S+(?:\s\S+)?)\s{2,}", result.stdout, re.MULTILINE):
            cmds.append(m.group(1).strip())
        return cmds
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return []


class IAShell(cmd.Cmd):
    prompt = "ia> "
    intro = (
        "\nia-fil-toolbox interactive shell\n"
        "Tab-complete subcommands. Type 'help' for usage or 'quit' to exit.\n"
    )

    # ── Subcommand dispatch ─────────────────────────────────────────────
    _GROUP_FLAGS = {'--pin'}

    def _run(self, argv):
        """Invoke a Click command with the given argv.

        Click requires group-level flags (--pin) before the subcommand
        name. This method hoists them automatically so users can type
        them in any position.
        """
        group_flags = [a for a in argv if a in self._GROUP_FLAGS]
        cmd_args = [a for a in argv if a not in self._GROUP_FLAGS]
        try:
            cli(group_flags + cmd_args, standalone_mode=False)
        except SystemExit:
            pass
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)

    def do_extract_items(self, line):
        """Extract items from _files.xml into directories"""
        self._run(["extract-items"] + shlex.split(line))

    def do_metadata(self, line):
        """Fetch and parse metadata files"""
        self._run(["metadata"] + shlex.split(line))

    def do_merge_roots(self, line):
        """Merge multiple root CIDs into single directory"""
        self._run(["merge-roots"] + shlex.split(line))

    def do_collect(self, line):
        """Collect CIDs into a parent directory (shallow)"""
        self._run(["collect"] + shlex.split(line))

    def do_verify(self, line):
        """Check retrievability of each CID via lightweight root-block fetch"""
        self._run(["verify"] + shlex.split(line))

    def do_daemon_status(self, line):
        """Check IPFS daemon status"""
        self._run(["daemon-status"])

    def emptyline(self):
        """Do nothing on empty input (default repeats last command)."""
        pass

    def do_quit(self, line):
        """Exit the shell"""
        return True

    do_exit = do_quit
    do_EOF = do_quit

    # ── Storacha passthrough ────────────────────────────────────────────
    _storacha_cmds = None

    @classmethod
    def _get_storacha_cmds(cls):
        if cls._storacha_cmds is None:
            cls._storacha_cmds = _parse_storacha_subcommands()
        return cls._storacha_cmds

    def do_storacha(self, line):
        """Storacha CLI (w3 storage). Run 'storacha help' for subcommands."""
        subprocess.run(["storacha"] + shlex.split(line))

    def complete_storacha(self, text, line, begidx, endidx):
        return [c for c in self._get_storacha_cmds() if c.startswith(text)]

    # ── Completions ─────────────────────────────────────────────────────
    _ia_commands = [
        "extract-items", "metadata", "merge-roots", "collect", "verify",
        "daemon-status", "storacha", "help", "quit", "exit",
    ]

    def completenames(self, text, *ignored):
        return [c for c in self._ia_commands if c.startswith(text)]

    def default(self, line):
        argv = shlex.split(line)
        method = "do_" + argv[0].replace("-", "_")
        if hasattr(self, method):
            getattr(self, method)(" ".join(argv[1:]))
        else:
            print(f"Unknown command: {argv[0]}. Type 'help' for available commands.")


if __name__ == "__main__":
    try:
        IAShell().cmdloop()
    except KeyboardInterrupt:
        print()
