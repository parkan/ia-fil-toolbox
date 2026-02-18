#!/usr/bin/env python3
"""Interactive ia-fil shell using the cmd module."""

import cmd
import shlex
import sys

from ia_fil import cli


class IAShell(cmd.Cmd):
    prompt = "ia> "
    intro = (
        "\nia-fil-toolbox interactive shell\n"
        "Tab-complete subcommands. Type 'help' for usage or 'quit' to exit.\n"
    )

    # ── Subcommand dispatch ─────────────────────────────────────────────
    def _run(self, argv):
        """Invoke a Click command with the given argv."""
        try:
            cli(argv, standalone_mode=False)
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

    def do_daemon_status(self, line):
        """Check IPFS daemon status"""
        self._run(["daemon-status"])

    def do_quit(self, line):
        """Exit the shell"""
        return True

    do_exit = do_quit
    do_EOF = do_quit

    # ── Completions ─────────────────────────────────────────────────────
    def completenames(self, text, *ignored):
        # Use hyphens in display, map to underscores internally
        commands = [
            "extract-items", "metadata", "merge-roots", "collect",
            "daemon-status", "help", "quit", "exit",
        ]
        return [c for c in commands if c.startswith(text)]

    def default(self, line):
        # Allow hyphenated names by mapping to underscore methods
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
