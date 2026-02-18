#!/usr/bin/env python3

import click
import os
import sys
from shared import read_cids_from_file, ensure_staging_ipfs, check_storacha_auth

@click.group()
@click.option('--pin', is_flag=True, default=False,
              help='Pin output CAR files to storacha after generation')
@click.pass_context
def cli(ctx, pin):
    """IA item filecoin/IPFS toolbox"""
    ctx.ensure_object(dict)
    ctx.obj['pin'] = pin

    if pin and ctx.invoked_subcommand not in (None, 'run-daemons', 'daemon-status', 'metadata'):
        if not check_storacha_auth():
            raise SystemExit(1)

@cli.command()
@click.argument('cids', nargs=-1)
@click.option('-f', '--file', type=click.Path(exists=True), help='File containing CIDs (plain text or CSV with "cid" column)')
@click.option('--db', default='metadata.db', help='SQLite database path')
@click.pass_context
def metadata(ctx, cids, file, db):
    """Fetch and parse metadata files"""
    from metadata_cmd import run_metadata

    cid_list = []
    if file:
        cid_list = read_cids_from_file(file)
    elif cids:
        cid_list = list(cids)
    else:
        click.echo("Error: Must provide either CIDs as arguments or use --file option", err=True)
        raise click.Abort()

    ensure_staging_ipfs()

    run_metadata(cid_list, db)

@cli.command()
@click.argument('cids', nargs=-1)
@click.option('-f', '--file', type=click.Path(exists=True), help='File containing CIDs (plain text or CSV with "cid" column)')
@click.pass_context
def extract_items(ctx, cids, file):
    """Extract items from _files.xml into directories"""
    from files_cmd import run_files

    cid_list = []
    if file:
        cid_list = read_cids_from_file(file)
    elif cids:
        cid_list = list(cids)
    else:
        click.echo("Error: Must provide either CIDs as arguments or use --file option", err=True)
        raise click.Abort()

    ensure_staging_ipfs()

    run_files(cid_list, pin=ctx.obj['pin'])

@cli.command()
@click.argument('cids', nargs=-1)
@click.option('-f', '--file', type=click.Path(exists=True), help='File containing CIDs (plain text or CSV with "cid" column)')
@click.option('--force-check-directories', is_flag=True, default=False,
              help='Force expensive directory checks (default: use file extension heuristics)')
@click.pass_context
def merge_roots(ctx, cids, file, force_check_directories):
    """Merge multiple root CIDs into single directory"""
    from merge_roots_cmd import run_merge_roots

    cid_list = []
    if file:
        cid_list = read_cids_from_file(file)
    elif cids:
        cid_list = list(cids)
    else:
        click.echo("Error: Must provide either CIDs as arguments or use --file option", err=True)
        raise click.Abort()

    ensure_staging_ipfs()

    run_merge_roots(cid_list, force_check_directories=force_check_directories, pin=ctx.obj['pin'])

@cli.command()
@click.argument('cids', nargs=-1)
@click.option('-f', '--file', type=click.Path(exists=True), help='File containing CIDs (plain text or CSV with "cid" column)')
@click.pass_context
def collect(ctx, cids, file):
    """Collect CIDs into a parent directory (shallow, no subgraph reads)"""
    from collect_cmd import run_collect

    cid_list = []
    if file:
        cid_list = read_cids_from_file(file)
    elif cids:
        cid_list = list(cids)
    else:
        click.echo("Error: Must provide either CIDs as arguments or use --file option", err=True)
        raise click.Abort()

    ensure_staging_ipfs()

    run_collect(cid_list, pin=ctx.obj['pin'])

@cli.command()
def run_daemons():
    """Run persistent IPFS and someguy daemons"""
    from daemon_cmd import run_persistent_daemons
    run_persistent_daemons()

@cli.command()
def daemon_status():
    """Check IPFS daemon status"""
    from daemon_cmd import run_daemon_status
    run_daemon_status()

if __name__ == "__main__":
    cli()
