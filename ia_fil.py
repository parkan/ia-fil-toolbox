#!/usr/bin/env python3

import click
from typing import List
from shared import read_cids_from_file, ensure_staging_ipfs

@click.group()
@click.pass_context
def cli(ctx):
    """IA item filecoin/IPFS toolbox"""
    # Ensure daemon is running for all commands (except daemon management commands)
    if ctx.invoked_subcommand not in ['start-daemons', 'stop-daemons', 'daemon-status']:
        ensure_staging_ipfs()
    
    # Ensure context object exists for cleanup
    ctx.ensure_object(dict)

@cli.command()
@click.argument('cids', nargs=-1)
@click.option('-f', '--file', type=click.Path(exists=True), help='File containing CIDs (plain text or CSV with "cid" column)')
@click.option('--db', default='metadata.db', help='SQLite database path')
def metadata(cids, file, db):
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
    
    run_metadata(cid_list, db)

@cli.command()
@click.argument('cids', nargs=-1)
@click.option('-f', '--file', type=click.Path(exists=True), help='File containing CIDs (plain text or CSV with "cid" column)')
def extract_items(cids, file):
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

    run_files(cid_list)

@cli.command()
@click.argument('cids', nargs=-1)
@click.option('-f', '--file', type=click.Path(exists=True), help='File containing CIDs (plain text or CSV with "cid" column)')
def merge_roots(cids, file):
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

    run_merge_roots(cid_list)

@cli.command()
def start_daemons():
    """Start IPFS staging daemon"""
    from daemon_cmd import run_start_daemons
    run_start_daemons()

@cli.command()
def stop_daemons():
    """Stop IPFS staging daemon"""
    from daemon_cmd import run_stop_daemons
    run_stop_daemons()

@cli.command()
def daemon_status():
    """Check IPFS daemon status"""
    from daemon_cmd import run_daemon_status
    run_daemon_status()

if __name__ == "__main__":
    cli()
