#!/usr/bin/env python3

import click
from typing import List
from shared import read_cids_from_file

@click.group()
def cli():
    """IA item filecoin/IPFS toolbox"""
    pass

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

if __name__ == "__main__":
    cli()
