#!/usr/bin/env python3

import click
from typing import List
from shared import read_cids_from_file

@click.group()
def cli():
    """Filecoin metadata crawler"""
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
def files(cids, file):
    """Process files.xml for each CID"""
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

if __name__ == "__main__":
    cli()
