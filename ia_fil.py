#!/usr/bin/env python3

import click
from typing import List
from shared import read_cids_from_file, ensure_staging_ipfs

@click.group()
@click.pass_context
def cli(ctx):
    """IA item filecoin/IPFS toolbox"""
    # Ensure daemon is running for all commands (except daemon management and utility commands)
    if ctx.invoked_subcommand not in ['start-daemons', 'stop-daemons', 'daemon-status', 'completion']:
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

@cli.group()
def completion():
    """Shell completion utilities"""
    pass

@completion.command()
@click.argument('shell', type=click.Choice(['bash', 'zsh', 'fish']))
def install(shell):
    """Install shell completion for the specified shell"""
    import subprocess
    import os
    
    if shell == 'bash':
        completion_script = f'eval "$(_IA_FIL_COMPLETE=bash_source ia-fil)"'
        bashrc_path = os.path.expanduser('~/.bashrc')
        
        # Check if already installed
        try:
            with open(bashrc_path, 'r') as f:
                if completion_script in f.read():
                    click.echo("Bash completion already installed!")
                    return
        except FileNotFoundError:
            pass
        
        # Add to .bashrc
        with open(bashrc_path, 'a') as f:
            f.write(f'\n# ia-fil completion\n{completion_script}\n')
        
        click.echo("Bash completion installed! Run 'source ~/.bashrc' or start a new shell to enable.")
        
    elif shell == 'zsh':
        completion_script = f'eval "$(_IA_FIL_COMPLETE=zsh_source ia-fil)"'
        zshrc_path = os.path.expanduser('~/.zshrc')
        
        # Check if already installed
        try:
            with open(zshrc_path, 'r') as f:
                if completion_script in f.read():
                    click.echo("Zsh completion already installed!")
                    return
        except FileNotFoundError:
            pass
        
        # Add to .zshrc
        with open(zshrc_path, 'a') as f:
            f.write(f'\n# ia-fil completion\n{completion_script}\n')
        
        click.echo("Zsh completion installed! Run 'source ~/.zshrc' or start a new shell to enable.")
        
    elif shell == 'fish':
        completion_dir = os.path.expanduser('~/.config/fish/completions')
        os.makedirs(completion_dir, exist_ok=True)
        completion_file = os.path.join(completion_dir, 'ia-fil.fish')
        
        # Generate completion script
        result = subprocess.run(
            ['ia-fil', 'completion', 'show', 'fish'],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            with open(completion_file, 'w') as f:
                f.write(result.stdout)
            click.echo("Fish completion installed!")
        else:
            click.echo("Failed to generate fish completion script", err=True)

@completion.command()
@click.argument('shell', type=click.Choice(['bash', 'zsh', 'fish']))
def show(shell):
    """Show completion script for the specified shell"""
    import os
    prog_name = 'ia-fil'
    
    if shell == 'bash':
        click.echo(f'_IA_FIL_COMPLETE=bash_source {prog_name}')
    elif shell == 'zsh':
        click.echo(f'_IA_FIL_COMPLETE=zsh_source {prog_name}')
    elif shell == 'fish':
        click.echo(f'_IA_FIL_COMPLETE=fish_source {prog_name}')

if __name__ == "__main__":
    cli()
