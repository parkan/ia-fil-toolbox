#!/usr/bin/env python3

import click
import os
import sys
from typing import List
from shared import read_cids_from_file, ensure_staging_ipfs

def get_someguy_default():
    """Smart default: enabled unless we detect testing environment"""
    # Disable in common testing scenarios
    if any(var in os.environ for var in ['CI', 'PYTEST_CURRENT_TEST', 'UNITTEST']):
        return False
    # Disable if explicitly running tests
    if 'test' in sys.argv[0] or any('test' in arg for arg in sys.argv):
        return False
    # Default to enabled for production use
    return True

@click.group()
@click.option('--someguy/--no-someguy',
              default=get_someguy_default(),
              envvar='SOMEGUY_ENABLED',
              help='Enable someguy delegated routing (auto-disabled in test environments)')
@click.pass_context
def cli(ctx, someguy):
    """IA item filecoin/IPFS toolbox"""
    # Store someguy setting in context for all subcommands
    ctx.ensure_object(dict)
    ctx.obj['someguy'] = someguy
    
    # Note: Daemon startup is deferred to individual commands to avoid
    # starting daemons when just showing help or when arguments are invalid

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
    
    # Start daemon now that we know arguments are valid
    ensure_staging_ipfs(someguy=ctx.obj['someguy'])
    
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

    # Start daemon now that we know arguments are valid
    ensure_staging_ipfs(someguy=ctx.obj['someguy'])

    run_files(cid_list)

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

    # Start daemon now that we know arguments are valid
    ensure_staging_ipfs(someguy=ctx.obj['someguy'])

    run_merge_roots(cid_list, force_check_directories=force_check_directories)

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

    # start daemon now that we know arguments are valid
    ensure_staging_ipfs(someguy=ctx.obj['someguy'])

    run_collect(cid_list)

@cli.command()
@click.pass_context
def run_daemons(ctx):
    """Run persistent IPFS and someguy daemons"""
    from daemon_cmd import run_persistent_daemons
    someguy = ctx.obj['someguy']
    run_persistent_daemons(someguy=someguy)

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
