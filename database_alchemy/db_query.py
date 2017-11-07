import datetime
from collections import OrderedDict

import click
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .db_create import Analysis, Base, Result, Sample


def return_dataframe(query):
    '''Convert a SQLAlchemy query object into a data frame for plotting.

    The query object assumes that instances of both Sample and Result are
    present.

    Args:
        query (:obj: `Query`): SQLAlchemy query result.

    Returns (:obj: `DataFrame`): pandas data frame with columns corresponding
      to sample names and metrics from the results table (flattened from json).
    '''
    rows = []
    for record in query:
        result, sample = record
        row = OrderedDict([('sample_name', sample.sample_name),
                           ('analysis_id', sample.analysis_id)])
        row.update(result.metrics.items())
        rows.append(row)

    return pd.DataFrame(rows)


def filter_dataframe(df, filter_kwargs):
    '''Filter a data frame by one or more columns.

    Filtering is specified by passing a dictionary with keys corresponding to column
    names in the data frame to filter on, and values representing the desired values
    within those columns.

    Args:
        df (:obj: `DataFrame`): generic pandas data frame to be filtered.
        filter_kwargs (dict): mapping of [column in data frame] : [desired value].

    Returns (:obj: `DataFrame`): a subset of df in which rows matching the criteria
      specified in filter_kwargs have been retained.
    '''
    for column_name, value in filter_kwargs.items():
        if value:
            if column_name == 'date_after':
                df = df[df['date'] > value]
            elif column_name == 'date_before':
                df = df[df['date'] < value]
            else:
                df = df[df[column_name] == value]

    return df


def get_analyses(session):
    '''Query the database for all analyses and return the result as a data frame.

    Args:
        session (:obj: `Session`): SQLAlchemy session instance to query.

    Returns (:obj: `DataFrame`): pandas data frame with columns corresponding
      to analysis information.
      '''
    query = session.query(Analysis)
    rows = []
    for analysis in query:
        rows.append(analysis.display())

    return pd.DataFrame(rows)


def get_results_by_analysis(session, analysis_id=None):
    '''Query the database for all samples and associated metrics linked
    to one or more analysis_id and return the result as a data frame.

    The metrics in the results table are stored in the database as json.
    This function also unpacks these metrics as key:value pairs and creates
    separate columns for them in the resulting data frame.

    Args:
        session (:obj: `Session`): SQLAlchemy session instance to query.
        analysis_id (Union[int, List[int]]): analysis id(s) to use for filtering
        records.

    Returns (:obj: `DataFrame`): pandas data frame with columns corresponding
      to sample names and metrics from the results table (flattened from json).
    '''
    query = session.query(Result, Sample).join(Sample)

    if analysis_id and isinstance(analysis_id, list):
        query = query.filter(Sample.analysis_id.in_(analysis_id)).all()
    elif analysis_id:
        query = query.filter(Sample.analysis_id == analysis_id).all()

    return return_dataframe(query)


def get_results_by_sample(session, sample_name=None):
    '''Query the database for all results linked to one or more sample and
    return the result as a data frame.

    Samples can be supplied as either a sample_name (str) or list of sample_names
    if multiple samples are to be queried.

    Args:
        session (:obj: `Session`): SQLAlchemy session instance to query.
        sample_name (Union[str, List[str]]): sample name(s) to use for filtering
        records.

    Returns (:obj: `DataFrame`): pandas data frame with columns corresponding
      to sample names and metrics from the results table (flattened from json).
    '''
    query = session.query(Result, Sample).join(Sample)

    if sample_name and isinstance(sample_name, list):
        query = query.filter(Sample.sample_name.in_(sample_name)).all()
    elif sample_name:
        query = query.filter(Sample.sample_name == sample_name).all()

    return return_dataframe(query)


@click.group()
@click.option('-a', '--ip-address', default='127.0.0.1', show_default=True,
              help='the ip address of the PostgreSQL server to bind to.')
@click.option('-p', '--port', default='5432', show_default=True,
              help='the port of the PostgreSQL server to bind to.')
@click.option('-o', '--output-csv', default=None, type=click.Path(dir_okay=False),
              help='write the results to csv file.')
@click.argument('db_name')
@click.pass_context
def cli(ctx, db_name, ip_address, port, output_csv):
    '''Perform a query on a project database.

    Query results can either be sent to stdout (default) or written to a csv file
    when the --output-csv option is supplied.
    '''
    engine = create_engine(f'postgresql://{ip_address}:{port}/{db_name}')
    Base.metadata.bind = engine
    Session = sessionmaker(bind=engine)
    ctx.obj['session'] = Session()
    ctx.obj['output_csv'] = output_csv


@cli.command()
@click.pass_context
def display_databases(ctx):
    '''Display a list of available databases.'''
    pass


@cli.command()
@click.option('-a', '--date-after', default=datetime.date(2015, 1, 1), show_default=True,
              help='only display results that occurred after a certain date')
@click.option('-b', '--date-before', default=datetime.date.today(), show_default=True,
              help='only display results that occurred before a certain date')
@click.option('-d', '--department', default=None, show_default=False,
              help='only display results for a specific department')
@click.option('-n', '--analyst-name', default=None, show_default=False,
              help='only display results for a specific analyst')
@click.pass_context
def display_analyses(ctx, date_after, date_before, department, analyst):
    '''Display information for all the analyses in a database.

    Results can optionally be filtered by date, department, or analyst name.
    '''
    session = ctx.obj['session']
    output_csv = ctx.obj['output_csv']
    filter_kwargs = {'date_after': date_after,
                     'date_before': date_before,
                     'department': department,
                     'analyst': analyst}

    df = get_analyses(session)
    df = filter_dataframe(df, filter_kwargs)

    if output_csv:
        df.to_csv(output_csv, index=False)
    else:
        click.echo(df)


def main():
    cli(obj={})