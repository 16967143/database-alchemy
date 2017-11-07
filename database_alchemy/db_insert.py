import json

import click
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .db_create import Analysis, Base, Result, Sample


def create_analysis(metadata):
    '''Create an instance of Analysis from an analysis metadata dict.

    Args:
        metadata (dict): dictionary containing the Analysis metadata.

    Returns (:obj:`Analysis`): analysis object which can be inserted
        into the database with the SQLAlchemy ORM.
    '''
    field_names = ['analysis_name', 'date', 'department', 'analyst']

    analysis_data = {}
    for field in field_names:
        if field in metadata:
            analysis_data[field] = metadata[field]
        else:
            # log warning
            pass

    return Analysis(**analysis_data)


def create_sample(metadata, analysis_obj):
    '''Create an instance of Sample from a sample metadata dict.

    Args:
        metadata (dict): dictionary containing the Sample metadata.
        analysis_obj (:obj:`Analysis`): analysis object to be used by
            SQLAlchemy for foreign key association with sample.

    Returns (:obj:`Sample`): sample object which can be inserted
        into the database with the SQLAlchemy ORM.
    '''
    field_names = ['sample_name', 'sample_type', 'sample_description']

    sample_data = {}
    for field in field_names:
        if field in metadata:
            sample_data[field] = metadata[field]
        else:
            # log warning
            pass
    sample_data['analysis'] = analysis_obj  # required for sample:analysis relationship

    return Sample(**sample_data)


def create_result(csv_path, sample_obj):
    '''Create an instance of Result from a results csv file.

    Args:
        csv_path (Union[str, Path]): path to the results CSV file.
        sample_obj (:obj: `Sample`): sample object to be used by
            SQLAlchemy for foreign key association with result.

    Returns: (:obj:`Result`): result object which can be inserted
        into the database with the SQLAlchemy ORM.
    '''
    results_df = pd.read_csv(csv_path)
    results_df = results_df.loc[results_df['sample_name'] == sample_obj.sample_name]
    results_df = results_df.drop('sample_name', axis=1)
    metrics = results_df.to_json(orient='records', lines=True)  # --> '{'met1': 2, 'met2': 4, 'met3': 6}'
    metrics = json.loads(metrics)  # --> {'met1': 2, 'met2': 4, 'met3': 6}

    return Result(metrics=metrics, sample=sample_obj)


@click.command()
@click.argument('metadata_json', type=click.Path(exists=True, dir_okay=False))
@click.argument('results_csv', type=click.Path(exists=True, dir_okay=False))
@click.argument('db_name')
@click.option('-a', '--ip-address', default='127.0.0.1', show_default=True,
              help='the ip address of the PostgreSQL server to bind to.')
@click.option('-p', '--port', default='5432', show_default=True,
              help='the port of the PostgreSQL server to bind to.')
def main(metadata_json, results_csv, db_name, ip_address, port):
    '''Insert new project data into an existing database by supplying a results
    csv file and an accompanying metadata json file describing the analysis.

    The METADATA_JSON file has a fixed structure. It must be valid json
    containing the top-level fields {'Analysis', 'Samples'}. The Analysis block
    must be a mapping of analysis-level metadata. The Samples block must be an
    array of mappings of sample-level metadata. The schema is the following:

    \b
      {
        "Analysis": {
          "analysis_name": "Troubleshoot drop out rates",
          "date": "2017-09-20",
          "department": "IT",
          "analyst": "Guido van Rossum"
        },
        "Samples": [
          {
            "sample_name": "sample01",
            "sample_type": "Reference",
            "sample_description": "NA"
          },
          {
            "sample_name": "sample02",
            "sample_type": "Test",
            "sample_description": "NA"
          }
        ]
      }

    The RESULTS_CSV file must contain a column corresponding to the sample names
    in the json file, followed by a column for each metric. The format is the
    following:

    \b
      sample_name\t{metric_1}\t{metric_2}\t{metric_3}\t.
      ----------------+---------------+---------------+---------------+-----
      sample01\t\t0.6\t\t45\t\t1500\t\t.
      sample02\t\t0.9\t\t12\t\t3000\t\t.

    '''
    engine = create_engine(f'postgresql://{ip_address}:{port}/{db_name}')
    # Bind the engine to the metadata of the Base class so that the
    # declaratives can be accessed through a Session instance
    Base.metadata.bind = engine
    Session = sessionmaker(bind=engine)
    session = Session()

    with open(metadata_json) as of:
        metadata_json = json.load(of)

    try:
        new_analysis = create_analysis(metadata_json['Analysis'])
    except Exception as e:
        # log warning or crash (TBD)
        raise
    else:
        session.add(new_analysis)

    for sample in metadata_json['Samples']:
        try:
            new_sample = create_sample(sample, new_analysis)
            new_result = create_result(results_csv, new_sample)
        except Exception as e:
            # log warning or crash (TBD)
            raise
        else:
            session.add(new_sample, new_result)

    session.commit()