import click
from collections import OrderedDict
from sqlalchemy import Column, ForeignKey, create_engine
from sqlalchemy import Integer, String, Date, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class Analysis(Base):
    __tablename__ = 'analyses'

    # Declaration of columns for the table analyses
    analysis_id = Column(Integer, primary_key=True)
    analysis_name = Column(String(50), nullable=False)
    date = Column(Date, default=func.now())
    department = Column(String(20), nullable=False)
    analyst = Column(String, nullable=False)
    samples = relationship("Sample", back_populates="analysis")


    def display(self):
        return OrderedDict([('analysis_id', self.analysis_id),
                            ('analysis_name', self.analysis_name),
                            ('date', self.date),
                            ('department', self.department),
                            ('analyst', self.analyst)])

    def __repr__(self):
        return f"Analysis(analysis_id: {self.analysis_id}, analysis_name: '{self.analysis_name}', " \
               f"date: '{self.date}', department: '{self.department}', analyst: '{self.analyst}')"


class Sample(Base):
    __tablename__ = 'samples'

    # Declaration of columns for the table samples.
    sample_id = Column(Integer, primary_key=True)
    sample_name = Column(String(30), nullable=False)
    sample_type = Column(String)
    sample_description = Column(String(50))
    analysis_id = Column(Integer, ForeignKey('analyses.analysis_id'))
    analysis = relationship("Analysis", back_populates="samples")
    results = relationship("Result", back_populates="sample")


    def __repr__(self):
        return f"Sample(sample_id: {self.sample_id}, sample_name: '{self.sample_name}', " \
               f"sample_type: '{self.sample_type}', sample_description: '{self.sample_description}')"


class Result(Base):
    __tablename__ = 'results'

    # Here we define columns for the table results
    result_id = Column(Integer, primary_key=True)
    sample_id = Column(Integer, ForeignKey('samples.sample_id'))
    metrics = Column(JSONB, nullable=False)
    sample = relationship("Sample", back_populates="results")


    def __repr__(self):
        return f"Result(result_id: {self.result_id}, sample_id: {self.sample_id}, " \
               f"metrics: '{self.metrics}')"


@click.command()
@click.argument('db_name')
@click.option('-a', '--ip-address', default='127.0.0.1', show_default=True,
              help='the ip address of the PostgreSQL server to bind to.')
@click.option('-p', '--port', default='5432', show_default=True,
              help='the port of the PostgreSQL server to bind to.')
def main(db_name, ip_address, port):
    '''Set up a project database for tracking analyses, samples, and results.

    The database schema applied to the database is generic in the sense that
    it should be suitable for most projects. The schema is the following:

    \b
      Table\tField\t\tType
      ---------+---------------+--------------
      Analyses:\tanalysis_id\t(Integer, PK)
               \tanalysis_name\t(String)
               \tdate\t\t(Date)
               \tdepartment\t(String)
               \tanalyst\t\t(String)
    \b
      Table\tField\t\tType
      ---------+---------------+--------------
      Samples:\tsample_id\t(Integer, PK)
              \tanalysis_id\t(Integer, FK)
              \tsample_name\t(String)
              \tsample_type\t(String)
              \tsample_description\t(String)
    \b
      Table\tField\t\tType
      ---------+---------------+--------------
      Results:\tresult_id\t(Integer, PK)
              \tsample_id\t(Integer, FK)
              \tmetrics\t\t(JSON)

    DB_NAME should be the name of a blank database which has already been created
    prior to running this script. To create a database, execute the following,
    where {project_x} refers to the name of the project/database:

    \b
      $ initdb Databases
      $ pg_ctl -D Databases -l logfile start
      $ createdb {projectx}
    '''
    engine = create_engine(f'postgresql://{ip_address}:{port}/{db_name}')
    Base.metadata.create_all(engine)