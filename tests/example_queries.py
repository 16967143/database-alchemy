import datetime

from sqlalchemy import create_engine, cast, Date
from sqlalchemy.orm import sessionmaker

from database_alchemy.db_create import Analysis, Base
from database_alchemy.db_query import get_results_by_analysis, get_results_by_sample


# Initialise DB connection
db_name = 'projectx'
ip_address = '127.0.0.1'
port = '5432'

engine = create_engine(f'postgresql://{ip_address}:{port}/{db_name}')
Base.metadata.bind = engine
Session = sessionmaker(bind=engine)
session = Session()

# find all analyses:
analyses = session.query(Analysis)
for analysis in analyses:
    print(analysis.analysis_id)

# find all analyses where date < or > x
analysis = session.query(Analysis).filter(cast(Analysis.date, Date) <= '2017-09-20').all()
analysis = session.query(Analysis).filter(cast(Analysis.date, Date) <= datetime.date.today()).all()
print(analysis)

# find all analyses where analyst/department/etc = x
analysis = session.query(Analysis).filter(Analysis.analyst == 'DMT').all()
analysis = session.query(Analysis).filter(Analysis.department == 'QC').all()
print(analysis)

# find all samples and results for a given analysis and return as a pandas data frame
df = get_results_by_analysis(session, analysis_id=[1])
print(df)

# find all results for a given sample and return as a pandas data frame
df = get_results_by_sample(session, sample_name=['S01'])
print(df)