from sqlalchemy import *
import pandas as pd


def loadvuln(csv_dir, vulnID, connstr, vuln_point_table="vulnIndex"):
    df = pd.read_csv(csv_dir)
    if df.columns.to_list()[0].isdigit():
        if len(df.columns) == 4:
            colnames = ['hazIntensity_from',
                        'hazIntensity_to', 'vulnAVG', 'vulnSTD']
        elif len(df.columns) == 3:
            colnames = ['hazIntensity_from', 'hazIntensity_to', 'vulnAVG']
        else:
            return ("Wrong number of columns it should be either 3 or 4")
        del df
        df = pd.read_csv(csv_dir, header=None, names=colnames)
    # conn = connect(connstr)
    df['vulnID_fk_id'] = vulnID
    # Inserting each row
    engine = create_engine(connstr)
    df.to_sql(vuln_point_table, engine, if_exists='append', index=False)
    engine.dispose()
    return('sucessfully loaded')
