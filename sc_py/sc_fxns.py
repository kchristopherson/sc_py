import sys
current_module = sys.modules[__name__]


def convert_id(row):
    """
    Tries to convert data to a float

    Parameters
    ---------
    row : float

    Returns
    -------
    new_id : int
    """
    try:
        new_id = int(float(row))
    except:
        new_id = row
    return new_id


def batch_delete(list_to_delete, table_name, delete_column_name):
    """
    Deletes a list of records from a given database table, given a column name
        Validates the data to add proper quotations based on the first item in the list
        Also checks to see how many records are being deleted and will batch delete if necessary

    Parameters
    ---------
    list_to_delete : list
        a list of items, each denoting one row, that will be deleted
        each item in the list should be one unique row
    table_name: string
        the name of the table to delete from
    delete_column_name: string
        the name of the column in <table_name> to delete the items from <list_to_delete>

    Returns
    -------

    """
    from sqlalchemy.engine import URL
    from sqlalchemy import create_engine
    from math import ceil
    from pyodbc import connect
    import pandas as pd
    connection_string = 'Driver={SQL Server};Server=scdb1.silvercreeksv.com;Database=scfundrisk;Trusted_Connection=yes;'
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})

    engine = create_engine(connection_url)
    conn = connect(connection_string)
    cursor = conn.cursor()

    if type(list_to_delete) is not list:
        raise ValueError("""'list_to_delete' must be of type list """)
    # get initial record count
    beg_no_records_df = pd.read_sql_query("""select count("""+delete_column_name+""") as ct from """+table_name, engine)
    no_records = beg_no_records_df.loc[0, 'ct']
    if len(list_to_delete) > 0:
        # the database can only delete 2090 records in one go
        # if number of records>2090, we have to beak it up
        if len(list_to_delete) > 2090:
            print('need to batch delete to accomodate database limits')
            num_iterations = ceil(len(list_to_delete)/2090)
            for i in range(num_iterations):
                print('deleting rows '+str(i*2090)+' to '+str(min(2090*(i+1), len(list_to_delete))))
                sub_list_to_delete = list_to_delete[2090*(i):2090*(i+1)]
                if type(sub_list_to_delete[0]) == str:
                    # we only check the first element because sql ensures constant datatypes
                    sub_list_to_delete = "','".join((map(str, sub_list_to_delete)))
                    sub_list_to_delete = "'"+sub_list_to_delete+"'"
                else:
                    sub_list_to_delete = ','.join((map(str, sub_list_to_delete)))
                sub_list_to_delete = '('+sub_list_to_delete+')'
                cursor.execute(''' DELETE FROM '''+table_name+''' where '''+delete_column_name+''' in '''+sub_list_to_delete)
                conn.commit()

        elif len(list_to_delete) > 0:
            if type(list_to_delete[0]) == str:
                # we only check the first element because sql ensures constant datatypes
                temp_list_to_delete = "','".join((map(str, list_to_delete)))
                temp_list_to_delete = "'"+temp_list_to_delete+"'"
            else:
                temp_list_to_delete = ','.join((map(str, list_to_delete)))
            temp_list_to_delete = '('+temp_list_to_delete+')'
            cursor.execute(''' DELETE FROM '''+table_name+''' where '''+delete_column_name+''' in '''+temp_list_to_delete)
            conn.commit()
        # get updated record count
        end_no_records_df = pd.read_sql_query("""select count("""+delete_column_name+""") as ct from """+table_name, engine)
        end_no_records = end_no_records_df.loc[0, 'ct']
        print('deleted '+str(no_records-end_no_records)+' number of records from table: '+table_name+', based on column: ' + delete_column_name)
    else:
        print('no records to delete from: '+table_name)


def adj_dataframe(df):
    """
    Ensures that a dataframes columns are consistent for merging and for sql datatypes

    Parameters
    ---------
    df : dataframe
        a dataframe whose columns you want to adjust

    Returns
    df : dataframe
        a dataframe with adjusted columns
    -------

    """
    def convert_id(row):
        try:
            new_id = int(float(row))
        except:
            new_id = row
        return new_id

    import numpy as np
    import pandas as pd
    df = df.replace('NaN', np.nan)
    df = df.fillna(value=np.nan)
    for col in df.columns:
        if col in ['asof_date', 'Date', 'date', 'dates']:
            df[col] = pd.to_datetime(df[col])
        if col in ['id', 'external_id', 'fundId', 'Fund_ID', 'Index_ID', 'external_strategy_id', 'strategy_code', 'strategy_fund_id', 'Fund ID', 'Firm_ID', 'Firm ID', 'ret_ts_id', 'aum_ts_id', 'id_record_number', 'risk_ts_id']:
            try:
                df[col] = df[col].apply(int)

            except:  # if there are np.nan's in the column, apply(int) fails - this is a workaround
                df[col] = df[col].astype('object')

            for index, row in df.iterrows():
                df.loc[index, col] = convert_id(df.loc[index, col])
    return df


def get_assets(source, aum_df, better_sources):
    """
    Runs the process to update AUMs given database logic
    Parameters
    ---------
    source : str
        source in question
    aum_df: dataframe
        dataframe of AUM values with corresponding asof_dates and internal IDs
        the asset_values here will all be in USD
    better_sources : list
        list where each element is a better source (one you would not want to overwrite) from aum_df

    Returns
    -------

    """
    
    import pandas as pd
    import numpy as np
    from sqlalchemy.engine import URL
    from sqlalchemy import create_engine
    connection_string = 'Driver={SQL Server};Server=scdb1.silvercreeksv.com;Database=scfundrisk;Trusted_Connection=yes;'
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    
    if type(better_sources) is not list:
        raise ValueError("""'better_sources' must be of type list """)

    if 'id' not in aum_df.columns.to_list():
        raise ValueError("""id must be a column in aum_df """)
    if 'asset_value' not in aum_df.columns.to_list():
        raise ValueError("""asset_value must be a column in aum_df """)
    if 'asof_date' not in aum_df.columns.to_list():
        raise ValueError("""asof_date must be a column in aum_df """)
    if 'source' not in aum_df.columns.to_list():
        raise ValueError("""source must be a column in aum_df """)

    quote_char = "'"
    ids = pd.read_sql_query("""
    select e.id,e.external_id 
    from external_entity_mapping e join (
        --query funds that do not have an AUM source
        select id from funds where id not in (select id from aum_ts)
        UNION
        --query funds that only use given source for AUMs
        select id from aum_ts where source="""+quote_char+source+quote_char+"""
    ) i on i.id=e.id 
    join funds f on f.id=e.id
    where e.mapping_status='Live'
    and e.is_shareclass=0
    and e.external_source="""+quote_char+source+quote_char+"""
    and f.blend_aums=0
    and f.id not in (
        select id from aum_ts where source!="""+quote_char+source+quote_char+""")
                          """, engine)
    ids = adj_dataframe(ids)
    # check to only update funds with existing AUMs from given source or missing AUMs
    assets_id = aum_df[aum_df['id'].isin(ids['id'].to_list())]
    assets_id = assets_id.reset_index(drop=True)

    db = pd.read_sql_query('SELECT * FROM aum_ts ', engine)
    db = adj_dataframe(db)
    db = rename_with_additional_string(db, 'existing')

    # check which internal IDs are in the aum_df but not in the list of funds whose aums we should be updating
    # these are funds with other sources in the database
    # strip out sources that are higher in our hierarchy
    # filter out funds with blended aums
    # then delete the aums of funds with non-blended aums
    other_sources = list(aum_df[~aum_df['id'].isin(ids['id'])]['id'].unique())
    fund_check = pd.read_sql_query("""select * from funds""", engine)
    fund_check = fund_check[fund_check['id'].isin(other_sources)]
    funds_to_delete = fund_check[fund_check['blend_aums'] == 0]
    worse_aums = funds_to_delete.merge(db, how='left', left_on=['id'], right_on=['id existing'])
    worse_aums = worse_aums[~worse_aums['source existing'].isin(better_sources)]

    worse_aums = worse_aums[['id existing', 'asof_date existing', 'asset_value existing', 'source existing', 'aum_ts_id existing']]
    worse_aums.to_csv(source+"_aum_backup.csv")

    # current process is to move old aums out of the primary aums database (aumss_ts) and into old_aum_ts
    # we do this as a backup in case any funds aums need to be restored
    # this only has to be done on the breaks df because that represents the aums we're about to delete
    old_upload = worse_aums[['id existing',
                             'asof_date existing',
                             'asset_value existing',
                             'source existing']].rename(columns={'id existing': 'id',
                                                                 'asof_date existing': 'asof_date',
                                                                 'asset_value existing': 'asset_value',
                                                                 'source existing': 'source'})
    if len(old_upload['id']) > 0:
        db_old = pd.read_sql_query('SELECT * FROM old_aum_ts', engine)
        db_old = adj_dataframe(db_old)
        db_old = rename_with_additional_string(db_old, 'existing')
        # merge on source here to ensure we are retaining records from various sources in case we need to fallback
        old_upload = old_upload.merge(db_old,
                                      how='left',
                                      left_on=['id', 'asof_date', 'source'],
                                      right_on=['id existing', 'asof_date existing', 'source existing'])
        # get new records to insert to old_aum_ts
        old_new_records = old_upload[old_upload['aum_ts_id existing'].isnull()]
        # get records to delete from old_aum_ts
        old_delete_records = old_upload[~old_upload['aum_ts_id existing'].isnull()]
        # delete the old records
        old_to_delete = old_delete_records['aum_ts_id existing'].to_list()
        batch_delete(old_to_delete, 'old_aum_ts', 'aum_ts_id')
        old_upload_combo = pd.concat([old_new_records, old_delete_records])
        old_upload_combo = old_upload_combo[['id', 'asof_date', 'asset_value', 'source']]
        old_upload_combo.to_sql('old_aum_ts', engine, if_exists='append', index=False)  # index=False prevents failure on trying to insert the index column
        print(str(len(old_new_records['id']))+' new rows inserted to old_aum_ts')
        print(str(len(old_delete_records['id']))+' rows deleted and updated to old_aum_ts')
    else:
        print('no records to move to old_aum_ts')

    # delete the old records
    to_delete = worse_aums['aum_ts_id existing'].to_list()
    batch_delete(to_delete, 'aum_ts', 'aum_ts_id')

    print(str(len(worse_aums['aum_ts_id existing']))+' rows of inferior aum sources deleted from aum_ts')

    ids = pd.read_sql_query("""
    select e.id,e.external_id 
    from external_entity_mapping e join (
        --query funds that do not have an AUM source
        select id from funds where id not in (select id from aum_ts)
        UNION
        --query funds that only use given source for AUMs
        select id from aum_ts where source="""+quote_char+source+quote_char+"""
    ) i on i.id=e.id 
    join funds f on f.id=e.id
    where e.mapping_status='Live'
    and e.is_shareclass=0
    and e.external_source="""+quote_char+source+quote_char+"""
    and f.blend_aums=0
    and f.id not in (
        select id from aum_ts where source!="""+quote_char+source+quote_char+""")
    """, engine)
    ids = adj_dataframe(ids)
    # check to only update funds with existing source's AUMs or missing AUMs
    assets_id = aum_df[aum_df['id'].isin(ids['id'].to_list())]
    assets_id = assets_id.reset_index(drop=True)

    db = pd.read_sql_query('SELECT * FROM aum_ts ', engine)
    db = adj_dataframe(db)
    db = rename_with_additional_string(db, 'existing')

    merge_df = assets_id.merge(db, how='left',
                               left_on=['id', 'asof_date'],
                               right_on=['id existing', 'asof_date existing'])

    merge_df = merge_df[~merge_df['source existing'].isin(better_sources)]
    new = merge_df[merge_df['aum_ts_id existing'].isnull()]
    breaks = merge_df[~merge_df['aum_ts_id existing'].isnull()]
    breaks = breaks[breaks['asset_value'] != breaks['asset_value existing']]

    # create dataframe to move old 'break' records to old_aum_ts
    old_upload = breaks[['id existing',
                         'asof_date existing',
                        'asset_value existing',
                         'source existing']].rename(columns={'id existing': 'id',
                                                             'asof_date existing': 'asof_date',
                                                             'asset_value existing': 'asset_value',
                                                             'source existing': 'source'})
    if len(old_upload['id']) > 0:
        db_old = pd.read_sql_query('SELECT * FROM old_aum_ts', engine)
        db_old = adj_dataframe(db_old)
        db_old = rename_with_additional_string(db_old, 'existing')

        old_upload = old_upload.merge(db_old,
                                      left_on=['id', 'asof_date', 'source'],
                                      right_on=['id existing', 'asof_date existing', 'source existing'])
        # get new records to insert to old_aum_ts
        old_new_records = old_upload[old_upload['aum_ts_id existing'].isnull()]
        # get records to delete from old_aum_ts
        old_delete_records = old_upload[~old_upload['aum_ts_id existing'].isnull()]

        # delete the old records
        old_to_delete = old_delete_records['aum_ts_id existing'].to_list()
        batch_delete(old_to_delete, 'old_aum_ts', 'aum_ts_id')

        old_upload = pd.concat([old_upload, old_delete_records])
        old_upload = old_upload[['id', 'asof_date', 'asset_value', 'source']]
        old_upload.to_sql('old_aum_ts', engine, if_exists='append', index=False)  # index=False prevents failure on trying to insert the index column
        print(str(len(old_upload['id']))+' new rows inserted to old_aum_ts')
        print(str(len(old_delete_records['id']))+' rows deleted and updated to old_aum_ts')
    else:
        print('no records to move to old_aum_ts')

    to_delete = breaks['aum_ts_id existing'].to_list()
    # delete the old records
    batch_delete(to_delete, 'aum_ts', 'aum_ts_id')

    upload = pd.concat([new, breaks])
    upload.loc[:, 'source'] = source

    upload = upload[['id', 'asof_date', 'asset_value', 'source']]
    upload.to_sql('aum_ts', engine, if_exists='append', index=False)  # index=False prevents failure on trying to insert the index column
    print(str(len(upload['id']))+' rows deleted and updated')

    # query funds with blended AUMs allowed
    blend_ids = pd.read_sql_query("""
    select e.id,e.external_id 
    from external_entity_mapping e 
    join funds f on f.id=e.id
    where e.mapping_status='Live'
    and e.is_shareclass=0
    and e.external_source="""+quote_char+source+quote_char+"""
    and f.blend_aums=1""", engine)
    blend_ids = adj_dataframe(blend_ids)
    blend_aums = aum_df[aum_df['id'].isin(blend_ids['id'].to_list())]
    blend_aums = blend_aums.reset_index(drop=True)

    db_blend = pd.read_sql_query('SELECT * FROM aum_ts', engine)
    db_blend = adj_dataframe(db_blend)
    db_blend = rename_with_additional_string(db_blend, 'existing')

    merge_blend_df = blend_aums.merge(db_blend,
                                      how='left',
                                      left_on=['id', 'asof_date'],
                                      right_on=['id existing', 'asof_date existing'])
    # filter our better sources
    merge_blend_df = merge_blend_df[~merge_blend_df['source existing'].isin(better_sources)]

    blend_new = merge_blend_df[merge_blend_df['aum_ts_id existing'].isnull()]
    blend_breaks = merge_blend_df[~merge_blend_df['aum_ts_id existing'].isnull()]
    blend_breaks = blend_breaks[blend_breaks['asset_value'] != blend_breaks['asset_value existing']]
    # we dont want to overwrite given source's aums
    blend_breaks = blend_breaks[blend_breaks['source existing'] != source]

    old_blend_upload = blend_breaks[['id existing',
                                     'asof_date existing',
                                    'asset_value existing',
                                     'source existing']].rename(columns={'id existing': 'id',
                                                                         'asof_date existing': 'asof_date',
                                                                         'asset_value existing': 'asset_value',
                                                                         'source existing': 'source'})

    if len(old_blend_upload['id']) > 0:
        db_blend_old = pd.read_sql_query('SELECT * FROM old_aum_ts', engine)
        db_blend_old = adj_dataframe(db_blend_old)
        db_blend_old = rename_with_additional_string(db_blend_old, 'existing')

        old_blend_upload = old_upload.merge(db_blend_old,
                                            left_on=['id', 'asof_date', 'source'],
                                            right_on=['id existing', 'asof_date existing', 'source existing'])
        # get new records to insert to old_aum_ts
        old_blend_new_records = old_blend_upload[old_blend_upload['aum_ts_id existing'].isnull()]
        # get records to delete from old_aum_ts
        old_blend_delete_records = old_blend_upload[~old_blend_upload['aum_ts_id existing'].isnull()]
        # delete the old records
        old_blend_to_delete = old_blend_delete_records['aum_ts_id existing'].to_list()
        # delete the old records
        batch_delete(old_blend_to_delete, 'old_aum_ts', 'aum_ts_id')

        old_blend_final_upload = pd.concat([old_blend_new_records, old_blend_delete_records])
        old_blend_final_upload = old_blend_final_upload[['id', 'asof_date', 'asset_value', 'source']]
        old_blend_final_upload.to_sql('old_aum_ts', engine, if_exists='append', index=False)  # index=False prevents failure on trying to insert the index column
        print(str(len(old_blend_new_records['id']))+' new rows inserted to old_aum_ts')
        print(str(len(old_blend_delete_records['id']))+' rows deleted and updated to old_aum_ts')
    else:
        print('no records to move to old_aum_ts')

    to_delete = blend_breaks['aum_ts_id existing'].to_list()
    #batch_delete(to_delete, 'aum_ts', 'aum_ts_id')
    blend_upload = pd.concat([blend_new, blend_breaks])
    if len(blend_upload['id']) > 0:
        blend_upload.loc[:, 'source'] = source
        blend_upload = blend_upload[['id', 'asof_date', 'asset_value', 'source']]
        blend_upload.to_sql('aum_ts', engine, if_exists='append', index=False)  # index=False prevents failure on trying to insert the index column
        print(str(len(blend_new['id']))+' new rows inserted to aum_ts')
        print(str(len(blend_breaks['id']))+' rows deleted and updated to aum_ts')
    else:
        print('no records to update with blended method')


def get_returns(source, returns_df, better_sources):
    """
    Runs the process to update returns given database logic
    Parameters
    ---------
    source : str
        source in question
    returns_df: dataframe
        dataframe of return values with corresponding asof_dates and internal IDs
        the return_values here will all be in USD
    better_sources : list
        list where each element is a better source (one you would not want to overwrite) from aum_df

    Returns
    -------

    """
    import pandas as pd
    import numpy as np
    from sqlalchemy.engine import URL
    from sqlalchemy import create_engine
    connection_string = 'Driver={SQL Server};Server=scdb1.silvercreeksv.com;Database=scfundrisk;Trusted_Connection=yes;'
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    
    engine = create_engine(connection_url)    
    if type(better_sources) is not list:
        raise ValueError("""'better_sources' must be of type list """)

    if 'id' not in returns_df.columns.to_list():
        raise ValueError("""id must be a column in returns_df """)
    if 'return_value' not in returns_df.columns.to_list():
        raise ValueError("""asset_value must be a column in returns_df """)
    if 'asof_date' not in returns_df.columns.to_list():
        raise ValueError("""asof_date must be a column in returns_df """)
    if 'source' not in returns_df.columns.to_list():
        raise ValueError("""source must be a column in returns_df """)

    quote_char = "'"
    sql_source = quote_char+source+quote_char
    # get list of funds that are missing returns or are currently using given source for returns
    ids = pd.read_sql_query("""
    select distinct e.id,e.external_id 
    from external_entity_mapping e join (
        --query funds that do not have a returns source
        select id from funds where id not in (select id from returns_ts)
        UNION
        --query funds that only use given source for returns
        select id from returns_ts where source="""+sql_source+"""
    ) i on i.id=e.id 
    join funds f on f.id=e.id
    where e.mapping_status='Live'
    and e.is_shareclass=0
    and e.external_source="""+sql_source+"""
    and f.blend_returns=0
    and e.id not in (select id from returns_ts where source!="""+sql_source+""")
    """, engine)
    ids = adj_dataframe(ids)

    db = pd.read_sql_query('SELECT * FROM returns_ts', engine)
    db = adj_dataframe(db)
    db = rename_with_additional_string(db, 'existing')

    print('starting process to remove inferior return sources')
    # check which internal IDs are in the returns_df but not in the list of funds whose returns we should be updating
    # these are funds with other sources in the database
    # strip out sources that are higher in our hierarchy
    # filter out funds with blended returns
    # then delete the returns of funds with non-blended returns
    other_sources = list(returns_df[~returns_df['id'].isin(ids['id'])]['id'].unique())
    fund_check = pd.read_sql_query("""select * from funds""", engine)
    fund_check = fund_check[fund_check['id'].isin(other_sources)]
    funds_to_delete = fund_check[fund_check['blend_returns'] == 0]
    worse_returns = funds_to_delete.merge(db, how='left', left_on=['id'], right_on=['id existing'])
    # strip out any sources we dont want to overwrite
    worse_returns = worse_returns[~worse_returns['source existing'].isin(better_sources)]
    worse_returns = worse_returns[['id existing', 'asof_date existing', 'return_value existing', 'source existing', 'ret_ts_id existing']]
    worse_returns.to_csv(source+"_backup.csv")

    # current process is to move old returns out of the primary returns database (returns_ts) and into old_returns_ts
    # we do this as a backup in case any funds returns need to be restored
    # this only has to be done on the breaks df because that represents the returns we're about to delete
    old_upload = worse_returns[['id existing',
                                'asof_date existing',
                                'return_value existing',
                                'source existing']].rename(columns={'id existing': 'id',
                                                                    'asof_date existing': 'asof_date',
                                                                    'return_value existing': 'return_value',
                                                                    'source existing': 'source'})

    if len(old_upload['id']) > 0:
        db_old = pd.read_sql_query('SELECT * FROM old_returns_ts', engine)
        db_old = adj_dataframe(db_old)
        db_old = rename_with_additional_string(db_old, 'existing')
        # merge on source here to ensure we are retaining records from various sources in case we need to fallback
        old_upload = old_upload.merge(db_old,
                                      how='left',
                                      left_on=['id', 'asof_date', 'source'],
                                      right_on=['id existing', 'asof_date existing', 'source existing'])
        # get new records to insert to old_returns_ts
        old_new_records = old_upload[old_upload['ret_ts_id existing'].isnull()]
        # get records to delete from old_returns_ts
        old_delete_records = old_upload[~old_upload['ret_ts_id existing'].isnull()]
        # delete the old records
        old_to_delete = old_delete_records['ret_ts_id existing'].to_list()
        # delete the old records
        batch_delete(old_to_delete, 'old_returns_ts', 'ret_ts_id')
        old_upload_combo = pd.concat([old_new_records, old_delete_records])
        old_upload_combo = old_upload_combo[['id', 'asof_date', 'return_value', 'source']]
        old_upload_combo.to_sql('old_returns_ts', engine, if_exists='append', index=False)  # index=False prevents failure on trying to insert the index column
        print('   '+str(len(old_new_records['id']))+' new rows inserted to old_returns_ts')
        print('   '+str(len(old_delete_records['id']))+' rows deleted and updated to old_returns_ts')
    else:
        print('   no records to move to old_returns_ts')

    # delete the old records
    to_delete = worse_returns['ret_ts_id existing'].to_list()
    batch_delete(to_delete, 'returns_ts', 'ret_ts_id')

    print('   '+str(len(worse_returns['ret_ts_id existing']))+' inferior rows of return sources deleted from returns_ts')

    print("""finished process to remove inferior return sources

    """)
    print("""starting process to update non-blended returns""")
    # re-query ids and returns to get an updated list of funds to insert and returns to check against
    # then merge to make sure we're inserting new rows from returns_ts
    ids = pd.read_sql_query("""
    select distinct e.id,e.external_id 
    from external_entity_mapping e join (
        --query funds that do not have a returns source
        select id from funds where id not in (select id from returns_ts)
        UNION
        --query funds that only use given source for returns
        select id from returns_ts where source="""+sql_source+"""
    ) i on i.id=e.id 
    join funds f on f.id=e.id
    where e.mapping_status='Live'
    and e.is_shareclass=0
    and e.external_source="""+sql_source+"""
    and f.blend_returns=0
    and e.id not in (select id from returns_ts where source!="""+sql_source+""")
    """, engine)
    ids = adj_dataframe(ids)
    rets_ids = returns_df[returns_df['id'].isin(ids['id'].to_list())]
    rets_ids = rets_ids.reset_index(drop=True)

    db = pd.read_sql_query('SELECT * FROM returns_ts', engine)
    db = adj_dataframe(db)
    db = rename_with_additional_string(db, 'existing')

    # check which funds have new returns or return differences
    # we query the whole database of returns, then left join
    # this will return only the funds that we are interested in replacing (missing returns or currently using given source's returns)
    # querying the whole returns_ts database allows us to check existing return sources that might NOT be given source
    merge_df = rets_ids.merge(db,
                              how='left',
                              left_on=['id', 'asof_date'],
                              right_on=['id existing', 'asof_date existing'])
    merge_df = adj_dataframe(merge_df)
    merge_df = merge_df[~merge_df['source existing'].isin(better_sources)]
    new = merge_df[merge_df['ret_ts_id existing'].isnull()]
    breaks = merge_df[~merge_df['ret_ts_id existing'].isnull()]
    breaks = breaks[breaks['return_value'] != breaks['return_value existing']]

    # current process is to move old returns out of the primary returns database (returns_ts) and into old_returns_ts
    # we do this as a backup in case any funds returns need to be restored
    # this only has to be done on the breaks df because that represents the returns we're about to delete
    old_upload = breaks[['id existing',
                         'asof_date existing',
                         'return_value existing',
                         'source existing']].rename(columns={'id existing': 'id',
                                                             'asof_date existing': 'asof_date',
                                                             'return_value existing': 'return_value',
                                                             'source existing': 'source'})

    if len(old_upload['id']) > 0:
        db_old = pd.read_sql_query('SELECT * FROM old_returns_ts', engine)
        db_old = adj_dataframe(db_old)
        db_old = rename_with_additional_string(db_old, 'existing')
        # merge on source here to ensure we are retaining records from various sources in case we need to fallback
        old_upload = old_upload.merge(db_old,
                                      how='left',
                                      left_on=['id', 'asof_date', 'source'],
                                      right_on=['id existing', 'asof_date existing', 'source existing'])
        # get new records to insert to old_returns_ts
        old_new_records = old_upload[old_upload['ret_ts_id existing'].isnull()]
        # get records to delete from old_returns_ts
        old_delete_records = old_upload[~old_upload['ret_ts_id existing'].isnull()]
        # delete the old records
        old_to_delete = old_delete_records['ret_ts_id existing'].to_list()
        # delete the old records
        batch_delete(old_to_delete, 'old_returns_ts', 'ret_ts_id')
        old_upload_combo = pd.concat([old_new_records, old_delete_records])
        old_upload_combo = old_upload_combo[['id', 'asof_date', 'return_value', 'source']]
        old_upload_combo.to_sql('old_returns_ts', engine, if_exists='append', index=False)  # index=False prevents failure on trying to insert the index column
        print('   '+str(len(old_new_records['id']))+' new rows inserted to old_returns_ts')
        print('   '+str(len(old_delete_records['id']))+' rows deleted and updated to old_returns_ts')
    else:
        print('   no records to move to old_returns_ts')

    breaks.to_csv(source+'_backup2.csv')
    # delete the old records
    to_delete = breaks['ret_ts_id existing'].to_list()
    batch_delete(to_delete, 'returns_ts', 'ret_ts_id')

    upload = pd.concat([new, breaks])
    upload = upload.reset_index(drop=True)
    upload.loc[:, 'source'] = source
    upload = upload[['id', 'asof_date', 'return_value', 'source']]
    upload.to_sql('returns_ts', engine, if_exists='append', index=False)  # index=False prevents failure on trying to insert the index column
    print('   '+str(len(new['id']))+' new rows inserted to returns_ts')
    print('   '+str(len(breaks['id']))+' rows deleted and updated to returns_ts')

    print("""finished process to update non-blended returns

    """)
    print("""starting process to update blended returns""")
    # get list of funds that we want to blend returns on
    blend_ids = pd.read_sql_query("""
    select distinct e.id,e.external_id 
    from external_entity_mapping e 
    join funds f on f.id=e.id
    where e.mapping_status='Live'
    and e.is_shareclass=0
    and e.external_source="""+sql_source+"""
    and f.blend_returns=1""", engine)
    blend_ids = adj_dataframe(blend_ids)

    # instead of inner mergeing here, we can just use isin to filter only on funds with blended returns
    blend_rets = returns_df[returns_df['id'].isin(blend_ids['id'].to_list())]
    blend_rets = blend_rets.reset_index(drop=True)

    db_blend = pd.read_sql_query('SELECT * FROM returns_ts', engine)
    db_blend = adj_dataframe(db_blend)
    db_blend = rename_with_additional_string(db_blend, 'existing')

    merge_blend_df = blend_rets.merge(db_blend,
                                      how='left',
                                      left_on=['id', 'asof_date'],
                                      right_on=['id existing', 'asof_date existing'])
    # Since given source is the top source in returns hierarchy, we dont have to strip out any returns sources here
    # in other words, delete any other source of return
    merge_blend_df = merge_blend_df[~merge_blend_df['source existing'].isin(better_sources)]
    blend_new = merge_blend_df[merge_blend_df['ret_ts_id existing'].isnull()]
    blend_breaks = merge_blend_df[~merge_blend_df['ret_ts_id existing'].isnull()]
    blend_breaks = blend_breaks[blend_breaks['return_value'] != blend_breaks['return_value existing']]

    # current process is to move old returns out of the primary returns database (returns_ts) and into ld_returns_ts
    # we do this as a backup in case any funds returns need to be restored
    # this only has to be done on the breaks df because that represents the returns we're about to delete
    old_blend_upload = blend_breaks[['id existing',
                                     'asof_date existing',
                                     'return_value existing',
                                     'source existing']].rename(columns={'id existing': 'id',
                                                                         'asof_date existing': 'asof_date',
                                                                         'return_value existing': 'return_value',
                                                                         'source existing': 'source'})

    if len(old_blend_upload['id']) > 0:
        db_old_blend = pd.read_sql_query('SELECT * FROM old_returns_ts', engine)
        db_old_blend = adj_dataframe(db_old_blend)
        db_old_blend = rename_with_additional_string(db_old_blend, 'existing')
        # merge on source to keep all previous versions of each source
        old_blend_upload = old_blend_upload.merge(db_old_blend,
                                                  left_on=['id', 'asof_date', 'source'],
                                                  right_on=['id existing', 'asof_date existing', 'source existing'])
        # get new records to insert to old_returns_ts
        old_blend_new_records = old_blend_upload[old_blend_upload['ret_ts_id existing'].isnull()]
        # get records to delete from old_returns_ts
        old_blend_delete_records = old_blend_upload[~old_blend_upload['ret_ts_id existing'].isnull()]
        # delete the old records
        old_blend_to_delete = old_blend_delete_records['ret_ts_id existing'].to_list()
        # delete the old records
        batch_delete(old_blend_to_delete, 'old_returns_ts', 'ret_ts_id')

        old_blend_upload_combo = pd.concat([old_blend_new_records, old_blend_delete_records])
        old_blend_upload_combo = old_blend_upload_combo[['id', 'asof_date', 'return_value', 'source']]
        old_blend_upload_combo.to_sql('old_returns_ts', engine, if_exists='append', index=False)  # index=False prevents failure on trying to insert the index column
        print('   '+str(len(old_blend_new_records['id']))+' new rows inserted to old_returns_ts')
        print('   '+str(len(old_blend_delete_records['id']))+' rows deleted and updated to old_returns_ts')
    else:
        print('   no records to move to old_returns_ts')

    blend_to_delete = blend_breaks['ret_ts_id existing'].to_list()

    # delete the old records
    batch_delete(blend_to_delete, 'returns_ts', 'ret_ts_id')

    blend_upload = pd.concat([blend_new, blend_breaks])
    blend_upload = blend_upload.reset_index(drop=True)
    if len(blend_upload['id']) > 0:
        blend_upload.loc[:, 'source'] = source
        blend_upload = blend_upload[['id', 'asof_date', 'return_value', 'source']]
        blend_upload.to_sql('returns_ts', engine, if_exists='append', index=False)  # index=False prevents failure on trying to insert the index column
        print('   '+str(len(blend_new['id']))+' new rows inserted to returns_ts')
        print('   '+str(len(blend_breaks['id']))+' rows deleted and updated to returns_ts')
    else:
        print('   no records to update with blended method')
    print("""finished process to update blended returns

    """)


def get_fees(source, fees_df, better_sources):
    """
    evaluates a dataframe to see which records insides the dataframe should be inserted to the fees table.
    The current process checks to ensure that we are not overwriting any better sources of data,
    and then checks for any discrepancies vs. the same or worse sources. 

    Parameters
    ---------
    source : str
        the name of the source of the data
        used to populate the 'source' column in fees table
    fees_df: dataframe
        the dataframe from 'source' that we want to evalute for insertion to the database
    better_sources: list
        list containing strings of sources that we would NOT want to overwite
        for example, if we are evaluating HFR's fees, we would NOT want to overwite
        any manually-specified fees or any fees from albourne. therefore better_sources = 
        ['albourne','manual']

    Returns
    -------

    """
    import pandas as pd
    import numpy as np
    from sqlalchemy.engine import URL
    from sqlalchemy import create_engine
    connection_string = 'Driver={SQL Server};Server=scdb1.silvercreeksv.com;Database=scfundrisk;Trusted_Connection=yes;'
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    
    if type(better_sources) is not list:
        raise ValueError("""'better_sources' must be of type list """)
    if 'id' not in fees_df.columns.to_list():
        raise ValueError("""id must be a column in fees_df """)
    if 'management_fee' not in fees_df.columns.to_list():
        raise ValueError("""management_fee must be a column in fees_df """)
    if 'performance_fee' not in fees_df.columns.to_list():
        raise ValueError("""performance_fee must be a column in fees_df """)
    if 'source' not in fees_df.columns.to_list():
        raise ValueError("""source must be a column in fees_df """)

    fees_df = fees_df.dropna(subset=['management_fee', 'performance_fee'], how='all').copy()

    fees_df.loc[:, 'management_fee'] = fees_df['management_fee'].round(decimals=8)
    fees_df.loc[:, 'performance_fee'] = fees_df['performance_fee'].round(decimals=8)

    quote_char = "'"
    ids = pd.read_sql_query("""
    select e.id,e.external_id 
    from external_entity_mapping e join (
        --query funds that do not have an fee source
        select id from funds where id not in (select id from fees)
        UNION
        --query funds that only use given source for fees
        select id from fees where source="""+quote_char+source+quote_char+"""
    ) i on i.id=e.id 
    join funds f on f.id=e.id
    where e.mapping_status='Live'
    and e.is_shareclass=0
    and e.external_source="""+quote_char+source+quote_char+"""
    and f.id not in (
        select id from fees where source!="""+quote_char+source+quote_char+""")
                          """, engine)
    ids = adj_dataframe(ids)
    # check to only update funds with existing fees from given source or missing fees
    assets_id = fees_df[fees_df['id'].isin(ids['id'].to_list())]
    assets_id = assets_id.reset_index(drop=True)

    db = pd.read_sql_query('SELECT * FROM fees ', engine)
    db = adj_dataframe(db)
    db = rename_with_additional_string(db, 'existing')

    # check which internal IDs are in the fees_df but not in the list of funds whose fees we should be updating
    # these are funds with other sources in the database
    # strip out sources that are higher in our hierarchy
    other_sources = list(fees_df[~fees_df['id'].isin(ids['id'])]['id'].unique())
    fund_check = pd.read_sql_query("""select * from funds""", engine)
    fund_check = fund_check[fund_check['id'].isin(other_sources)]
    funds_to_delete = fund_check.merge(db, how='left', left_on=['id'], right_on=['id existing'])
    worse_fee_sources = funds_to_delete[~funds_to_delete['source existing'].isin(better_sources)]

    worse_fee_sources = worse_fee_sources[['id existing',
                                           'management_fee existing',
                                           'performance_fee existing',
                                           'source existing',
                                           'id_record_number existing',
                                           'hurdle_rate existing',
                                           'high_water_mark existing']]
    worse_fee_sources.to_csv(source+"_fee_backup.csv")

    # delete the old records
    to_delete = worse_fee_sources['id_record_number existing'].to_list()
    print('deleting '+str(len(to_delete))+' based on worse sources')
    batch_delete(to_delete, 'fees', 'id_record_number')

    ids = pd.read_sql_query("""
    select e.id,e.external_id 
    from external_entity_mapping e join (
        --query funds that do not have an fee source
        select id from funds where id not in (select id from fees)
        UNION
        --query funds that only use given source for fees
        select id from fees where source="""+quote_char+source+quote_char+"""
    ) i on i.id=e.id 
    join funds f on f.id=e.id
    where e.mapping_status='Live'
    and e.is_shareclass=0
    and e.external_source="""+quote_char+source+quote_char+"""
    and f.id not in (
        select id from fees where source!="""+quote_char+source+quote_char+""")
    """, engine)
    ids = adj_dataframe(ids)
    # check to only update funds with existing source's fees or missing fees
    assets_id = fees_df[fees_df['id'].isin(ids['id'].to_list())]
    assets_id = assets_id.reset_index(drop=True)

    db = pd.read_sql_query('SELECT * FROM fees ', engine)
    db = adj_dataframe(db)
    # round to match existing df
    db['management_fee'] = db['management_fee'].round(decimals=8)
    db['performance_fee'] = db['performance_fee'].round(decimals=8)

    db = rename_with_additional_string(db, 'existing')

    merge_df = assets_id.merge(db, how='left',
                               left_on='id',
                               right_on='id existing')
    # get only records with sources we would want to overwrite
    merge_df = merge_df[~merge_df['source existing'].isin(better_sources)]
    # get all new records (which we would insert no matter what)
    new = merge_df[merge_df['id_record_number existing'].isnull()]
    breaks = merge_df[~merge_df['id_record_number existing'].isnull()].copy()

    def updated_records(df, colname1, colname2):
        """
        Evalautes a dataframe across colname1 and colname2 to ensure that:
            1) both are not null
            2) the values in colname1 and colname2 differ
        then the function returns the dataframe.
        This is to ensure that we are not deleting and re-inserting records with nulls,
        or where the data is unchanged.

        Parameters
        ---------
        df : dataframe
            the dataframe to evaluate
        colname1: string
            the name of the first column inside df to evaluate
        colname2: string
            the name of the second column inside df to evaluate

        Returns
        -------
        df : dataframe
            the original dataframe
        """
        df = df[~df[colname1].isnull() | ~df[colname2].isnull()]
        df = df[df[colname1] != df[colname2]]
        df.reset_index(drop=True, inplace=True)
        return df

    perf_check = updated_records(breaks, 'performance_fee', 'performance_fee existing')
    mgmt_check = updated_records(breaks, 'management_fee', 'management_fee existing')
    hwm_check = updated_records(breaks, 'high_water_mark', 'high_water_mark existing')
    hurdle_check = updated_records(breaks, 'hurdle_rate', 'hurdle_rate existing')
    final_breaks = pd.concat([perf_check, mgmt_check, hwm_check, hurdle_check])
    final_breaks = final_breaks.drop_duplicates(keep='first')
    final_breaks = final_breaks.reset_index(drop=True)

    to_delete = final_breaks['id_record_number existing'].to_list()
    # delete the old records
    batch_delete(to_delete, 'fees', 'id_record_number')

    upload = pd.concat([new, final_breaks])
    upload.loc[:, 'source'] = source
    upload = upload.drop_duplicates(keep='first')

    upload = upload[['id', 'management_fee', 'performance_fee', 'hurdle_rate', 'high_water_mark', 'source']]
    upload.to_sql('fees', engine, if_exists='append', index=False)  # index=False prevents failure on trying to insert the index column
    print(str(len(upload['id']))+' rows inserted')


def rename_with_additional_string(df, string_without_leading_space):
    """
    Returns a dataframe whose column names are a concatenation of the original column names, plus the string parameter

    Parameters
    ---------
    df : dataframe
        a dataframe to change the column names of
    additional_string: string
        the additonal string to add to each column

    Returns
    -------
    df : dataframe
        a dataframe with adjusted column names
    """
    column_rename = {}
    for column in df.columns:
        column_rename[column] = str(column)+' '+string_without_leading_space
    df = df.rename(columns=column_rename)
    return df


def get_liquidity(source, liquidity_df, better_sources):
    """
    evaluates a dataframe to see which records insides the dataframe should be inserted to the fund_liquidity table.
    The current process checks to ensure that we are not overwriting any better sources of data,
    and then checks for any discrepancies vs. the same or worse sources. 

    Parameters
    ---------
    source : str
        the name of the source of the data
        used to populate the 'source' column in fund_liquidity table
    fees_df: dataframe
        the dataframe from 'source' that we want to evalute for insertion to the database
    better_sources: list
        list containing strings of sources that we would NOT want to overwite
        for example, if we are evaluating HFR's liquidity data, we would NOT want to overwite
        any manually-specified liqidity data or any liqidity data from albourne. therefore better_sources = 
        ['albourne','manual']

    Returns
    -------
    """
    import pandas as pd
    import numpy as np
    from sqlalchemy.engine import URL
    from sqlalchemy import create_engine
    connection_string = 'Driver={SQL Server};Server=scdb1.silvercreeksv.com;Database=scfundrisk;Trusted_Connection=yes;'
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    
    if type(better_sources) is not list:
        raise ValueError("""'better_sources' must be of type list """)
    if 'id' not in liquidity_df.columns.to_list():
        raise ValueError("""id must be a column in liquidity_df """)
    if 'redemption_notice_days' not in liquidity_df.columns.to_list():
        raise ValueError("""redemption_notice_days must be a column in liquidity_df """)
    if 'redemption_frequency' not in liquidity_df.columns.to_list():
        raise ValueError("""redemption_frequency must be a column in liquidity_df """)
    if 'redemption_gate' not in liquidity_df.columns.to_list():
        raise ValueError("""redemption_gate must be a column in liquidity_df """)
    if 'lock_up' not in liquidity_df.columns.to_list():
        raise ValueError("""lock_up must be a column in liquidity_df """)
    if 'subscription_frequency' not in liquidity_df.columns.to_list():
        raise ValueError("""subscription_frequency must be a column in liquidity_df """)
    if 'source' not in liquidity_df.columns.to_list():
        raise ValueError("""source must be a column in liquidity_df """)
    
    liquidity_df = liquidity_df.dropna(subset=['redemption_notice_days',
                                               'redemption_frequency','redemption_gate',
                                               'lock_up','subscription_frequency'], how='all').copy()

    quote_char = "'"
    ids = pd.read_sql_query("""
    select e.id,e.external_id 
    from external_entity_mapping e join (
        --query funds that do not have an fund_liquidity source
        select id from funds where id not in (select id from fund_liquidity)
        UNION
        --query funds that only use given source for fund_liquidity
        select id from fund_liquidity where source="""+quote_char+source+quote_char+"""
        UNION
        select id from fund_liquidity l where 
            (l.redemption_frequency in ('N/A','Unknown')) or 
            (redemption_frequency is null) or 
            (subscription_frequency in ('N/A','Unknown')) or 
            (subscription_frequency is null) 

    ) i on i.id=e.id 
    join funds f on f.id=e.id
    where e.mapping_status='Live'
    and e.is_shareclass=0
    and e.external_source="""+quote_char+source+quote_char+"""
    and f.id not in (
        select id from fund_liquidity where source!="""+quote_char+source+quote_char+""")
                          """, engine)
    ids = adj_dataframe(ids)
    # check to only update funds with existing fund_liquidity from given source or missing fund_liquidity
    assets_id = liquidity_df[liquidity_df['id'].isin(ids['id'].to_list())]
    assets_id = assets_id.reset_index(drop=True)

    db = pd.read_sql_query('SELECT * FROM fund_liquidity ', engine)
    db = adj_dataframe(db)
    db = rename_with_additional_string(db, 'existing')

    # check which internal IDs are in the liquidity_df but not in the list of funds whose fund_liquidity we should be updating
    # these are funds with other sources in the database
    # strip out sources that are higher in our hierarchy
    other_sources = list(liquidity_df[~liquidity_df['id'].isin(ids['id'])]['id'].unique())
    fund_check = pd.read_sql_query("""select * from funds""", engine)
    fund_check = fund_check[fund_check['id'].isin(other_sources)]
    funds_to_delete = fund_check.merge(db, how='left', left_on=['id'], right_on=['id existing'])
    worse_liq_sources = funds_to_delete[~funds_to_delete['source existing'].isin(better_sources)]

    worse_liq_sources = worse_liq_sources[['id existing',
                                           'id_record_number existing']]


    # delete the old records
    to_delete = worse_liq_sources['id_record_number existing'].to_list()
    batch_delete(to_delete, 'fund_liquidity', 'id_record_number')

    print(str(len(to_delete))+' rows of inferior fund_liquidity sources deleted from fund_liquidity table')

    ids = pd.read_sql_query("""
    select e.id,e.external_id 
    from external_entity_mapping e join (
        --query funds that do not have an fund_liquidity source
        select id from funds where id not in (select id from fund_liquidity)
        UNION
        --query funds that only use given source for fund_liquidity
        select id from fund_liquidity where source="""+quote_char+source+quote_char+"""
    ) i on i.id=e.id 
    join funds f on f.id=e.id
    where e.mapping_status='Live'
    and e.is_shareclass=0
    and e.external_source="""+quote_char+source+quote_char+"""
    and f.id not in (
        select id from fund_liquidity where source!="""+quote_char+source+quote_char+""")
    """, engine)
    ids = adj_dataframe(ids)
    # check to only update funds with existing source's fund_liquidity or missing fund_liquidity
    assets_id = liquidity_df[liquidity_df['id'].isin(ids['id'].to_list())]
    assets_id = assets_id.reset_index(drop=True)

    db = pd.read_sql_query('SELECT * FROM fund_liquidity ', engine)
    db = adj_dataframe(db)


    db = rename_with_additional_string(db, 'existing')

    merge_df = assets_id.merge(db, how='left',
                               left_on='id',
                               right_on='id existing')

    merge_df = merge_df[~merge_df['source existing'].isin(better_sources)]
    new = merge_df[merge_df['id_record_number existing'].isnull()]
    breaks = merge_df[~merge_df['id_record_number existing'].isnull()].copy()
    
    
    def updated_records(df, colname1, colname2):
        """
        Evalautes a dataframe across colname1 and colname2 to ensure that:
            1) both are not null
            2) the values in colname1 and colname2 differ
        then the function returns the dataframe.
        This is to ensure that we are not deleting and re-inserting records with nulls,
        or where the data is unchanged.

        Parameters
        ---------
        df : dataframe
            the dataframe to evaluate
        colname1: string
            the name of the first column inside df to evaluate
        colname2: string
            the name of the second column inside df to evaluate

        Returns
        -------
        df : dataframe
            the original dataframe
        """
        df = df[~df[colname1].isnull() | ~df[colname2].isnull()]
        df = df[df[colname1] != df[colname2]]
        df.reset_index(drop = True, inplace = True)
        return df
    
	
    red_not_check =  updated_records(breaks, 'redemption_notice_days', 'redemption_notice_days existing')
    red_freq_check =  updated_records(breaks, 'redemption_frequency', 'redemption_frequency existing') 
    red_gate_check = updated_records(breaks, 'redemption_gate', 'redemption_gate existing') 
    lu_check = updated_records(breaks, 'lock_up', 'lock_up existing') 
    sub_check = updated_records(breaks, 'subscription_frequency', 'subscription_frequency existing') 
	
    final_breaks = pd.concat([red_not_check, red_freq_check, red_gate_check, lu_check, sub_check])
    final_breaks = final_breaks.reset_index(drop = True)

    to_delete = final_breaks['id_record_number existing'].to_list()
    # delete the old records
    batch_delete(to_delete, 'fund_liquidity', 'id_record_number')

    #since there can be breaks across multiple fields, keep only the unique records after dropping existing columns
    final_breaks = final_breaks[['id', 'redemption_notice_days', 'redemption_frequency',
                                 'redemption_gate', 'lock_up', 'subscription_frequency', 'source']]
    final_breaks = final_breaks.drop_duplicates(keep='first')
    
    upload = pd.concat([new, final_breaks])
    upload.loc[:, 'source'] = source

    upload = upload[['id', 'redemption_notice_days', 'redemption_frequency', 'redemption_gate',
                     'lock_up', 'subscription_frequency', 'source']]
    upload.to_sql('fund_liquidity', engine, if_exists='append', index=False)  # index=False prevents failure on trying to insert the index column
    print(str(len(upload['id']))+' rows deleted and updated')
 


def send_email_with_attachment(receiver_email, sender_email, subject, body, attachment_file):
    """
    This sends an email with given subject, body, and attachment
    
    Parameters
    ----------
    receiver_email : string
        email address this is being sent to
    sender_email : string
        email address this is being sent from
    subject : string
        subject line of the email
    body : string
        body of the email
    attachment_file : string
        filename that will be attached
    
    -----
    Sample usage: 
    send_email_with_attachment("scfundriskmonitor@silvercreekcapital.com",
                           "notification@silvercreekcapital.com",
                           'Look into this',
                           'this is the body?',
                           't.csv')
    """
    import email
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.base import MIMEBase
    from email import encoders
    import sys, traceback

    port = 25
    smtp_server = "webmail.silvercreekcapital.com"
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message['Subject'] = subject

    # Add body to email
    message.attach(MIMEText(body, "plain"))

    # Open file in binary mode
    with open(attachment_file, "rb") as attachment:
        # Add file as application/octet-stream
        # Email client can usually download this automatically as attachment
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())

    # Encode file in ASCII characters to send by email    
    encoders.encode_base64(part)

    # Add header as key/value pair to attachment part
    part.add_header(
        "Content-Disposition",
        f"attachment; filename= {attachment_file}",
    )

    # Add attachment to message and convert message to string
    message.attach(part)
    sobj=smtplib.SMTP(smtp_server, port)
    sobj.ehlo()
    sobj.sendmail(sender_email, receiver_email, message.as_string())
    
def convert_lockup(row):
    """
    evaluates a lockup record to insert to the database
    since lockup is a bit (true/false) if the record here is not null and not equal to 
    no or a synonym of "no" then we say the fund has a lockup

    Parameters
    ---------
    row : 
        the dataframe record evaluate

    Returns
    -------
    true, false, or null depending on input
    """
    import numpy as np
    if type(row)==str:
        row = row.lower()
        if row in ['no', 'none', 'no lockup', 'no lock up' 'no lock']:
            return False
        elif row in ['n/a', 'na', 'unk', 'unknown']:
            return np.nan
        else:
            return True
    elif (row in [None]) or (np.isnan(row) == True): #np.nan not comparable to nan row
        return np.nan 
    else:
        raise ValueError('the lockup: '+str(row)+' is unnaccounted for in the convert_lockup function. Please investigate.')

        
def pct_returns_from_levels(df):
    """
    Returns a dataframe whose levels (values) have been converted to percentage change

    Parameters
    ---------
    df : dataframe
        a dataframe to get percentage change of 
    Returns
    -------
    df_temp : dataframe
        a dataframe with percentage returns
    """
    # select only 'number' dtypes to omit any datetimes
    df_temp = df.select_dtypes(include=['number']).pct_change(1).merge(df['asof_date'], left_index=True, right_index=True)
    df_temp.drop(index=0, inplace=True)
    date_col = df_temp.pop('asof_date')
    df_temp.insert(0, 'asof_date', date_col)  # re-insert as first column
    df_temp.reset_index(inplace=True, drop=True)
    df_temp = df_temp
    return df_temp