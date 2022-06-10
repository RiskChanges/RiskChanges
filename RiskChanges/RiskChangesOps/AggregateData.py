import geopandas as gpd
import pandas as pd


# write functions to take exposure, loss and risk + admin unit as input and give combined information as an output
def aggregateexpoure(exposure, adminunit, adminpk):
    overlaid_Data = gpd.overlay(
        exposure, adminunit[[adminpk, 'geom']], how='intersection', make_valid=True, keep_geom_type=True)
    assert not overlaid_Data.empty, f"The geospatial overlay gave empty data in exposure aggregation. check it"
    # ADMIN_ID
    try:
        overlaid_Data['exposed_areaOrLen'] = overlaid_Data['exposed'] * \
            overlaid_Data['areaOrLen']/100
        df_aggregated = overlaid_Data.groupby([adminpk, 'class'], as_index=False).agg(
            {'exposed_areaOrLen': 'sum', 'exposed': 'count'})
    except:
        df_aggregated = overlaid_Data.groupby(
            [adminpk, 'class'], as_index=False).agg({'exposed': 'count'})
    # print(df_aggregated.head())
    # df_aggregated = pd.DataFrame(df_aggregated.drop(columns='geom'))
    df_aggregated = df_aggregated.rename(columns={adminpk:'admin_id'})
    return df_aggregated


def aggregateloss(loss, adminunit, adminpk):
    overlaid_Data = gpd.overlay(
        loss, adminunit[[adminpk, 'geom']], how='intersection', make_valid=True, keep_geom_type=True)
    # ADMIN_ID
    df_aggregated = overlaid_Data.groupby(
        [adminpk], as_index=False).agg({'loss': 'sum'})
    # df_aggregated = pd.DataFrame(df_aggregated.drop(columns='geom'))
    return df_aggregated


def aggregaterisk(risk, adminunit, adminpk):
    overlaid_Data = gpd.overlay(
        risk, adminunit[[adminpk, 'geom']], how='intersection', make_valid=True, keep_geom_type=True)
    # ADMIN_ID
    df_aggregated = overlaid_Data.groupby(
        [adminpk], as_index=False).agg({'AAL': 'sum'})
    # df_aggregated = pd.DataFrame(df_aggregated.drop(columns='geom'))
    return df_aggregated
