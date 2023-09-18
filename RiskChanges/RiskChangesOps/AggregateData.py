import geopandas as gpd
import pandas as pd
# import logging
# logger = logging.getLogger(__file__)

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
    df_aggregated = df_aggregated.rename(columns={adminpk: 'admin_id'})
    return df_aggregated


def aggregateloss(loss, adminunit, adminpk, admin_dataid):
    overlaid_Data = gpd.overlay(
        loss, adminunit[[adminpk, admin_dataid, 'geom']], how='intersection', make_valid=True, keep_geom_type=True)
    # ADMIN_ID
    df_aggregated = overlaid_Data.groupby(
        [adminpk, admin_dataid], as_index=False).agg({'loss': 'sum'})
    # df_aggregated = pd.DataFrame(df_aggregated.drop(columns='geom'))
    return df_aggregated


def aggregaterisk(risk, adminunit, adminpk, admin_dataid):
    geometry_types=risk.geometry.geom_type.unique()
    geometry_type=''
    if len(geometry_types) == 1:
        geometry_type = geometry_types[0]
    else:
        print("Multiple geometry types found:", geometry_types)
    if geometry_type=="Polygon":
        risk['centroid'] = risk.centroid
        temp_risk_gdf=gpd.GeoDataFrame(risk, geometry='centroid')
        overlaid_Data = gpd.overlay(
            temp_risk_gdf, adminunit[[adminpk, admin_dataid, 'geom']], how='intersection', make_valid=True, keep_geom_type=True)
    else:
        overlaid_Data = gpd.overlay(
            risk, adminunit[[adminpk, admin_dataid, 'geom']], how='intersection', make_valid=True, keep_geom_type=True)
        
    # ADMIN_ID
    df_aggregated = overlaid_Data.groupby(
        [adminpk, admin_dataid], as_index=False).agg({'AAL': 'sum'})
    # df_aggregated = pd.DataFrame(df_aggregated.drop(columns='geom'))
    return df_aggregated
