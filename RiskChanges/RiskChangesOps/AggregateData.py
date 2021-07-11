import RiskChangesOps.readmeta as readmeta
import RiskChangesOps.readvector as readvector
import geopandas as gpd
import pandas as pd

# write functions to take exposure, loss and risk + admin unit as input and give combined information as an output
def aggregateexpoure(exposure,adminunit):
    overlaid_Data=gpd.overlay(exposure, adminunit['ADMIN_ID','geom'], how='intersection', make_valid=True, keep_geom_type=True)
    #ADMIN_ID
    try:
        overlaid_Data['exposed_areaOrLen']=overlaid_Data['exposed']*overlaid_Data['areaOrLen']/100
        df_aggregated=overlaid_Data.groupby(['ADMIN_ID','class'],as_index=False).agg({'exposed_areaOrLen':'sum','exposed':'count'})
    except:
        df_aggregated=overlaid_Data.groupby(['ADMIN_ID','class'],as_index=False).agg({'exposed':'count'})
    df_aggregated = pd.DataFrame(df_aggregated.drop(columns='geometry'))
    return df_aggregated

def aggregateloss(loss,adminunit):
    overlaid_Data=gpd.overlay(loss, adminunit['ADMIN_ID','geom'], how='intersection', make_valid=True, keep_geom_type=True)
    #ADMIN_ID
    df_aggregated=overlaid_Data.groupby(['ADMIN_ID'],as_index=False).agg({'loss':'sum'})
    df_aggregated = pd.DataFrame(df_aggregated.drop(columns='geometry'))
    return df_aggregated


def aggregaterisk(risk,adminunit):
    overlaid_Data=gpd.overlay(risk, adminunit['ADMIN_ID','geom'], how='intersection', make_valid=True, keep_geom_type=True)
    #ADMIN_ID
    df_aggregated=overlaid_Data.groupby(['ADMIN_ID'],as_index=False).agg({'AAL':'sum'})
    df_aggregated = pd.DataFrame(df_aggregated.drop(columns='geometry'))
    return df_aggregated

