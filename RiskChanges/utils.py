def get_geom_type(geom_types):
    '''
    geom_types: list of unique geom types
    '''
    geometrytype=None
    try:
        if len(geom_types) == 1:
            geometrytype = geom_types[0]
            if geometrytype== "Point" or  geometrytype== "MultiPoint":
                geometrytype='point'
            elif geometrytype== "LineString" or  geometrytype== "MultiLineString":
                geometrytype='line'
            elif geometrytype== "Polygon" or  geometrytype== "MultiPolygon":
                geometrytype='polygon'
            else:
                return False, f"invalid geom_type {geom_types}"
        elif len(geom_types) == 2:
            if "Point" and "MultiPoint" in geom_types:
                geometrytype = "point"
            elif "LineString" and "MultiLineString" in geom_types:
                geometrytype = "line"
            elif "Polygon" and "MultiPolygon" in geom_types:
                geometrytype = "polygon"
            else:
                return False,f"error occured : more than one geometry type is found i.e. {geom_types}"
        elif len(geom_types) > 2:
            return False, f"error occured : more than one geometry type is found i.e. {geom_types}"
        else:
            return False, f"error occured : missing geometry"
        return True, geometrytype
    except Exception as e:
        return False, str(e)