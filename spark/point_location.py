from shapely.geometry import MultiPoint, Point, Polygon
import shapefile
#return a polygon for each state in a dictionary
def get_us_border_polygon():

    sf = shapefile.Reader("./states/cb_2017_us_state_20m")
    shapes = sf.shapes()
    fields = sf.fields
    records = sf.records()
    state_polygons = {}
    for i, record in enumerate(records):
        state = record[5]
        points = shapes[i].points
        poly = Polygon(points)
        state_polygons[state] = poly

    return state_polygons


#us border
state_polygons = get_us_border_polygon()   
#check if in one of the states then True, else False
def in_us(lat, lon):
    p = Point(lon, lat)
    for state, poly in state_polygons.items():
        if poly.contains(p):
            return state
    return None

