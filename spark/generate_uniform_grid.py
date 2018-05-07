import json
import shapefile
from point_location import in_us

N = 72.
S = 18.
W = -178.
E = -65.

N_lat = 430
N_lon = 860

d_lat = (N-S)/float(N_lat)
d_lon = (E-W)/float(N_lon)

grid = []
grid_id = 0
precision = 3
for ilat in range(0, N_lat):
    for ilon in range(0, N_lon):
        lat = S + d_lat*ilat
        lon = W + d_lon*ilon
        if in_us(lat, lon):
            grid_id += 1
            grid.append({"id": grid_id, "lat": round(lat, precision), "lon": round(lon, precision)})

print(grid_id)
with open('grid.json', 'w') as f:
    json.dump(grid, f)
