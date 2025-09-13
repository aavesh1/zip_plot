import pandas as pd
import geopandas as gpd
from flask import Flask, jsonify, render_template, request
from shapely.errors import GEOSException
import os

# --- 1. CONFIGURATION & SETUP ---
TERRITORY_WORKLOAD_THRESHOLD = 250
app = Flask(__name__)

# --- 2. DATA LOADING & PRE-PROCESSING ---
def load_and_process_data():
    """
    Loads all data, calculates territory counts, and prepares a master GeoDataFrame.
    """
    try:
        data_dir = 'data'
        shapefile_path = os.path.join(data_dir, 'tl_2024_us_zcta520.shp')
        call_plan_path = os.path.join(data_dir, 'call_plan.csv')
        hierarchy_path = os.path.join(data_dir, 'hierarchy.csv')

        print("Loading ZIP code shapefile...")
        zip_geometries = gpd.read_file(shapefile_path)[['ZCTA5CE20', 'geometry']]
        zip_geometries['ZCTA5CE20'] = zip_geometries['ZCTA5CE20'].astype(str)

        print("Loading call plan data...")
        call_plan_df = pd.read_csv(call_plan_path)
        call_plan_df['zip'] = call_plan_df['zip'].astype(str)
        zip_aggregations = call_plan_df.groupby('zip').agg(
            calls=('calls', 'sum'),
            hcp_count=('hcpid', 'count')
        ).reset_index()
        zip_aggregations.rename(columns={'zip': 'ZCTA5CE20'}, inplace=True)

        print("Loading hierarchy data and calculating territory counts...")
        hierarchy_df = pd.read_csv(hierarchy_path)
        hierarchy_df['zip'] = hierarchy_df['zip'].astype(str)
        
        district_territory_counts = hierarchy_df.groupby('district')['territory'].nunique().to_dict()
        region_territory_counts = hierarchy_df.groupby('region')['territory'].nunique().to_dict()

        hierarchy_df.rename(columns={'zip': 'ZCTA5CE20'}, inplace=True)
        
        print("Merging all data sources...")
        merged_df = pd.merge(zip_aggregations, hierarchy_df, on='ZCTA5CE20', how='left')
        master_gdf = pd.merge(zip_geometries, merged_df, on='ZCTA5CE20', how='inner')
        master_gdf[['territory', 'district', 'region']] = master_gdf[['territory', 'district', 'region']].fillna('Unassigned')
        
        print("Data loading and pre-processing complete.")
        return master_gdf, district_territory_counts, region_territory_counts

    except Exception as e:
        print(f"FATAL ERROR during data loading: {e}")
        return None, None, None

master_gdf, district_terr_counts, region_terr_counts = load_and_process_data()


# --- 3. FLASK ROUTES ---

@app.route('/')
def index():
    """Serves the main HTML page."""
    return render_template('index.html')

@app.route('/data')
def get_geospatial_data():
    """
    Processes and returns data with workload, HCP count, and the calculated threshold.
    """
    if master_gdf is None: return jsonify({"error": "Data could not be loaded."}), 500

    level = request.args.get('level', 'Region').lower()
    dissolve_col = 'ZCTA5CE20'
    if level in ['territory', 'district', 'region']:
        dissolve_col = level

    print(f"Processing request for level: {level}")
    
    try:
        aggregated = master_gdf.dissolve(by=dissolve_col, aggfunc={'calls': 'sum', 'hcp_count': 'sum'})
        aggregated = aggregated.reset_index().rename(columns={dissolve_col: 'name'})

        if level == 'territory':
            aggregated['threshold'] = TERRITORY_WORKLOAD_THRESHOLD
        elif level == 'district':
            aggregated['threshold'] = aggregated['name'].map(district_terr_counts).fillna(0) * TERRITORY_WORKLOAD_THRESHOLD
        elif level == 'region':
            aggregated['threshold'] = aggregated['name'].map(region_terr_counts).fillna(0) * TERRITORY_WORKLOAD_THRESHOLD
        else:
            aggregated['threshold'] = 0

        return aggregated[['name', 'calls', 'hcp_count', 'threshold', 'geometry']].to_json()
    except Exception as e:
        print(f"Error during data processing for level '{level}': {e}")
        return jsonify({"error": f"Failed to process data for level {level}"}), 500

# NEW: Endpoint to provide all searchable names to the frontend
@app.route('/names')
def get_all_names():
    """
    Returns a JSON object with lists of all unique names for each level.
    """
    if master_gdf is None:
        return jsonify({"error": "Data not loaded"}), 500
    
    names = {
        'region': sorted(master_gdf['region'].unique().tolist()),
        'district': sorted(master_gdf['district'].unique().tolist()),
        'territory': sorted(master_gdf['territory'].unique().tolist()),
        'zip': sorted(master_gdf['ZCTA5CE20'].unique().tolist())
    }
    return jsonify(names)

# --- 4. RUN THE APPLICATION ---
if __name__ == '__main__':
    app.run(debug=True)

