import pandas as pd
import numpy as np
from pathlib import Path
from math import radians, cos, sin, asin, sqrt


def haversine_distance(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    
    # Haversine formula (shortest distance between two points on a sphere)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    
    r = 6371  # (Radius of Earth in km)
    
    return c * r


def calculate_closest5_average(station_lat, station_lon, turbine_coords):
    distances = []
    
    for turbine_lat, turbine_lon in turbine_coords:
        dist = haversine_distance(station_lat, station_lon, turbine_lat, turbine_lon)
        distances.append(dist)
    
    distances.sort()
    closest_5 = distances[:5] # Average of 5 closest turbines
    
    return np.mean(closest_5)


def normalize_score(avg_distance, max_distance):
    # (0 = furthest, 1 = closest; normalized by the max. average distance in dataset)
    normalized_score = avg_distance / max_distance
    turbine_score = 1 - normalized_score
    
    return turbine_score


def compute_all_station_scores(station_data_df, turbine_coords):
    print(f"Computing scores for {len(station_data_df)} stations...")
    
    turbine_array = turbine_coords.values if isinstance(turbine_coords, pd.DataFrame) else turbine_coords
    
    distances_dict = {}
    for idx, (station_id, group) in enumerate(station_data_df.groupby('id')):
        station_lat = group['lat'].values[0]
        station_lon = group['lon'].values[0]
        
        avg_distance = calculate_closest5_average(station_lat, station_lon, turbine_array)
        distances_dict[station_id] = avg_distance
        
        if (idx + 1) % 20 == 0:
            print(f"  Processed {idx + 1}/{len(station_data_df)} stations...")
    
    max_distance = max(distances_dict.values())
    print(f"Maximum closest-5 average distance: {max_distance:.2f} km\n")
    
    return distances_dict, max_distance


def load_data():
    turbine_file = Path('data/turbine_data/turbines.csv')
    turbines_df = pd.read_csv(turbine_file)
    turbine_coords = turbines_df[['Latitude', 'Longitude']].copy()
    
    station_data_file = Path('data/mesonet_data/station_data.csv')
    station_data_df = pd.read_csv(station_data_file)
    
    return turbine_coords, station_data_df


def process_batch(batch_folder, output_dir, score_lookup):
    output_batch_folder = output_dir / batch_folder.name
    output_batch_folder.mkdir(exist_ok=True)
    
    files_processed = 0
    for csv_file in batch_folder.glob('*_measurements.csv'):
        station_id = csv_file.stem.replace('_measurements', '')
        
        turbine_score = score_lookup.get(station_id)
        
        if turbine_score is None:
            print(f"Warning: No score found for station {station_id}, skipping...")
            continue
        
        # Add score label column
        try:
            df = pd.read_csv(csv_file)
            df['TurbineScore'] = turbine_score

            output_file = output_batch_folder / csv_file.name
            df.to_csv(output_file, index=False)
            files_processed += 1
            
        except Exception as e:
            print(f"Error processing {csv_file.name}: {e}")
    
    return files_processed


def label_mesonet_data():    
    mesonet_dir = Path('data/mesonet_data')
    output_dir = Path('data/labeled_mesonet_data')
    output_dir.mkdir(exist_ok=True)
    
    print("Loading turbine and station data...")
    turbine_coords, station_data_df = load_data()
    print(f"  Loaded {len(turbine_coords)} turbines")
    print(f"  Loaded {len(station_data_df)} stations\n")
    
    distances_dict, max_distance = compute_all_station_scores(station_data_df, turbine_coords)
    
    # Score lookup with normalized values
    score_lookup = {
        station_id: normalize_score(avg_dist, max_distance)
        for station_id, avg_dist in distances_dict.items()
    }
    
    print("Processing measurement files...")
    batch_folders = sorted(mesonet_dir.glob('batch_*'))
    total_files = 0

    for batch_folder in batch_folders:
        print(f"  Processing {batch_folder.name}...")
        files_processed = process_batch(batch_folder, output_dir, score_lookup)
        total_files += files_processed
    
    print(f"\n{'=' * 60}")
    print(f"Processed {total_files} measurement files across {len(batch_folders)} batches")
    print(f"Output saved to: {output_dir.absolute()}")
    print(f"{'=' * 60}")
    

if __name__ == "__main__":
    label_mesonet_data()
