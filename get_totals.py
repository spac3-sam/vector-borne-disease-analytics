from collections import defaultdict
from tqdm import tqdm
import pandas as pd
import h5py
import os

if __name__ == '__main__':
	promed_df = pd.read_feather(r'F:\OneDrive\School\research\thilanka-summer2021\scraper2.0\combined_df_raw.feather')

	focus_points = pd.Series(list(zip(promed_df.gpm_lat_idx, promed_df.gpm_lon_idx))).unique()
	precip_totals = defaultdict(list)

	for dataset_path in tqdm([x for x in os.listdir() if x.endswith('.HDF5')]):
		with h5py.File(dataset_path, 'r') as dataset:
			for lat, lon in tqdm(focus_points, leave=False):
				precip_totals[(lat, lon)].append(dataset['Grid']['precipitation'][0][lon][lat])

	output = pd.DataFrame([[key[0], key[1], value] for key, value in precip_totals.items()], columns=['lat_idx', 'lon_idx', 'precips'])
	output.to_csv('total_precip.csv')