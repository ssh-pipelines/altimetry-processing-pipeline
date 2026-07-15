import os

import cartopy
import matplotlib.pyplot as plt

'''
Initializes cartopy by pre-downloading features such that downloads aren't attempted during runtime
'''

cartopy.config['data_dir'] = os.getenv('CARTOPY_DATA_DIR', cartopy.config.get('data_dir'))

fig, ax = plt.subplots(subplot_kw={'projection': cartopy.crs.PlateCarree()})
ax.coastlines('110m')    # Explicitly specify resolution to ensure pre-loading
ax.add_feature(cartopy.feature.OCEAN)  # Example color; adjust as needed
ax.add_feature(cartopy.feature.LAND)   # Example color; adjust as needed

# Force feature download
plt.savefig('cartopy_test_map.png')
os.remove('cartopy_test_map.png')
