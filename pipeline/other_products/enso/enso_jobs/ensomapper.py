import xarray as xr
from matplotlib import pyplot as plt
from matplotlib import colors
import matplotlib.ticker as mticker

from datetime import datetime, date, timedelta
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER


class ENSOMapper:
    def __init__(self):
        self.cmap = self.load_cmap()

    @staticmethod
    def load_cmap() -> colors.ListedColormap:
        values = []
        with open("enso_jobs/ref_files/akiko_colorscale.txt", "r") as f:
            lines = f.readlines()
            for line in lines:
                vals = line.split()
                row = [float(v) / 256 for v in vals]
                row.append(1)
                values.append(row)
        return colors.ListedColormap(values, name="my_colormap_name")

    @staticmethod
    def date_sat_map(dt: date) -> str:
        """
        TOPEX/Poseidon -> Jason-1:            14 May 2002
        Jason-1 -> Jason-2:                   12 Jul 2008
        Jason-2 -> Jason-3:                   18 Mar 2016
        Jason-3 -> Sentinel-6 Michael Freilich: 07 Apr 2022
        """
        date_ranges = [
            (date(1992, 1, 1), date(2002, 5, 14), "TOPEX/Poseidon"),
            (date(2002, 5, 14), date(2008, 7, 12), "Jason-1"),
            (date(2008, 7, 12), date(2016, 3, 18), "Jason-2"),
            (date(2016, 3, 18), date(2022, 4, 7), "Jason-3"),
            (date(2022, 4, 7), date.today() + timedelta(days=1), "Sentinel-6 Michael Freilich"),
        ]

        for start, end, satellite in date_ranges:
            if start <= dt < end:
                return satellite

    def plot_orth(self, enso_ds, date, vmin=-180, vmax=180):
        fig = plt.figure(figsize=(10, 10))
        ax = fig.add_subplot(1, 1, 1, projection=ccrs.Orthographic(-150, 10))

        ax.pcolormesh(
            enso_ds.longitude,
            enso_ds.latitude,
            enso_ds["ssha"],
            transform=ccrs.PlateCarree(),
            vmin=vmin,
            vmax=vmax,
            cmap=self.cmap,
            shading="nearest",
        )
        ax.add_feature(cfeature.OCEAN, facecolor="lightgrey")
        ax.add_feature(cfeature.LAND, facecolor="dimgrey", zorder=10)
        ax.coastlines(zorder=11)

        gl = ax.gridlines(
            crs=ccrs.PlateCarree(), linewidth=2, color="black", alpha=0.75, zorder=12
        )
        gl.xlocator = mticker.FixedLocator([])
        gl.ylocator = mticker.FixedLocator([0])
        gl.xformatter = LONGITUDE_FORMATTER
        gl.yformatter = LATITUDE_FORMATTER

        fig.set_facecolor("black")
        fig.text(
            -0.1,
            1.02,
            datetime.strftime(date, "%b %d %Y").upper(),
            color="white",
            ha="left",
            va="top",
            size=20,
            transform=ax.transAxes,
        )

        satellite = self.date_sat_map(date)

        if satellite == "Sentinel-6 Michael Freilich":
            fig.text(
                1.1,
                1.02,
                satellite.split(" ")[0],
                color="white",
                ha="right",
                va="top",
                size=20,
                transform=ax.transAxes,
                wrap=True,
            )
            fig.text(
                1.1,
                0.98,
                satellite.split("Sentinel-6 ")[-1],
                color="white",
                ha="right",
                va="top",
                size=20,
                transform=ax.transAxes,
                wrap=True,
            )
        else:
            fig.text(
                1.1,
                1.02,
                satellite,
                color="white",
                ha="right",
                va="top",
                size=20,
                transform=ax.transAxes,
                wrap=True,
            )

        outpath = f'/tmp/ENSO_ortho_{str(date).replace("-","")}.png'
        plt.savefig(outpath, bbox_inches="tight", pad_inches=0.5)

    def plot_plate(self, enso_ds, date, vmin=-180, vmax=180):
        fig = plt.figure(figsize=(20, 8))
        ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree(-180))

        g = plt.pcolormesh(
            enso_ds.longitude,
            enso_ds.latitude,
            enso_ds["ssha"],
            transform=ccrs.PlateCarree(),
            vmin=vmin,
            vmax=vmax,
            cmap=self.cmap,
        )
        ax.add_feature(cfeature.OCEAN, facecolor="lightgrey")
        ax.add_feature(cfeature.LAND, facecolor="dimgrey", zorder=10)
        ax.coastlines(zorder=11)

        gl = ax.gridlines(
            crs=ccrs.PlateCarree(),
            draw_labels=True,
            linewidth=1,
            color="gray",
            alpha=0.5,
            linestyle="--",
            zorder=15,
        )
        gl.xlabels_top = False
        gl.ylabels_right = False
        gl.xlocator = mticker.FixedLocator([40, 80, 120, 160, -160, -120, -80, -40])
        ax.xaxis.set_major_formatter(LONGITUDE_FORMATTER)
        ax.xaxis.set_minor_formatter(LONGITUDE_FORMATTER)
        gl.xlabel_style = {"size": 14}
        gl.ylabel_style = {"size": 14}

        plt.title(
            f'{self.date_sat_map(date)} Sea Level Residuals {datetime.strftime(date, "%b %d %Y").upper()}',
            size=16,
        )
        cb = plt.colorbar(g, orientation="horizontal", shrink=0.5, aspect=30, pad=0.1)
        cb.set_label("MM", fontsize=14)
        cb.ax.tick_params(labelsize=12)
        fig.tight_layout()

        outpath = f'/tmp/ENSO_plate_{str(date).replace("-","")}.png'
        plt.savefig(outpath, bbox_inches="tight", pad_inches=0.5)

    def make_maps(self, ds: xr.Dataset):
        date_dt = datetime.strptime(str(ds.time.values)[:10], "%Y-%m-%d").date()
        self.plot_orth(ds, date_dt)
        self.plot_plate(ds, date_dt)
