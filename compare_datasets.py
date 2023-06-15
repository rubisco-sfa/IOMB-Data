"""."""
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.pyplot as plt
import numpy as np
from ILAMB.Variable import Variable
from mpl_toolkits.axes_grid1.inset_locator import inset_axes


def CreateVariableComparisonArray(data, cmap, name, filename, ncolors=7):
    # initialization stuff
    mean_cmap = plt.cm.get_cmap(cmap, ncolors)
    bias_cmap = plt.cm.get_cmap("seismic", ncolors)
    sources = sorted(list(data.keys()))
    n = len(data)

    # interpolate all variables to a composed grid
    lat = None
    lon = None
    for i in range(n):
        if lat is None:
            lat = data[sources[i]].lat
            lon = data[sources[i]].lon
        else:
            lat = np.hstack([lat, data[sources[i]].lat])
            lon = np.hstack([lon, data[sources[i]].lon])
    lat = np.unique(lat)
    lon = np.unique(lon)
    for i in range(n):
        data[sources[i]] = data[sources[i]].interpolate(lat=lat, lon=lon)

    # find limits of the difference
    bias = None
    for i in range(n):
        for j in range(n):
            if i < j:
                a = data[sources[i]]
                b = data[sources[j]]
                if bias is None:
                    bias = np.abs(a.data - b.data).compressed()
                else:
                    bias = np.hstack([bias, np.abs(a.data - b.data).compressed()])
    bias = np.percentile(bias, 98)

    # find limits of the mean
    values = None
    for key in data:
        if values is None:
            values = data[key].data.compressed()
        else:
            values = np.hstack([values, data[key].data.compressed()])
    limits = np.percentile(values, [2, 98])

    # plots
    f = 1.5
    fig = plt.figure(figsize=(f * 4 * n, (f + 0.5 * (n == 2)) * 2.2 * n), dpi=200)
    mean_ax = None
    bias_ax = None
    for i in range(n):
        for j in range(n):
            if i == j:
                a = data[sources[i]]
                lat = np.hstack([a.lat_bnds[:, 0], a.lat_bnds[-1, -1]])
                lon = np.hstack([a.lon_bnds[:, 0], a.lon_bnds[-1, -1]])
                ax = fig.add_subplot(n, n, n * i + j + 1, projection=ccrs.Robinson())
                if i == 0:
                    mean_ax = ax
                mean_plot = ax.pcolormesh(
                    lon,
                    lat,
                    a.data,
                    cmap=mean_cmap,
                    vmin=limits[0],
                    vmax=limits[1],
                    transform=ccrs.PlateCarree(),
                )
                ax.set_title(sources[i])
                ax.add_feature(
                    cfeature.NaturalEarthFeature(
                        "physical", "land", "110m", edgecolor="face", facecolor="0.875"
                    ),
                    zorder=-1,
                )
                ax.add_feature(
                    cfeature.NaturalEarthFeature(
                        "physical", "ocean", "110m", edgecolor="face", facecolor="0.750"
                    ),
                    zorder=-1,
                )
            else:
                a = data[sources[i]]
                b = data[sources[j]]
                if i < j:
                    ax = fig.add_subplot(
                        n, n, n * i + j + 1, projection=ccrs.Robinson()
                    )
                    if i == 0 and j == (n - 1):
                        bias_ax = ax
                    bias_plot = ax.pcolormesh(
                        lon,
                        lat,
                        a.data - b.data,
                        vmin=-bias,
                        vmax=+bias,
                        cmap=bias_cmap,
                        transform=ccrs.PlateCarree(),
                    )
                    ax.set_title(sources[i])
                    ax.add_feature(
                        cfeature.NaturalEarthFeature(
                            "physical",
                            "land",
                            "110m",
                            edgecolor="face",
                            facecolor="0.875",
                        ),
                        zorder=-1,
                    )
                    ax.add_feature(
                        cfeature.NaturalEarthFeature(
                            "physical",
                            "ocean",
                            "110m",
                            edgecolor="face",
                            facecolor="0.750",
                        ),
                        zorder=-1,
                    )
                    ax.set_title("%s - %s" % (sources[i], sources[j]))
                else:
                    ax = fig.add_subplot(n, n, n * i + j + 1)
                    mask = a.data.mask + b.data.mask
                    x = np.ma.masked_array(a.data, mask=mask).compressed()
                    y = np.ma.masked_array(b.data, mask=mask).compressed()
                    ax.plot([limits[0], limits[1]], [limits[0], limits[1]], "--r")
                    ax.scatter(x, y, color="k", s=0.6, alpha=0.1, linewidths=0)
                    ax.set_xlim(limits[0], limits[1])
                    ax.set_ylim(limits[0], limits[1])
                    ax.set_xlabel(sources[i])
                    ax.set_ylabel(sources[j])
                    ax.spines["right"].set_color("none")
                    ax.spines["top"].set_color("none")

    axins = inset_axes(
        mean_ax,
        width="100%",
        height="30%",
        bbox_to_anchor=(0.0, 1.2, 1.0, 0.3),
        bbox_transform=mean_ax.transAxes,
        borderpad=0,
    )
    fig.colorbar(
        mean_plot, orientation="horizontal", cax=axins, label="%s [%s]" % (name, a.unit)
    )

    axins_bias = inset_axes(
        bias_ax,
        width="100%",
        height="30%",
        bbox_to_anchor=(0.0, 1.2, 1.0, 0.3),
        bbox_transform=bias_ax.transAxes,
        borderpad=0,
    )
    fig.colorbar(
        bias_plot,
        orientation="horizontal",
        cax=axins_bias,
        label="Differences [%s]" % (a.unit),
    )

    fig.savefig(filename)
    plt.close()


if __name__ == "__main__":
    # dCant
    data = {}
    data["Gruber"] = Variable(filename="Gruber/Gruber.nc", variable_name="dcant")
    data["OCIM"] = Variable(filename="OCIM/OCIM.nc", variable_name="dcant")
    for source, v in data.items():
        if v.temporal:
            v = v.integrateInTime(mean=True)
        data[source] = v
    CreateVariableComparisonArray(data, "plasma", "dCant 1994-2007", "dCant.png")
