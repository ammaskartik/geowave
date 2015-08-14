package mil.nga.giat.geowave.analytic.mapreduce.dbscan;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import mil.nga.giat.geowave.analytic.GeometryHullTool;
import mil.nga.giat.geowave.analytic.distance.DistanceFn;
import mil.nga.giat.geowave.analytic.mapreduce.dbscan.ClusterItemDistanceFn.ClusterProfileContext;
import mil.nga.giat.geowave.analytic.mapreduce.nn.DistanceProfile;
import mil.nga.giat.geowave.analytic.mapreduce.nn.NeighborList;
import mil.nga.giat.geowave.analytic.mapreduce.nn.NeighborListFactory;
import mil.nga.giat.geowave.core.index.ByteArrayId;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

/**
 * 
 * Maintains a single hull around a set of points.
 * 
 * Intended to run in a single thread. Not Thread Safe.
 * 
 */
public class SingleItemClusterList extends
		DBScanClusterList implements
		CompressingCluster<ClusterItem, Geometry>
{

	private final boolean initializedAsPoint;
	private boolean compressed = false;
	private final Set<Coordinate> clusterPoints = new HashSet<Coordinate>();

	public SingleItemClusterList(
			final int mergeSize,
			final GeometryHullTool connectGeometryTool,
			final ByteArrayId centerId,
			final ClusterItem center,
			final NeighborListFactory<ClusterItem> factory,
			final Map<ByteArrayId, Cluster<ClusterItem>> index ) {
		super(
				mergeSize,
				connectGeometryTool,
				centerId,
				index);

		final Geometry clusterGeo = center.getGeometry();

		super.clusterGeo = clusterGeo.getCentroid();

		initializedAsPoint = clusterGeo instanceof Point;

		if (initializedAsPoint) {
			clusterPoints.add(clusterGeo.getCoordinate());
		}
	}

	@Override
	public int size() {
		return super.size() + this.clusterPoints.size();
	}

	@Override
	public void invalidate() {
		super.invalidate();
	}

	@Override
	public void clear() {
		super.clear();
		clusterPoints.clear();
	}

	@Override
	protected long addAndFetchCount(
			final ByteArrayId id,
			final ClusterItem newInstance,
			final DistanceProfile<?> distanceProfile ) {
		final ClusterProfileContext context = (ClusterProfileContext) distanceProfile.getContext();

		// If initialized from a point, then any hull created during compression
		// contains that point.
		// Adding that point is not needed. Points from coordinates[0] (center)
		// are only added if they are part of more complex geometry.
		if (!initializedAsPoint) {
			final Coordinate centerCoordinate = context.getItem1() == newInstance ? context.getPoint2() : context.getPoint1();
			if (!clusterPoints.contains(centerCoordinate) && (!this.clusterGeo.covers(clusterGeo.getFactory().createPoint(
					centerCoordinate)))) {
				clusterPoints.add(centerCoordinate);
			}
		}
		final Coordinate newInstanceCoordinate = context.getItem2() == newInstance ? context.getPoint2() : context.getPoint1();
		// optimization to avoid creating a point if a representative one
		// already exists. Also, do not add if the point is already accounted
		// for
		if (newInstance.getGeometry() instanceof Point) {
			if (!clusterGeo.covers(newInstance.getGeometry())) {
				clusterPoints.add(newInstanceCoordinate);
			}
		}
		else {
			// need to create point since the provided coordinate is most likely
			// some point on a segment rather than a vertex
			if (!clusterGeo.covers(clusterGeo.getFactory().createPoint(
					newInstanceCoordinate))) {
				clusterPoints.add(newInstanceCoordinate);
			}
		}

		checkForCompression();
		return 1;
	}

	@Override
	public void merge(
			Cluster<ClusterItem> cluster ) {
		if (this == cluster) return;
		super.merge(cluster);
		final SingleItemClusterList singleItemCluster = ((SingleItemClusterList) cluster);
		if (this.compressed || singleItemCluster.compressed) {
			if (!this.compressed) {
				this.clusterGeo = singleItemCluster.clusterGeo;
				clusterPoints.addAll(singleItemCluster.clusterPoints);
			}
			else {
				if (singleItemCluster.compressed) {
					union(singleItemCluster.clusterGeo);
				}
				for (Coordinate newInstanceCoordinate : singleItemCluster.clusterPoints) {
					if (!clusterGeo.covers(clusterGeo.getFactory().createPoint(
							newInstanceCoordinate))) {
						clusterPoints.add(newInstanceCoordinate);
					}
				}
			}
		}
		else {
			clusterPoints.addAll(singleItemCluster.clusterPoints);
		}
		checkForCompression();
	}

	public boolean isCompressed() {
		return compressed;
	}

	private void checkForCompression() {
		if (clusterPoints.size() > 200) {
			clusterGeo = compress();
			incrementItemCount(clusterPoints.size());
			clusterPoints.clear();
			compressed = true;
		}
	}

	@Override
	protected Geometry compress() {
		return super.connectGeometryTool.createHullFromGeometry(
				clusterGeo,
				clusterPoints,
				true);
	}

	public static class SingleItemClusterListFactory implements
			NeighborListFactory<ClusterItem>
	{
		private final Map<ByteArrayId, Cluster<ClusterItem>> index;
		protected final GeometryHullTool connectGeometryTool = new GeometryHullTool();
		final int mergeSize;

		public SingleItemClusterListFactory(
				final int mergeSize,
				final DistanceFn<Coordinate> distanceFnForCoordinate,
				final Map<ByteArrayId, Cluster<ClusterItem>> index ) {
			super();
			this.mergeSize = mergeSize;
			connectGeometryTool.setDistanceFnForCoordinate(distanceFnForCoordinate);
			this.index = index;
		}

		public NeighborList<ClusterItem> buildNeighborList(
				final ByteArrayId centerId,
				final ClusterItem center ) {
			Cluster<ClusterItem> list = index.get(centerId);
			if (list == null) {
				list = new SingleItemClusterList(
						mergeSize,
						connectGeometryTool,
						centerId,
						center,
						this,
						index);

			}
			return list;
		}
	}
}
