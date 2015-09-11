package mil.nga.giat.geowave.analytic.mapreduce.dbscan;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import mil.nga.giat.geowave.analytic.GeometryHullTool;
import mil.nga.giat.geowave.analytic.distance.DistanceFn;
import mil.nga.giat.geowave.analytic.mapreduce.dbscan.ClusterItemDistanceFn.ClusterProfileContext;
import mil.nga.giat.geowave.analytic.nn.DistanceProfile;
import mil.nga.giat.geowave.analytic.nn.NeighborList;
import mil.nga.giat.geowave.analytic.nn.NeighborListFactory;
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
				(int) center.getCount(),
				mergeSize,
				connectGeometryTool,
				centerId,
				index);

		final Geometry clusterGeo = center.getGeometry();

		initializedAsPoint = clusterGeo instanceof Point;

		this.compressed = center.isCompressed();

		if (initializedAsPoint || compressed) {
			clusterPoints.add(clusterGeo.getCentroid().getCoordinate());
			super.clusterGeo = clusterGeo;
		}
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

		boolean checkForCompress = false;
		// If initialized from a point, then any hull created during compression
		// contains that point.
		// Adding that point is not needed. Points from coordinates[0] (center)
		// are only added if they are part of more complex geometry.
		if (!initializedAsPoint) {
			final Coordinate centerCoordinate = context.getItem1() == newInstance ? context.getPoint2() : context.getPoint1();
			checkForCompress = clusterPoints.add(centerCoordinate);
		}
		final Coordinate newInstanceCoordinate = context.getItem2() == newInstance ? context.getPoint2() : context.getPoint1();

		checkForCompress = clusterPoints.add(newInstanceCoordinate);
		/*
		 * // optimization to avoid creating a point if a representative one //
		 * already exists. Also, do not add if the point is already accounted //
		 * for if (newInstance.getGeometry() instanceof Point) { if (clusterGeo
		 * == null || clusterGeo instanceof Point) { checkForCompress =
		 * clusterPoints.add(newInstanceCoordinate); } else if
		 * (!clusterGeo.contains(newInstance.getGeometry())) { checkForCompress
		 * = clusterPoints.add(newInstanceCoordinate); } } else { // need to
		 * create point since the provided coordinate is most likely // some
		 * point on a segment rather than a vertex if (clusterGeo == null ||
		 * !clusterGeo.contains(clusterGeo.getFactory().createPoint(
		 * newInstanceCoordinate))) { checkForCompress =
		 * clusterPoints.add(newInstanceCoordinate); } }
		 */
		if (checkForCompress) checkForCompression();
		return 1;
	}

	@Override
	public void merge(
			Cluster<ClusterItem> cluster ) {
		if (this == cluster) return;

		final SingleItemClusterList singleItemCluster = ((SingleItemClusterList) cluster);
		// do not want to count these items with the geometry intersection
		singleItemCluster.itemCount -= singleItemCluster.clusterPoints.size();

		super.merge(cluster);

		if (singleItemCluster.clusterGeo != null) {
			clusterPoints.addAll(Arrays.asList(singleItemCluster.clusterGeo.getCoordinates()));
		}

		/*
		 * if (this.compressed || singleItemCluster.compressed) { if
		 * (!this.compressed) { this.clusterGeo = singleItemCluster.clusterGeo;
		 * add( this.clusterPoints, singleItemCluster.clusterPoints);
		 * clusterPoints.addAll(singleItemCluster.clusterPoints);
		 * singleItemCluster.clusterPoints.clear(); } else { if
		 * (singleItemCluster.compressed) { clusterGeo =
		 * connectGeometryTool.createHullFromGeometry( clusterGeo,
		 * Arrays.asList(singleItemCluster.clusterGeo.getCoordinates()), false);
		 * } add( singleItemCluster.clusterPoints, this.clusterPoints); } } else
		 * {
		 */

		// handle any remaining points
		long snapShot = clusterPoints.size();
		clusterPoints.addAll(singleItemCluster.clusterPoints);
		incrementItemCount(clusterPoints.size() - snapShot);

		checkForCompression();
	}

	/*
	 * private void add( final Set<Coordinate> fromCluster, final
	 * Set<Coordinate> toCluster ) { for (Coordinate newInstanceCoordinate :
	 * fromCluster) { if (clusterGeo == null ||
	 * !clusterGeo.contains(clusterGeo.getFactory().createPoint(
	 * newInstanceCoordinate))) { itemCount +=
	 * (toCluster.add(newInstanceCoordinate) ? 1 : 0); } } fromCluster.clear();
	 * }
	 */

	public boolean isCompressed() {
		return compressed;
	}

	private void checkForCompression() {
		if (clusterPoints.size() > 200) {
			clusterGeo = compress();
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
