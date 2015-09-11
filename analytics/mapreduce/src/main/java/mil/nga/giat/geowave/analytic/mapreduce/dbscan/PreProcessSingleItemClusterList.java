package mil.nga.giat.geowave.analytic.mapreduce.dbscan;

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
public class PreProcessSingleItemClusterList extends
		SingleItemClusterList implements
		CompressingCluster<ClusterItem, Geometry>
{

	public PreProcessSingleItemClusterList(
			int mergeSize,
			GeometryHullTool connectGeometryTool,
			ByteArrayId centerId,
			ClusterItem center,
			NeighborListFactory<ClusterItem> factory,
			Map<ByteArrayId, Cluster<ClusterItem>> index ) {
		super(
				mergeSize,
				connectGeometryTool,
				centerId,
				center,
				factory,
				index);
	}

	protected void mergeIfPossible(
			final boolean deleteNonLinks ) {}

	public static class PreProcessSingleItemClusterListFactory implements
			NeighborListFactory<ClusterItem>
	{
		private final Map<ByteArrayId, Cluster<ClusterItem>> index;
		protected final GeometryHullTool connectGeometryTool = new GeometryHullTool();
		final int mergeSize;

		public PreProcessSingleItemClusterListFactory(
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
				list = new PreProcessSingleItemClusterList(
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
