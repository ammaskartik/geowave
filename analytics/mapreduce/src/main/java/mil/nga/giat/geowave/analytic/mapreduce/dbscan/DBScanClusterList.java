package mil.nga.giat.geowave.analytic.mapreduce.dbscan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import mil.nga.giat.geowave.analytic.GeometryHullTool;
import mil.nga.giat.geowave.analytic.nn.DistanceProfile;
import mil.nga.giat.geowave.core.index.ByteArrayId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.TopologyException;

/**
 * 
 * Represents a cluster. Maintains links to other clusters through shared
 * components Maintains counts contributed by components of this cluster.
 * Supports merging with other clusters, incrementing the count by only those
 * components different from the other cluster.
 * 
 * Intended to run in a single thread. Not Thread Safe.
 * 
 */
public abstract class DBScanClusterList implements
		CompressingCluster<ClusterItem, Geometry>
{

	protected static final Logger LOGGER = LoggerFactory.getLogger(DBScanClusterList.class);

	// internal state
	protected Geometry clusterGeo = null;
	protected int itemCount = 1;
	private final Set<ByteArrayId> linkedClusters = new HashSet<ByteArrayId>();
	private final List<ByteArrayId> ids = new ArrayList<ByteArrayId>(
			4);
	private boolean isFinished = false;
	protected final GeometryHullTool connectGeometryTool;
	int mergeSize = 0;

	// global state
	// ID to cluster.
	private final Map<ByteArrayId, Cluster<ClusterItem>> index;

	public DBScanClusterList(
			final int itemCount,
			final int mergeSize,
			final GeometryHullTool connectGeometryTool,
			final ByteArrayId centerId,
			final Map<ByteArrayId, Cluster<ClusterItem>> index ) {
		super();
		this.itemCount = itemCount;
		this.mergeSize = mergeSize;
		this.connectGeometryTool = connectGeometryTool;
		this.index = index;
		this.ids.add(centerId);
	}

	protected abstract long addAndFetchCount(
			final ByteArrayId newId,
			final ClusterItem newInstance,
			final DistanceProfile<?> distanceProfile );

	@Override
	public final boolean add(
			final DistanceProfile<?> distanceProfile,
			final ByteArrayId newId,
			final ClusterItem newInstance ) {

		LOGGER.trace(
				"link {} to {}",
				newId,
				ids);

		if (!linkedClusters.add(newId)) return false;

		Cluster<ClusterItem> cluster = index.get(newId);

		if (cluster == this )
			return false;

		incrementItemCount(addAndFetchCount(
				newId,
				newInstance,
				distanceProfile));

		if (isFinished) mergeIfPossible(false);

		return true;
	}

	protected void incrementItemCount(
			long amount ) {
		int c = itemCount;
		itemCount += amount;
		assert (c <= itemCount);
	}

	/**
	 * Clear the contents. Invoked when the contents of a cluster are merged
	 * with another cluster. This method is supportive for GC, not serving any
	 * algorithm logic.
	 */

	@Override
	public void clear() {
		linkedClusters.clear();
		clusterGeo = null;
	}

	@Override
	public void invalidate() {
		ByteArrayId id = ids.get(0);
		for (ByteArrayId linkedId : this.linkedClusters) {
			DBScanClusterList linkedCluster = (DBScanClusterList) index.get(linkedId);
			if (linkedCluster != null && linkedCluster != this) {
				linkedCluster.linkedClusters.remove(id);
			}
		}
		LOGGER.trace("Invalidate " + id);
		index.remove(id);
		clusterGeo = null;
		itemCount = -1;
	}

	@Override
	public InferType infer(
			final ByteArrayId id,
			final ClusterItem value ) {
		final Cluster<ClusterItem> cluster = index.get(id);
		if (cluster == this || linkedClusters.contains(id)) return InferType.SKIP;
		return InferType.NONE;
	}

	@Override
	public Iterator<Entry<ByteArrayId, ClusterItem>> iterator() {
		return Collections.<Entry<ByteArrayId, ClusterItem>> emptyList().iterator();
	}

	@Override
	public int currentLinkSetSize() {
		return linkedClusters.size();
	}

	public void finish() {
		this.mergeIfPossible(true);
		isFinished = true;
	}

	public boolean isFinished() {
		return isFinished;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + ((ids == null) ? 0 : ids.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final DBScanClusterList other = (DBScanClusterList) obj;
		if (ids == null) {
			if (other.ids != null) {
				return false;
			}
		}
		else if (!ids.equals(other.ids)) {
			return false;
		}
		return true;
	}

	@Override
	public int size() {
		return (int) (itemCount);
	}

	@Override
	public boolean isEmpty() {
		return size() <= 0;
	}

	@Override
	public Geometry get() {
		return compress();
	}

	@Override
	public abstract boolean isCompressed();

	@Override
	public void merge(
			final Cluster<ClusterItem> cluster ) {
		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace(
					"Merging {} into {}",
					cluster.getId(),
					this.ids);
		}
		if (cluster != this) {
			for (ByteArrayId id : ((DBScanClusterList) cluster).ids) {
				index.put(
						id,
						this);
				this.ids.add(id);
			}
			this.linkedClusters.addAll(((DBScanClusterList) cluster).linkedClusters);
			if (isCompressed() && ((DBScanClusterList) cluster).isCompressed()) {
				incrementItemCount((long) (interpolateFactor(((DBScanClusterList) cluster).clusterGeo) * ((DBScanClusterList) cluster).itemCount));
			}
		}
	}

	protected double interpolateFactor(
			final Geometry areaBeingMerged ) {
		try {
			if (clusterGeo == null) return 1.0;
			Geometry intersection = areaBeingMerged.intersection(clusterGeo);
			double geo2Area = areaBeingMerged.getArea();
			if (intersection != null) {
				if (intersection instanceof Point && areaBeingMerged instanceof Point)
					return 0.0;
				else if (intersection.isEmpty())
					return 1.0;
				else if (geo2Area > 0)
					return 1.0 - (intersection.getArea() / geo2Area);
				else
					return 0.0;
			}
			return 1.0;
		}
		catch (final Exception ex) {
			LOGGER.warn(
					"Cannot calculate difference of geometries to interpolate size ",
					ex);
		}
		return 0.0;

	}

	@Override
	public ByteArrayId getId() {
		return ids.get(0);
	}

	protected abstract Geometry compress();

	@Override
	public Iterator<ByteArrayId> getLinkedClusters() {
		return linkedClusters.iterator();
	}

	protected void union(
			Geometry otherGeo ) {

		if (otherGeo == null) return;
		try {

			if (clusterGeo == null) {
				clusterGeo = otherGeo;
			}
			else if (clusterGeo instanceof Point) {
				clusterGeo = otherGeo.union(clusterGeo);
			}
			else {
				clusterGeo = clusterGeo.union(otherGeo);
			}
		}
		catch (TopologyException ex) {

			LOGGER.error(
					"Union failed due to non-simple geometries",
					ex);
			clusterGeo = connectGeometryTool.createHullFromGeometry(
					clusterGeo,
					Arrays.asList(otherGeo.getCoordinates()),
					false);
		}
	}

	protected void mergeIfPossible(
			final boolean deleteNonLinks ) {
		if (this.size() < this.mergeSize || this.linkedClusters.size() == 0) return;

		final Set<Cluster<ClusterItem>> readyClusters = new HashSet<Cluster<ClusterItem>>();

		readyClusters.add(this);
		buildClusterLists(
				readyClusters,
				this,
				deleteNonLinks);
		if (readyClusters.size() == 1) return;

		readyClusters.remove(this);
		final Iterator<Cluster<ClusterItem>> finishedIt = readyClusters.iterator();
		Cluster<ClusterItem> top = this;
		while (finishedIt.hasNext()) {
			top.merge(finishedIt.next());
		}
	}

	private void buildClusterLists(
			final Set<Cluster<ClusterItem>> readyClusters,
			final DBScanClusterList cluster,
			final boolean deleteNonLinks ) {
		final Iterator<ByteArrayId> linkedClusterIt = cluster.getLinkedClusters();
		while (linkedClusterIt.hasNext()) {
			boolean merged = false;
			final ByteArrayId linkedClusterId = linkedClusterIt.next();
			final Cluster<ClusterItem> linkedCluster = index.get(linkedClusterId);
			if (linkedCluster != null) {
				if (linkedCluster.size() >= this.mergeSize) {
					if (readyClusters.add(linkedCluster)) {
						buildClusterLists(
								readyClusters,
								(DBScanClusterList) linkedCluster,
								false);
						merged = true;
					}
				}
			}
			if (deleteNonLinks || merged) linkedClusterIt.remove();
		}
	}

	@Override
	public String toString() {
		return "DBScanClusterList [clusterGeo=" + (clusterGeo == null ? "null" : clusterGeo.toString()) + ", ids=" + ids + "]";
	}

}
