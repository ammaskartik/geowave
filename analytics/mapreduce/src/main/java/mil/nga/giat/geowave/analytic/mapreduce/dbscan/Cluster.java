package mil.nga.giat.geowave.analytic.mapreduce.dbscan;

import java.util.Iterator;

import mil.nga.giat.geowave.analytic.nn.NeighborList;
import mil.nga.giat.geowave.core.index.ByteArrayId;

public interface Cluster<NNTYPE> extends
		NeighborList<NNTYPE>
{
	public void merge(
			Cluster<NNTYPE> cluster );

	public ByteArrayId getId();

	/*
	 * Return the cluster to which this cluster is linked
	 */
	public Iterator<ByteArrayId> getLinkedClusters();

	public int currentLinkSetSize();

	public void invalidate();

	public void finish();

	public boolean isFinished();

	public boolean isCompressed();

}
