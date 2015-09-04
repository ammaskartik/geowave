package mil.nga.giat.geowave.analytic.mapreduce.dbscan;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.analytic.nn.DistanceProfile;
import mil.nga.giat.geowave.analytic.nn.NeighborList;
import mil.nga.giat.geowave.analytic.nn.NeighborListFactory;
import mil.nga.giat.geowave.core.index.ByteArrayId;

public class ClusterNeighborList<NNTYPE> implements
		NeighborList<NNTYPE>
{
	private final ByteArrayId id;
	final Map<ByteArrayId, Cluster<NNTYPE>> index;

	public ClusterNeighborList(
			final ByteArrayId centerId,
			final NNTYPE center,
			final NeighborListFactory<NNTYPE> factory,
			final Map<ByteArrayId, Cluster<NNTYPE>> index ) {
		super();
		this.index = index;
		this.id = centerId;
		Cluster<NNTYPE> cluster = getCluster();
		if (cluster == null) {
			cluster = (Cluster<NNTYPE>) factory.buildNeighborList(
					id,
					center);
			index.put(
					id,
					cluster);
		}
	}

	public Cluster<NNTYPE> getCluster() {
		return index.get(id);
	}

	@Override
	public Iterator<Entry<ByteArrayId, NNTYPE>> iterator() {
		return getCluster().iterator();
	}

	@Override
	public boolean add(
			DistanceProfile<?> distanceProfile,
			final ByteArrayId id,
			final NNTYPE value ) {
		return getCluster().add(
				distanceProfile,
				id,
				value);
	}

	@Override
	public InferType infer(
			final ByteArrayId id,
			final NNTYPE value ) {
		return getCluster().infer(
				id,
				value);
	}

	@Override
	public void clear() {
		getCluster().clear();

	}

	@Override
	public int size() {
		return getCluster().size();
	}

	@Override
	public boolean isEmpty() {
		return getCluster().isEmpty();
	}

	public static class ClusterNeighborListFactory<NNTYPE> implements
			NeighborListFactory<NNTYPE>
	{
		final Map<ByteArrayId, Cluster<NNTYPE>> index;
		final NeighborListFactory<NNTYPE> factory;

		public ClusterNeighborListFactory(
				NeighborListFactory<NNTYPE> factory,
				Map<ByteArrayId, Cluster<NNTYPE>> index ) {
			super();
			this.index = index;
			this.factory = factory;
		}

		@Override
		public NeighborList<NNTYPE> buildNeighborList(
				ByteArrayId centerId,
				NNTYPE center ) {
			return new ClusterNeighborList(
					centerId,
					center,
					factory,
					index);
		}
	}
}
