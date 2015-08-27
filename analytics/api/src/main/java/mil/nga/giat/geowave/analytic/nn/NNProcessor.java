package mil.nga.giat.geowave.analytic.nn;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import mil.nga.giat.geowave.analytic.nn.NeighborList.InferType;
import mil.nga.giat.geowave.analytic.partitioner.Partitioner;
import mil.nga.giat.geowave.analytic.partitioner.Partitioner.PartitionData;
import mil.nga.giat.geowave.analytic.partitioner.Partitioner.PartitionDataCallback;
import mil.nga.giat.geowave.core.index.ByteArrayId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NNProcessor<PARTITION_VALUE, STORE_VALUE>
{
	protected static final Logger LOGGER = LoggerFactory.getLogger(NNProcessor.class);

	final Map<PartitionData, PartitionData> uniqueSetOfPartitions = new HashMap<PartitionData, PartitionData>();
	final Map<PartitionData, Set<ByteArrayId>> partitionsToIds = new HashMap<PartitionData, Set<ByteArrayId>>();
	final Map<ByteArrayId, Set<PartitionData>> idsToPartition = new HashMap<ByteArrayId, Set<PartitionData>>();
	final Map<ByteArrayId, STORE_VALUE> primaries = new HashMap<ByteArrayId, STORE_VALUE>();
	final Map<ByteArrayId, STORE_VALUE> others = new HashMap<ByteArrayId, STORE_VALUE>();

	protected final Partitioner<Object> partitioner;
	protected final TypeConverter<STORE_VALUE> typeConverter;

	protected final DistanceProfileGenerateFn<?, STORE_VALUE> distanceProfileFn;
	protected final double maxDistance;
	protected final PartitionData parentPartition;

	/**
	 * Run State
	 */
	protected ByteArrayId startingPoint;
	protected NeighborIndex<STORE_VALUE> index;

	public NNProcessor(
			Partitioner<Object> partitioner,
			TypeConverter<STORE_VALUE> typeConverter,
			DistanceProfileGenerateFn<?, STORE_VALUE> distanceProfileFn,
			double maxDistance,
			PartitionData parentPartition ) {
		super();
		this.partitioner = partitioner;
		this.typeConverter = typeConverter;
		this.distanceProfileFn = distanceProfileFn;
		this.maxDistance = maxDistance;
		this.parentPartition = parentPartition;
	}

	private PartitionData add(
			final PartitionData pd,
			final ByteArrayId itemId ) {
		PartitionData singleton = uniqueSetOfPartitions.get(pd);
		if (singleton == null) {
			uniqueSetOfPartitions.put(
					pd,
					pd);
			singleton = pd;
		}

		Set<ByteArrayId> idsSet = partitionsToIds.get(singleton);
		if (idsSet == null) {
			idsSet = new HashSet<ByteArrayId>();
			partitionsToIds.put(
					singleton,
					idsSet);
		}
		idsSet.add(itemId);

		Set<PartitionData> partitionSet = idsToPartition.get(itemId);
		if (partitionSet == null) {
			partitionSet = new HashSet<PartitionData>();
			idsToPartition.put(
					itemId,
					partitionSet);
		}
		partitionSet.add(singleton);

		return singleton;
	}

	public void remove(
			final ByteArrayId id ) {

		final Set<PartitionData> partitionSet = idsToPartition.remove(id);
		if (partitionSet != null) {
			for (PartitionData pd : partitionSet) {
				final Set<ByteArrayId> idSet = partitionsToIds.get(pd);
				if (idSet != null) idSet.remove(id);
			}
		}
		primaries.remove(id);
		others.remove(id);
		if (index != null) {
			index.empty(id);
		}
	}

	public void add(
			final ByteArrayId id,
			final boolean isPrimary,
			final PARTITION_VALUE partitionValue )
			throws IOException {

		STORE_VALUE value = this.typeConverter.convert(
				id,
				partitionValue);

		if (isPrimary)
			primaries.put(
					id,
					value);
		else
			others.put(
					id,
					value);

		try {
			partitioner.partition(
					partitionValue,
					new PartitionDataCallback() {

						@Override
						public void partitionWith(
								final PartitionData partitionData )
								throws Exception {
							PartitionData singleton = add(
									partitionData,
									id);
							singleton.setPrimary(partitionData.isPrimary() | singleton.isPrimary());
						}
					});

		}
		catch (Exception e) {
			throw new IOException(
					e);
		}

		if (isPrimary) {
			if (startingPoint == null) startingPoint = id;
		}
	}

	public interface CompleteNotifier<STORE_VALUE>
	{
		public void complete(
				ByteArrayId id,
				STORE_VALUE value,
				NeighborList<STORE_VALUE> list )
				throws IOException,
				InterruptedException;
	}

	public void process(
			NeighborListFactory<STORE_VALUE> listFactory,
			final CompleteNotifier<STORE_VALUE> notification )
			throws IOException,
			InterruptedException {

		LOGGER.info("Processing " + parentPartition.toString() + " with primary = " + primaries.size() + " and other = " + others.size());
		LOGGER.info("Processing " + parentPartition.toString() + " with sub-partitions = " + uniqueSetOfPartitions.size());

		index = new NeighborIndex<STORE_VALUE>(
				listFactory);

		double farthestDistance = 0;
		ByteArrayId farthestNeighbor = null;
		ByteArrayId nextStart = startingPoint;
		final Set<ByteArrayId> inspectionSet = new HashSet<ByteArrayId>();
		inspectionSet.addAll(primaries.keySet());

		if (inspectionSet.size() > 0 && nextStart == null) {
			nextStart = inspectionSet.iterator().next();
		}

		while (nextStart != null) {
			inspectionSet.remove(nextStart);
			farthestDistance = 0;
			final Set<PartitionData> partition = idsToPartition.get(nextStart);
			final STORE_VALUE primary = primaries.get(nextStart);
			final ByteArrayId primaryId = nextStart;
			nextStart = null;
			farthestNeighbor = null;
			if (LOGGER.isTraceEnabled()) LOGGER.trace("processing " + primaryId);
			if (primary == null) {
				if (inspectionSet.size() > 0) {
					nextStart = inspectionSet.iterator().next();
				}
				continue;
			}
			final NeighborList<STORE_VALUE> primaryList = index.init(
					primaryId,
					primary);

			for (PartitionData pd : partition) {
				for (ByteArrayId neighborId : partitionsToIds.get(pd)) {
					if (neighborId.equals(primaryId)) continue;
					boolean isAPrimary = true;
					STORE_VALUE neighbor = primaries.get(neighborId);
					if (neighbor == null) {
						neighbor = others.get(neighborId);
						isAPrimary = false;
					}
					else // prior processed primary
					if (!inspectionSet.contains(neighborId)) continue;

					if (neighbor == null) continue;
					final InferType inferResult = primaryList.infer(
							neighborId,
							neighbor);
					if (inferResult == InferType.NONE) {
						final DistanceProfile<?> distanceProfile = distanceProfileFn.computeProfile(
								primary,
								neighbor);
						final double distance = distanceProfile.getDistance();
						if (distance <= maxDistance) {
							index.add(
									distanceProfile,
									primaryId,
									primary,
									neighborId,
									neighbor,
									isAPrimary);
							if (LOGGER.isTraceEnabled()) LOGGER.trace("Neighbor " + neighborId);
						}
						if (distance > farthestDistance && inspectionSet.contains(neighborId)) {
							farthestDistance = distance;
							farthestNeighbor = neighborId;
						}
					}
					else if (inferResult == InferType.REMOVE) {
						inspectionSet.remove(neighborId);
					}
				}
			}
			notification.complete(
					primaryId,
					primary,
					primaryList);
			index.empty(primaryId);
			if (farthestNeighbor == null && inspectionSet.size() > 0) {
				nextStart = inspectionSet.iterator().next();
			}
			else {
				nextStart = farthestNeighbor;
			}
		}

	}
}
