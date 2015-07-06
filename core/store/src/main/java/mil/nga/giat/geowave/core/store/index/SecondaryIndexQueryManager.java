package mil.nga.giat.geowave.core.store.index;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.query.BasicQuery;

/**
 * Manages query the secondary indices given a query. Eventually is replaced by
 * a CBO!
 * 
 * 
 * @param <T>
 *            The type of entity being indexed
 */
public class SecondaryIndexQueryManager
{
	final SecondaryIndexDataStore secondaryIndexDataStore;

	public SecondaryIndexQueryManager(
			final SecondaryIndexDataStore secondaryIndexDataStore ) {
		this.secondaryIndexDataStore = secondaryIndexDataStore;
	}

	/**
	 * Query across primary indices
	 * 
	 * @param query
	 * @param visibility
	 * @return association between primary index ID and the ranges associated
	 *         with that index
	 * @throws IOException
	 */
	public CloseableIterator<Map<ByteArrayId, List<ByteArrayRange>>> query(
			final BasicQuery query,
			final SecondaryIndex index,
			final String... visibility )
			throws IOException {
		if (query.isSupported(index)) {
			return secondaryIndexDataStore.query(
					new ByteArrayId(
							StringUtils.stringToBinary(index.getIndexStrategy().getId())),
					query.getSecondaryIndexConstraints(index),
					query.getSecondaryQueryFilter(index),
					visibility);
		}
		return new CloseableIterator.Empty<Map<ByteArrayId, List<ByteArrayRange>>>();
	}

}
