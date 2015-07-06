package mil.nga.giat.geowave.datastore.accumulo.index.secondary;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.SecondaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.Closable;
import mil.nga.giat.geowave.datastore.accumulo.Writer;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.log4j.Logger;

public class AccumuloSecondaryIndexDataStore implements
		SecondaryIndexDataStore,
		Closable
{
	private final static Logger LOGGER = Logger.getLogger(AccumuloSecondaryIndexDataStore.class);
	private static final String TABLE_PREFIX = "GEOWAVE_2ND_IDX_";
	private final AccumuloOperations accumuloOperations;
	private final Map<String, Writer> writerCache = new HashMap<>();

	public AccumuloSecondaryIndexDataStore(
			final AccumuloOperations accumuloOperations ) {
		super();
		this.accumuloOperations = accumuloOperations;
	}

	private Writer getWriter(
			final String secondaryIndexName ) {
		if (writerCache.containsKey(secondaryIndexName)) {
			return writerCache.get(secondaryIndexName);
		}
		Writer writer = null;
		try {
			writer = accumuloOperations.createWriter(
					TABLE_PREFIX + secondaryIndexName,
					true,
					false);
			writerCache.put(
					secondaryIndexName,
					writer);
		}
		catch (final TableNotFoundException e) {
			LOGGER.error(
					"Error creating writer",
					e);
		}
		return writer;
	}

	@Override
	public void store(
			final SecondaryIndex<?> secondaryIndex,
			final ByteArrayId dataLocationID,
			final List<ByteArrayId> dataRowIds,
			final List<FieldInfo<?>> attributeInfos ) {
		final Writer writer = getWriter(secondaryIndex.getIndexStrategy().getId());
		if (writer != null) {
			final ByteArrayId firstPrimaryIdxRowId = dataRowIds.get(0);
			ColumnVisibility columnVisibility = null;
			for (final FieldInfo<?> fieldInfo : attributeInfos) {
				@SuppressWarnings("unchecked")
				final List<ByteArrayId> ranges = secondaryIndex.getIndexStrategy().getInsertionIds(
						Arrays.asList(fieldInfo));
				Mutation m = null;
				for (ByteArrayId range : ranges) {
					m = new Mutation(
							range.getBytes());
					columnVisibility = new ColumnVisibility(
							fieldInfo.getVisibility());
					m.put(
							secondaryIndex.getId().getBytes(),
							fieldInfo.getDataValue().getId().getBytes(),
							columnVisibility,
							fieldInfo.getWrittenValue());
					m.put(
							secondaryIndex.getId().getBytes(),
							dataLocationID.getBytes(),
							columnVisibility,
							firstPrimaryIdxRowId.getBytes());
					writer.write(m);
				}
			}
		}
	}

	@Override
	public void remove(
			final ByteArrayId indexID,
			final ByteArrayId dataId,
			final List<ByteArrayId> ranges,
			final ByteArrayId visibility,
			final ByteArrayId dataLocationID,
			final List<ByteArrayId> dataRowIds,
			final List<FieldInfo<?>> attributeInfos ) {
		// TODO Auto-generated method stub
	}

	@Override
	public CloseableIterator<Map<ByteArrayId, List<ByteArrayRange>>> query(
			final ByteArrayId indexID,
			final List<ByteArrayRange> ranges,
			final List<QueryFilter> constraints,
			final String... visibility ) {
		// TODO
		return null;
	}

	@Override
	public void close() {
		for (final Writer writer : writerCache.values()) {
			writer.close();
		}
	}
}
