package mil.nga.giat.geowave.datastore.accumulo;

import java.util.Arrays;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;
import mil.nga.giat.geowave.core.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.EmptyStatisticVisibility;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatisticalDataAdapter;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class DataAdapterStatsWrapper<T> implements
		StatisticalDataAdapter<T>
{

	final DataAdapter<T> adapter;
	final PrimaryIndex index;

	public DataAdapterStatsWrapper(
			final PrimaryIndex index,
			DataAdapter<T> adapter ) {
		super();
		this.index = index;
		this.adapter = adapter;
	}

	@Override
	public ByteArrayId getAdapterId() {
		return adapter.getAdapterId();
	}

	@Override
	public boolean isSupported(
			T entry ) {
		return adapter.isSupported(entry);
	}

	@Override
	public ByteArrayId getDataId(
			T entry ) {
		return adapter.getDataId(entry);
	}

	@Override
	public T decode(
			IndexedAdapterPersistenceEncoding data,
			PrimaryIndex index ) {
		return adapter.decode(
				data,
				index);
	}

	@Override
	public AdapterPersistenceEncoding encode(
			T entry,
			CommonIndexModel indexModel ) {
		return adapter.encode(
				entry,
				indexModel);
	}

	@Override
	public FieldReader<Object> getReader(
			ByteArrayId fieldId ) {
		return adapter.getReader(fieldId);
	}

	@Override
	public byte[] toBinary() {
		return adapter.toBinary();
	}

	@Override
	public void fromBinary(
			byte[] bytes ) {
		adapter.fromBinary(bytes);
	}

	@Override
	public FieldWriter<T, Object> getWriter(
			ByteArrayId fieldId ) {
		return (adapter instanceof WritableDataAdapter) ? ((WritableDataAdapter<T>) adapter).getWriter(fieldId) : null;
	}

	@Override
	public ByteArrayId[] getSupportedStatisticsIds() {
		final ByteArrayId[] idsFromAdapter = (adapter instanceof StatisticalDataAdapter) ? ((StatisticalDataAdapter) adapter).getSupportedStatisticsIds() : new ByteArrayId[0];
		final ByteArrayId[] newSet = Arrays.copyOf(
				idsFromAdapter,
				idsFromAdapter.length + 2);
		newSet[idsFromAdapter.length] = RowRangeDataStatistics.STATS_ID;
		newSet[idsFromAdapter.length + 1] = RowRangeHistogramStatistics.STATS_ID;
		return newSet;
	}

	@Override
	public DataStatistics<T> createDataStatistics(
			ByteArrayId statisticsId ) {
		if (statisticsId.equals(RowRangeDataStatistics.STATS_ID)) return new RowRangeDataStatistics(
				index.getId());
		if (statisticsId.equals(RowRangeHistogramStatistics.STATS_ID)) return new RowRangeHistogramStatistics(
				adapter.getAdapterId(),
				index.getId(),
				1024);
		return (adapter instanceof StatisticalDataAdapter) ? ((StatisticalDataAdapter) adapter).createDataStatistics(statisticsId) : null;
	}

	@Override
	public EntryVisibilityHandler<T> getVisibilityHandler(
			ByteArrayId statisticsId ) {
		return (adapter instanceof StatisticalDataAdapter) ? ((StatisticalDataAdapter) adapter).getVisibilityHandler(statisticsId) : new EmptyStatisticVisibility<T>();
	}

	public DataAdapter<T> getAdapter() {
		return adapter;
	}

}
