package mil.nga.giat.geowave.datastore.accumulo;

import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public interface AttachedIteratorDataAdapter<T> extends
		WritableDataAdapter<T>
{
	public static final String ATTACHED_ITERATOR_CACHE_ID = "AttachedIterators";

	public IteratorConfig[] getAttachedIteratorConfig(
			final PrimaryIndex index );
}
