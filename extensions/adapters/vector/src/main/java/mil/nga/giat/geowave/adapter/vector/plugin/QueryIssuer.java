package mil.nga.giat.geowave.adapter.vector.plugin;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.Query;

import org.opengis.feature.simple.SimpleFeature;

public interface QueryIssuer
{
	CloseableIterator<SimpleFeature> query(
			PrimaryIndex index,
			Query constraints );
}
