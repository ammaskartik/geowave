package mil.nga.giat.geowave.core.store.index.numeric;

import java.util.Collections;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;

public class FilterableLessThanOrEqualToConstraint extends
		NumericQueryConstraint
{

	public FilterableLessThanOrEqualToConstraint(
			final ByteArrayId fieldId,
			final Number number ) {
		super(
				fieldId,
				number);
	}

	@Override
	public QueryFilter getFilter() {
		return new QueryFilter() {
			@Override
			public boolean accept(
					final IndexedPersistenceEncoding<?> persistenceEncoding ) {
				final Number value = (Number) persistenceEncoding.getCommonData().getValue(
						fieldId);
				return value.doubleValue() <= number.doubleValue();
			}
		};
	}

	@Override
	public List<ByteArrayRange> getRange() {
		return Collections.singletonList(new ByteArrayRange(
				new ByteArrayId(
						NumericIndexStrategy.toIndexByte(Double.MIN_VALUE)),
				new ByteArrayId(
						NumericIndexStrategy.toIndexByte(number.doubleValue()))));
	}

}
