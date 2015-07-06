package mil.nga.giat.geowave.core.store.index.temporal;

import java.util.Collections;
import java.util.Date;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;

public class FilterableDateRangeConstraint extends
		TemporalQueryConstraint
{

	public FilterableDateRangeConstraint(
			final ByteArrayId fieldId,
			final Date start,
			final Date end ) {
		super(
				fieldId,
				start,
				end);
	}

	public FilterableDateRangeConstraint(
			final ByteArrayId fieldId,
			final Date start,
			final Date end,
			final boolean rangeInclusive ) {
		super(
				fieldId,
				start,
				end,
				rangeInclusive);
	}

	@Override
	public QueryFilter getFilter() {
		return new QueryFilter() {
			@Override
			public boolean accept(
					final IndexedPersistenceEncoding<?> persistenceEncoding ) {
				final Date value = (Date) persistenceEncoding.getCommonData().getValue(
						fieldId);
				if (start != null) {
					if (rangeInclusive) if (value.compareTo(start) < 0)
						return false;
					else if (value.compareTo(start) <= 0) return false;
				}
				if (end != null) {
					if (rangeInclusive) if (value.compareTo(end) > 0)
						return false;
					else if (value.compareTo(end) >= 0) return false;
				}
				return true;
			}
		};
	}

	@Override
	public List<ByteArrayRange> getRange() {
		return Collections.singletonList(new ByteArrayRange(
				new ByteArrayId(
						TemporalIndexStrategy.toIndexByte(start)),
				new ByteArrayId(
						TemporalIndexStrategy.toIndexByte(end))));
	}

}
