package mil.nga.giat.geowave.core.store.index.temporal;

import java.util.Date;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.index.FilterableConstraints;

public abstract class TemporalQueryConstraint implements
		FilterableConstraints
{
	protected final ByteArrayId fieldId;
	protected final Date start;
	protected final Date end;
	protected final boolean rangeInclusive;

	public TemporalQueryConstraint(
			final ByteArrayId fieldId,
			final Date start,
			final Date end ) {
		this(
				fieldId,
				start,
				end,
				false);
	}

	public TemporalQueryConstraint(
			final ByteArrayId fieldId,
			final Date start,
			final Date end,
			final boolean rangeInclusive ) {
		super();
		this.fieldId = fieldId;
		this.start = start;
		this.end = end;
		this.rangeInclusive = rangeInclusive;
	}

	@Override
	public int getDimensionCount() {
		return 1;
	}

	@Override
	public boolean isEmpty() {
		return false;
	}

	public abstract List<ByteArrayRange> getRange();

}
