package mil.nga.giat.geowave.core.store.index.numeric;

import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.index.FilterableConstraints;

public abstract class NumericQueryConstraint implements
		FilterableConstraints
{
	protected final ByteArrayId fieldId;
	protected final Number number;

	public NumericQueryConstraint(
			final ByteArrayId fieldId,
			final Number number ) {
		super();
		this.fieldId = fieldId;
		this.number = number;
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
