package edu.buffalo.www.cse4562;

public class RecordLocation {

	public long offset;
	public long limit;

	@Override
	public String toString() {
		return "RecordLocation [offset=" + offset + ", limit=" + limit + "]";
	}

	/**
	 * @param offset
	 * @param limit
	 */
	public RecordLocation(long offset, long limit) {
		super();
		this.offset = offset;
		this.limit = limit;
	}
	public long getOffset() {
		return offset;
	}
	public long getLimit() {
		return limit;
	}
}
