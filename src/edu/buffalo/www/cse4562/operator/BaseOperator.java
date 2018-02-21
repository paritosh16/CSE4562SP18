package edu.buffalo.www.cse4562.operator;

import java.util.Iterator;

// TODO make BaseOperator an abstract class if possible
public class BaseOperator implements Iterator<Object[]> {
	Object [] rowRecord;
	BaseOperator childOperator;

	public BaseOperator()
	{
		// Does nothing : just assigns the memory
	}
	/* Constructor initializes the size of the rowRecord which would be typecasted*/
	public BaseOperator(int rowSize)
	{
		rowRecord = new Object[rowSize];
		childOperator = null;
	}

	@Override
	public boolean hasNext() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object[] next() {
		throw new UnsupportedOperationException();
	}

	/**
	 * called by parser during operator chain creation.. whenever an alias is encountered
	 * @param string
	 */
	public void setAlias(String string) {
		// TODO concrete implementation in child classes
		throw new UnsupportedOperationException();
	}

}
