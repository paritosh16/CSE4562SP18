package edu.buffalo.www.cse4562;

public class BaseOperator {
	Object [] rowRecord;

	public BaseOperator()
	{
		// Does nothing : just assigns the memory
	}
	/* Constructor initializes the size of the rowRecord which would be typecasted*/
	public BaseOperator(int rowSize)
	{
		rowRecord = new Object[rowSize];
	}
}
