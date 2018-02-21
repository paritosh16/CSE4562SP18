package edu.buffalo.www.cse4562.operator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

import edu.buffalo.www.cse4562.TableSchema;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

/* Imported Libraries specific to scan*/


public class ScanOperator extends BaseOperator implements Iterator<Object[]>{
	/* Overloading the constructor for the method*/
	TableSchema tabSchema;
	BufferedReader br ;
	Boolean oFlag = false;
	String line = "";
	Object[] record;

	/* Overloaded Constructor
	 * The constructor gets the schema and sets the value of the Buffered reader*
	 * Opens the buffered reader and opens the connection*/
	public ScanOperator(String tableName, TableSchema tableSchema) throws IOException
	{
		// FIXME: ensure file exists
		String path = "./" + tableName + ".csv";

		BufferedReader reader = new BufferedReader(new FileReader(path));
		{
			/* Assigning the value to the Iterator and records*/
			this.br = reader;
			if(br != null)
			{
				System.out.println("read opened correct");
				// TODO: see if flag setting is necessary or file handle has
				// a method to check if its open
				this.oFlag = true;
			}
			this.tabSchema = tableSchema;
			this.record = new Object[tabSchema.getTabColumns().size()];

		}
	}

	public TableSchema getTabSchema() {
		return tabSchema;
	}

	// TODO: remove method later if no callers
	public void setTabSchema(TableSchema tabSchema) {
		this.tabSchema = tabSchema;
	}


	@Override
	public boolean hasNext() {
		try {
			return (this.line = br.readLine()) != null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			return false;
		}
	}

	@Override
	/* This function gets the next line */
	public Object[] next() {
		if (this.line != null)
		{
			String[] tempRecord = line.split("\\|");
			for(int i = 0; i < this.tabSchema.getTabColumns().size();i++)
			{
				ColumnDefinition tempColumn = this.tabSchema.getTabColumns().get(i);

				String columnType = tempColumn.getColDataType().toString();
				if (columnType.equals("int"))
				{
					this.record[i] = new LongValue(tempRecord[i]);
				}
				else if (columnType.equals("char"))
				{
					this.record[i] = new StringValue(tempRecord[i]);
				}
				else if (columnType.equals("varchar"))
				{
					this.record[i] = new StringValue(tempRecord[i]);
				}
				else if (columnType.equals("string"))
				{
					this.record[i] = new StringValue(tempRecord[i]);
				}
				else if (columnType.equals("decimal"))
				{
					this.record[i] = new DoubleValue(tempRecord[i]);
				}
				else if (columnType.equals("date"))
				{
					this.record[i] = new DateValue(tempRecord[i]);
				}
				else
				{
					System.out.println("Unsupported Data type");
				}
			}
		}
		return this.record;
	}

	public Boolean close() throws IOException
	{
		if (!oFlag)
		{
			return false;
		}

		this.oFlag = false;
		this.br.close();
		return true;
	}
}
