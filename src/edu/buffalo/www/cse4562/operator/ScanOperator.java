package edu.buffalo.www.cse4562.operator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import edu.buffalo.www.cse4562.TableSchema;


public class ScanOperator extends BaseOperator implements Iterator<Object[]>{
	BufferedReader br;
	Boolean oFlag = false;
	String line = "";
	Object[] record;

	/* Overloaded Constructor
	 * The constructor gets the schema and sets the value of the Buffered reader*
	 * Opens the buffered reader and opens the connection*/
	public ScanOperator(BaseOperator childOperator, String tableName, TableSchema tableSchema) throws IOException
	{
		super(childOperator, tableSchema);

		// FIXME: ensure file exists
		String path = "./data/" + tableName + ".dat";

		BufferedReader reader = new BufferedReader(new FileReader(path));
		{
			/* Assigning the value to the Iterator and records*/
			this.br = reader;
			if(br != null)
			{
				// TODO: see if flag setting is necessary or file handle has
				// a method to check if its open
				this.oFlag = true;
			}
			this.record = new Object[this.getTableSchema().getTabColumns().size()];

		}
	}

	@Override
	public boolean hasNext() {
		try {
			boolean success =  (this.line = br.readLine()) != null;
			if (!success) {
				close();
			}
			return success;
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
			for(int i = 0; i < this.getTableSchema().getTabColumns().size();i++)
			{
				ColumnDefinition tempColumn = this.getTableSchema().getTabColumns().get(i);

				String columnType = tempColumn.getColDataType().toString();
				if (columnType.toLowerCase().equals("int"))
				{
					this.record[i] = new LongValue(tempRecord[i]);
				}
				else if (columnType.toLowerCase().equals("char"))
				{
					this.record[i] = new StringValue(tempRecord[i]);
				}
				else if (columnType.toLowerCase().equals("varchar"))
				{
					this.record[i] = new StringValue(tempRecord[i]);
				}
				else if (columnType.toLowerCase().equals("string"))
				{
					this.record[i] = new StringValue(tempRecord[i]);
				}
				else if (columnType.toLowerCase().equals("decimal"))
				{
					this.record[i] = new DoubleValue(tempRecord[i]);
				}
				else if (columnType.toLowerCase().equals("date"))
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

	@Override
	public void setAlias(String tabAlias) {

		super.getTableSchema().setTabAlias(tabAlias);
	}
}
