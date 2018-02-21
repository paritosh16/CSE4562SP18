package edu.buffalo.www.cse4562.operator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

import edu.buffalo.www.cse4562.TableSchema;
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
		// FIXME: rename dataPath to tableName
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
			//			this.tabSchema = tableSchema;
			// FIXME: refactor BaseOperator creation and move out buffer creation to Row class
			//			this.record = new BaseOperator(tabSchema.getTabColumns().size());
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

	@SuppressWarnings("deprecation")
	@Override
	/* This function gets the next line */
	public Object[] next() {
		if (this.line != null)
		{
			System.out.println(this.line);
			String[] tempRecord = line.split("|");
			//			System.out.println(record[])
			for(int i = 0; i < this.tabSchema.getTabColumns().size();i++)
			{
				ColumnDefinition tempColumn = this.tabSchema.getTabColumns().get(i);
				// FIXME: (tempColumn.getColDataType().toString() to String colDataType
				if (tempColumn.getColDataType().toString() == "int")
				{
					// FIXME: cast to net.sf.jsqlparser.expression.PrimitiveValue types
					this.record[i] = new Long(tempRecord[2*i]);
				}
				else if (tempColumn.getColDataType().toString() == "char")
				{
					this.record[i] = new String(tempRecord[2*i]);
				}
				else if (tempColumn.getColDataType().toString() == "varchar")
				{
					this.record[i] = new String(tempRecord[2*i]);
				}
				else if (tempColumn.getColDataType().toString() == "string")
				{
					this.record[i] = new String(tempRecord[2*i]);
				}
				else if (tempColumn.getColDataType().toString() == "decimal")
				{
					this.record[i] = new Double(tempRecord[2*i]);
				}
				else if (tempColumn.getColDataType().toString() == "date")
				{
					this.record[i] = new Date(tempRecord[2*i]);
				}
				else
				{
					System.out.println("Unsupported Data type");
				}
			}
		}
		System.out.println(this.record[0]);
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


	/* getSchema and get Alias */
}
