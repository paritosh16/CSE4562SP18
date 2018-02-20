package edu.buffalo.www.cse4562.operator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import edu.buffalo.www.cse4562.Main;
import edu.buffalo.www.cse4562.TableSchema;

/* Imported Libraries specific to scan*/


public class ScanOperator implements Iterator<BaseOperator>{
	/* Overloading the constructor for the method*/
	TableSchema tabSchema;
	BufferedReader br ;
	Boolean oFlag = false;
	String line = "";
	BaseOperator record ;
	public ScanOperator()
	{
		/* Default constructor : just assigns the memory*/
	}

	/* Overloaded Constructor
	 * The constructor gets the schema and sets the value of the Buffered reader*
	 * Opens the buffered reader and opens the connection*/
	public ScanOperator(String dataPath) throws IOException
	{
		String path = "./" + dataPath + ".csv";
		try(
				BufferedReader brVal = new BufferedReader(new FileReader(path));
				)
				{
			/* Assigning the value to the Iterator and  records*/
			this.br = brVal;
			if(br != null)
			{
				this.oFlag = true;
			}
			this.tabSchema = Main.dataObjects.get(dataPath);
			this.record = new BaseOperator(tabSchema.getTabColumns().size());

				}
	}

	public TableSchema getTabSchema() {
		return tabSchema;
	}

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
	public BaseOperator next() {
		if (this.line != null)
		{
			String[] tempRecord = line.split("|");
			for(int i = 0; i < this.tabSchema.getTabColumns().size();i++)
			{
				ColumnDefinition tempColumn = this.tabSchema.getTabColumns().get(i);
				if (tempColumn.getColDataType().toString() == "int")
				{
					this.record.rowRecord[i] = new Long(tempRecord[2*i]);
				}
				else if (tempColumn.getColDataType().toString() == "char")
				{
					this.record.rowRecord[i] = new String(tempRecord[2*i]);
				}
				else if (tempColumn.getColDataType().toString() == "varchar")
				{
					this.record.rowRecord[i] = new String(tempRecord[2*i]);
				}
				else if (tempColumn.getColDataType().toString() == "string")
				{
					this.record.rowRecord[i] = new String(tempRecord[2*i]);
				}
				else if (tempColumn.getColDataType().toString() == "decimal")
				{
					this.record.rowRecord[i] = new Double(tempRecord[2*i]);
				}
				else if (tempColumn.getColDataType().toString() == "date")
				{
					this.record.rowRecord[i] = new Date(tempRecord[2*i]);
				}
				else
				{
					System.out.println("Unsupported Data type");
				}

				//this.record[i] = new Integer(arg0)


			}
		}
		return record;
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
