package edu.buffalo.www.cse4562.evaluator;

import java.util.List;

import edu.buffalo.www.cse4562.TableSchema;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class evalOperator extends Eval {
	// The current row that needs to be evaluated.
	private Object[] currentRow;
	private TableSchema tableSchema;

	// Constructor function to set the current row read from the CSV file/data
	// source.
	public evalOperator(Object[] row, TableSchema tableSchema) {
		this.currentRow = row;
		this.tableSchema = tableSchema;
	}

	@Override
	public PrimitiveValue eval(Column col) {
		int colIndex = -1;
		List<ColumnDefinition> test = (tableSchema.getTabColumns());
		for(int i = 0; i < test.size(); i++) {
			String schemaName = test.get(i).getColumnName().toString();
			String argumentName = col.getColumnName().toString();
			if(schemaName.equals(argumentName)) {
				// Grab the index at which the column definition is stored in the table schema.
				colIndex = i;
			}
		}
		// Grab the value of the required column with the help of index and the
		// currently read row. Return whilst type casting to the PrimitiveValue
		// data type.
		return (PrimitiveValue) currentRow[colIndex];
	}
}