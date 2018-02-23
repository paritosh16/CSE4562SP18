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
	// The schema that should be referred while evaluating the current row.
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
		String argColumnName = col.getColumnName().toString().toUpperCase();
		List<ColumnDefinition> columnList = (tableSchema.getTabColumns());
		String schemaColName;
		for (int i = 0; i < columnList.size(); i++) {
			schemaColName = columnList.get(i).getColumnName().toString().toUpperCase();
			if (schemaColName.equals(argColumnName)) {
				// Grab the index at which the column definition is stored in the table schema.
				// colIndex = i;
				return (PrimitiveValue) this.currentRow[i];
			}
		}
		return (PrimitiveValue) this.currentRow[colIndex];
	}
}