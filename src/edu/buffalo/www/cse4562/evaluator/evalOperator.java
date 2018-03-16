package edu.buffalo.www.cse4562.evaluator;

import java.util.ArrayList;
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
	// Reference table name list for join conditions.
	private List<String> referenceTableName = new ArrayList<String>(10);

	// Constructor function to set the current row read from the CSV file/data
	// source.
	public evalOperator(Object[] row, TableSchema tableSchema, List<String> refList) {
		this.currentRow = row;
		this.tableSchema = tableSchema;
		this.referenceTableName = refList;
	}

	@Override
	public PrimitiveValue eval(Column col) {
		int colIndex = -1;
		String argColumnName = col.getColumnName().toString().toUpperCase();
		String tableName = col.getTable().getName();
		List<ColumnDefinition> columnList = (tableSchema.getTabColumns());
		String schemaColName;
		for (int i = 0; i < columnList.size(); i++) {
			schemaColName = columnList.get(i).getColumnName().toString().toUpperCase();
			if(tableName == null) {
				// Grab the first ever column name because table name is not specified explicitly.
				if (schemaColName.equals(argColumnName)) {
					// Grab the index at which the column definition is stored in the table schema.
					// colIndex = i;
					return (PrimitiveValue) this.currentRow[i];
				}
			} else {
				// Need to check for the column name along with the table name.
				if (schemaColName.equals(argColumnName) && referenceTableName.get(i).equals(tableName)) {
					// Grab the index at which the column definition is stored in the table schema.
					// colIndex = i;
					return (PrimitiveValue) this.currentRow[i];
				}
			}
		}
		return (PrimitiveValue) this.currentRow[colIndex];
	}
}