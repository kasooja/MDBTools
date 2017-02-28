package tools.mdb;


import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Table;

import utils.BasicFileTools;

public class MDBSchema {

	private static String underscoreName (final String name) {
		return name.replace(" ", "_").toLowerCase();
	}
	/**
	 * Create an SQLite table for the corresponding MS Access table.
	 * 
	 * @param table MS Access table
	 * @param jdbc The SQLite database JDBC connection
	 * @throws SQLException 
	 */
	private static String createTable (final Table table) throws SQLException {    	
		final List<? extends Column> columns = table.getColumns();
		final StringBuilder stmtBuilder = new StringBuilder();
		if("contractor".equalsIgnoreCase(table.getName()))
			System.out.println("debug");
		/* Create the statement */
		System.out.println(table.getName());
		stmtBuilder.append("CREATE TABLE [" + underscoreName(table.getName()) + "] \n\t(\n");
		//stmtBuilder.append("CREATE TABLE " + table.getName() + " (");

		final int columnCount = columns.size();
		for (int i = 0; i < columnCount; i++) {
			final Column column = columns.get(i);
			
			// stmtBuilder.append(escapeIdentifier(column.getName()));
			stmtBuilder.append("\t\t[" + underscoreName(column.getName()) + "]\t\t\t");
			//stmtBuilder.append(" ");
			DataType type = column.getType();
			if("dmerc_rgn".equalsIgnoreCase(column.getName())){
				System.out.println("d");
						
			}
			switch (column.getType()) {
			/* Blob */
			case BINARY:
			case OLE:
				stmtBuilder.append("BLOB");
				break;

				/* Integers */
			case BOOLEAN:
			case BYTE:
			case INT:
			case LONG:
				stmtBuilder.append("Long INTEGER NOT NULL");
				break;

				/* Timestamp */
			case SHORT_DATE_TIME:
				// stmtBuilder.append("DATETIME");
				stmtBuilder.append("DateTime NOT NULL");
				break;

				/* Floating point */
			case DOUBLE:
			case FLOAT:
			case NUMERIC:
				stmtBuilder.append("DOUBLE PRECISION");
				break;

				/* Strings */
			case TEXT:
			case GUID:
			case MEMO:
				stmtBuilder.append("TEXT (" + column.getLength() + ")" + " NOT NULL" );
				break;

				/* Money -- This can't be floating point, so let's be safe with strings */
			case MONEY:
				stmtBuilder.append("TEXT");
				break;

			default:
				throw new SQLException("Unhandled MS Acess datatype: " + column.getType());
			}

			if (i + 1 < columnCount)
				stmtBuilder.append(",\n ");
		}
		stmtBuilder.append("\n);");
		return stmtBuilder.toString().trim();  	
	}

	public static void main(String[] args) {
		String file = "all_lmrp.mdb";
		StringBuilder schema  = new StringBuilder();
		String filePath = "my_schema.sql";

		try {
			Database db = DatabaseBuilder.open(new File(file));
	
			Set<String> tableNames = db.getTableNames();

			for(String tableName : tableNames){
				Table table = db.getTable(tableName);
				try {
					String tableSchemaString = createTable(table);
					schema.append(tableSchemaString);
					schema.append("\n\n");
					System.out.println(tableSchemaString);
				} catch (SQLException e) {
					e.printStackTrace();
				} 
			}
			BasicFileTools.writeFile(filePath, schema.toString().trim());
		} catch (IOException e1) {
			e1.printStackTrace();
		}		

	}

}
