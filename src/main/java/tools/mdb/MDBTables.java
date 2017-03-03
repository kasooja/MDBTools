package tools.mdb;


import java.io.File;
import java.io.IOException;
import java.util.Set;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;

public class MDBTables {

	public static void main(String[] args) {
		//String file = "/home/kat/git/wk_health_analysis_kat_py_lat/data/sources/CMS/sample/lcd/Articles/20161129/all_article.mdb";
		String file = "all_article.mdb";

		try {
			Database db = DatabaseBuilder.open(new File(file));

			Set<String> tableNames = db.getTableNames();

			for(String tableName : tableNames){
				System.out.println(tableName);

			}

		} catch (IOException e1) {
			e1.printStackTrace();
		}		

	}

}
