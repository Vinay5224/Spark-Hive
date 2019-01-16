import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.datanucleus.query.evaluator.memory.GetClassMethodEvaluator;

public class demo {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		String path = System.getProperty("user.dir")+"/migration.properties";
		InputStream in = new FileInputStream(new File(path));
		Properties properties = new Properties();
		properties.load(in);
		
		System.out.println(properties.getProperty("JDBC.source.databasename"));

	}

}
