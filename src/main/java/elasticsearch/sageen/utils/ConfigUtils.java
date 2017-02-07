package elasticsearch.sageen.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigUtils {

	/**
	 * This method reads and returns specified property from properties file
	 * Note that VM argument -Dfuse.env=value is mandatory
	 * 
	 * @param text
	 * @return
	 */
	public static String getProperty(String text) {

		Properties prop = new Properties();
		String propFileName = "config.properties."
				+ ConfigUtils.getEnvironmentVariable();
		InputStream inputStream = ConfigUtils.class.getClassLoader()
				.getResourceAsStream(propFileName);
		try {
			prop.load(inputStream);
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (inputStream == null) {
			try {
				throw new FileNotFoundException("Property file '"
						+ propFileName + "' not found in classpath");
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}

		return prop.getProperty(text);

	}

	/**
	 * This method reads the environment variables supplied as vm arguments to
	 * the program
	 * 
	 * @return
	 */
	public static String getEnvironmentVariable() {

		String envvar = System.getProperty("cafe.env");
		return envvar;

	}

	/**
	 * Main method for testing purpose
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println(ConfigUtils.getProperty("esIndex"));
	}

}
