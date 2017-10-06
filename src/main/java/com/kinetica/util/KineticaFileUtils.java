package com.kinetica.util;

import com.kinetica.exception.KineticaException;
import org.apache.commons.io.FileUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class KineticaFileUtils {

	public void append(File file, 
			String data) throws IOException {
		String encoding = null;
		FileUtils.write(file, data, encoding, true);
	}
	
	public List<String> readFromFile(
			String fileName) {	
		try {
			String encoding = null;
			return FileUtils.readLines(
				new File(fileName), encoding);
		} catch (IOException e) {
			throw new KineticaException(e);
		}
	}	

	public void writeToDisk(String 
			fileName, String contents) {
		FileWriter fw = null;
		BufferedWriter bw = null;
		try {
			fw = new FileWriter(fileName);
			bw = new BufferedWriter(fw);
			bw.write(contents);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (bw != null)
					bw.close();
				if (fw != null)
					fw.close();
			} catch (IOException e) {
				throw new KineticaException(e);
			}
		}
	}

}