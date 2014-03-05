import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class GetSqlTraceFiles {

	/**
	 * @param args
	 */
	
    private static final String jMeterFileSuffix    ="_jmeter.csv";
    private static final String sqlTraceFileSuffix  =".trc";
    private static final String tkProfOutFileSuffix = ".out";
//    private static final String jMeterFilesPath = "/home/mmokhtar/Documents/ChatterFeeds/FollowV2/Trace/201402261326";
//    private static final String sqlTraceFilesPath = "/home/mmokhtar/Documents/ChatterFeeds/FollowV2/TraceFiles";
    private static final String tkProfExec = "/usr/bin/tkprof";
    private static final String allNonRecursiveStatementString = "OVERALL TOTALS FOR ALL NON-RECURSIVE STATEMENTS";
    private static final String allRecursiveStatementString = "OVERALL TOTALS FOR ALL RECURSIVE STATEMENTS";
    private static final String jmeterPerTestSummaryFileName = "SummaryPerJmeterTest.csv";
    private static final String jmeterGlobalSummaryFile = "SummaryGlobal.csv"; 
    private static final String SummaryFileHeader = "JMeterFileName,jMeterTestName,jMetertestElapsedms,statementType,stmtId,packageName,procedureName,count,cpu,elapsed,disk,query,current,rows,jMetertestTimeStamp,sqlTraceLastModified,jMeterTestStartTimeStamp,jMeterTestEndTimeStamp,sqlTraceFile";
    public static File jmeterPerTestSummaryFile;
    public static File jmeterSummaryFile;
    public static BufferedWriter jmeterPerTestsummaryFileWriter;
    public static BufferedWriter jmeterTestsummaryFileWriter;
    private static final long firstTimeStampId = 1;
    
    private static File[] getFilesInFolder(String folderPath,final String fileSuffix) {
    	File dir = new File(folderPath);
        return dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith(fileSuffix);
            }
        });
    }
    
    private static void execTkProf (String trcFile,String outFile)
    {
    	try{
            String[] cmd = {tkProfExec,trcFile,outFile};
            Process pr = Runtime.getRuntime().exec(cmd);
            int exitVal = pr.waitFor();

            if (exitVal != 0)
            {
                System.out.println("Exit code: " + exitVal);
            	BufferedReader input = new BufferedReader(new InputStreamReader(pr.getInputStream()));
	            String line=null;
	            while((line=input.readLine()) != null) {
	                System.out.println(line);
	            }
            }
            
        } catch (Throwable t){
            t.printStackTrace();
        }
    }
    
    private static void getSqlTraceFilesPerJmeterTest (List<JMeterResultTuple> parsedJMeterList, File[] sqlTraceFiles,File jMeterFile,BufferedWriter jmeterPerTestsummaryFileWriter) throws Exception
    {
    	int jmeterTestId = 0;
    	long firstTimeStamp = 0;
    	long lastTimeStamp = 0;
    	String subFolder = null;
    	String copyDestinationFolder = null;
    	String testName = null;
    	String baseDirectory = jMeterFile.getPath().replace(jMeterFileSuffix, "");
    	String jMeterFileName = jMeterFile.getName().replace(jMeterFileSuffix, "");
    	
    	for ( JMeterResultTuple jmrt : parsedJMeterList)
    	{
    		subFolder = jmrt.getTestName().replace("/", "_").replace("?", "_").replace("%", "_").replace("=", "_").replace(" ", "_").replace("#", "_").replace("&", "_").replace("{", "_").replace("}", "_").replace("$", "_");
    		copyDestinationFolder = baseDirectory +"/"+subFolder;
    		
    		// Now are actually parsing the Jmeter file
    		if (jmeterTestId == 0)
    		{
    			firstTimeStamp = parsedJMeterList.get(0).getTimeStamp() - 1000;
    		}
    		else
    		{
    			firstTimeStamp = parsedJMeterList.get(jmeterTestId-1).getTimeStamp();
    			firstTimeStamp = parsedJMeterList.get(jmeterTestId).getTimeStamp() -parsedJMeterList.get(jmeterTestId).getElapsed();
    			firstTimeStamp = Math.min(parsedJMeterList.get(jmeterTestId-1).getTimeStamp() + parsedJMeterList.get(jmeterTestId-1).getElapsed(),
    								parsedJMeterList.get(jmeterTestId).getTimeStamp() -parsedJMeterList.get(jmeterTestId).getElapsed());
    			firstTimeStamp = parsedJMeterList.get(jmeterTestId-1).getTimeStamp();
    		}
    		
    		if (jmeterTestId == parsedJMeterList.size() - 1)
    		{
    			// If this is the last test , just append a 1 sec to the end time
    			lastTimeStamp  = parsedJMeterList.get(jmeterTestId).getTimeStamp() + 2000;
    		}
    		else
    		{
    			lastTimeStamp  = parsedJMeterList.get(jmeterTestId+1).getTimeStamp() - parsedJMeterList.get(jmeterTestId+1).getElapsed();
    			lastTimeStamp  = Math.min(parsedJMeterList.get(jmeterTestId).getTimeStamp() + parsedJMeterList.get(jmeterTestId).getElapsed(), parsedJMeterList.get(jmeterTestId+1).getTimeStamp() - parsedJMeterList.get(jmeterTestId+1).getElapsed());
    			lastTimeStamp  = parsedJMeterList.get(jmeterTestId).getTimeStamp();
    			//lastTimeStamp  = parsedJMeterList.get(jmeterTestId).getTimeStamp();
    		}
    		
    		
    		
    		testName = jmrt.getTestName();
    		System.out.println("Test ID ," + jmeterTestId + "," +" first Time stamp ," + firstTimeStamp+ "," +" last Time stamp ," + lastTimeStamp + " test end time "+ jmrt.getTimeStamp() + ","+ testName);

    		File[] sqlTraceFilesMatchingTimeStamp = getFilesWithinTimeRange(sqlTraceFiles,firstTimeStamp,lastTimeStamp);
    		
    		copyAndProcessTraceFiles(sqlTraceFilesMatchingTimeStamp,subFolder,copyDestinationFolder,jMeterFileName,jmrt.getElapsed(),jmrt.getTimeStamp(),firstTimeStamp,lastTimeStamp,jmeterPerTestsummaryFileWriter);
    
    		jmeterTestId++;
    	}
    }
    
    private static void copyAndProcessTraceFiles(File[] sqlTraceFilesMatchingTimeStamp,String subFolder,
    		String copyDestinationFolder,String jMeterFileName,int jMetertestElapsed,long jMetertestTimeStamp,
    		long firstTimeStamp,long lastTimeStamp,BufferedWriter fileWriter) throws Exception{
		try {
			if (sqlTraceFilesMatchingTimeStamp.length > 0)
			{
				copySqlStraceFilesToDir(sqlTraceFilesMatchingTimeStamp,copyDestinationFolder);
			    
				for (int i = 0; i < sqlTraceFilesMatchingTimeStamp.length; i++) {
			    	String trcFile = sqlTraceFilesMatchingTimeStamp[i].getAbsolutePath();
			    	String outFile = sqlTraceFilesMatchingTimeStamp[i].getAbsolutePath().replace(sqlTraceFileSuffix, tkProfOutFileSuffix);
			    	
			    	// run the Tkprof command
			    	execTkProf(trcFile,outFile);

			    	// Parse and summarize the tkprof out file
			    	parseTkprofFile(outFile,subFolder,jMeterFileName,jMetertestElapsed,jMetertestTimeStamp,sqlTraceFilesMatchingTimeStamp[i].lastModified(),firstTimeStamp,lastTimeStamp,fileWriter);
			    }
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }
    
    private static File[] getFilesWithinTimeRange(File[] listOfFilesToSearch,long minTime,long maxTime)
    {
    	List<File> filesWithinRange = new ArrayList<File>();
	    for (int i = 0; i < listOfFilesToSearch.length; i++) {
		      if (listOfFilesToSearch[i].isFile()) {
		        long sqlTraceLastModifiedTime = listOfFilesToSearch[i].lastModified();

		        if((sqlTraceLastModifiedTime <= maxTime) && (sqlTraceLastModifiedTime >= minTime))
		        {
		        	//System.out.println(" Matching file, " + listOfFilesToSearch[i] + " ,Time stamp, " + sqlTraceLastModifiedTime + " ,requested range, " + minTime +","+maxTime);
		        	filesWithinRange.add(listOfFilesToSearch[i]);
		        }
		      } else if (listOfFilesToSearch[i].isDirectory()) {
		        System.out.println("Directory " + listOfFilesToSearch[i].getName());
		      }
		    }
    	
	    File[] stockArr = new File[filesWithinRange.size()];
	    stockArr = filesWithinRange.toArray(stockArr);
    	return stockArr;
    }
    
    private static long getNthTimeStampinJmeterFile(File jmeterFile,Long timeStampNumber) throws IOException
    {
    	long timeStamp = 0;
    	try {
    		
    		BufferedReader br = new BufferedReader(new FileReader(jmeterFile.getPath()));
    		String s=null;
    		int rowNum = 0;
    		
	    		while((s=br.readLine())!=null ){
	
	    			if(rowNum == timeStampNumber)
	    			{
		    			String f[] = s.split(",");
		    			timeStamp =Long.parseLong(f[0]);
		    			break;
	    			}
	    			rowNum++;
	    		}
    		br.close();
    	 } 
    	catch (FileNotFoundException ex) {
    		Logger.getLogger(GetSqlTraceFiles.class.getName()).log(Level.SEVERE, null, ex);
    	 }
    	
		return timeStamp ;
    }
    
    
    private static List<JMeterResultTuple> parseJMeterFile(File jmeterFile) throws Exception
    {
    	long timeStamp = 0;
    	int elapsed = 0; 
    	int responseCode = 0;
    	String testName = null;
    	String responseMessage = null;
    	List<JMeterResultTuple> jmList = new ArrayList<JMeterResultTuple>();
    	try {
    		
    		BufferedReader br = new BufferedReader(new FileReader(jmeterFile.getPath()));
    		String s=null;
    		int rowNum = 0;
    		
	    		while((s=br.readLine())!=null ){
	
	    			if(rowNum >= 1)
	    			{
		    			String f[] = s.split(",");
		    			try {
		    				responseMessage = f[4];
		    				responseCode = Integer.parseInt(f[3]);
		    				if((responseCode == 200) && (responseMessage.contains("OK")))
		    				{
		    					timeStamp = Long.parseLong(f[0]);
		    					elapsed = Integer.parseInt(f[1]);
		    					testName = f[2];
		    					JMeterResultTuple jmrt = new JMeterResultTuple(timeStamp,testName,elapsed);
		    					jmList.add(jmrt);
		    				}
		    			}
		    			catch (Exception e){}
		    			finally {}
	    			}
	    			rowNum++;
	    		}
    		br.close();
    		
    	 } 
    	catch (Exception ex) {
    		Logger.getLogger(GetSqlTraceFiles.class.getName()).log(Level.SEVERE, null, ex);
    	 }
    	
		return jmList ;
    }
    
    private static void parseTkprofFile(String tkProfFileName,String jMeterTestName,String jMeterFileName,int jMetertestElapsed,
    		long jMetertestTimeStamp,long sqlTraceLastModified,
    		long firstTimeStamp,long lastTimeStamp,BufferedWriter summaryFileWriter) throws Exception
    {
    	File tkProfFile = new File(tkProfFileName);
    	
    	try {
    		
    		BufferedReader br = new BufferedReader(new FileReader(tkProfFile.getPath()));
    		String s=null;
    		int stmtIndex = 0;
    		boolean matchSelect = false;
    		boolean matchBegin = false;
    		boolean matchAllNonRecursive = false;
    		boolean matchAllRecursive = false;
    		String statementType = null;
    		String selectStatmentPrefix = null;
    		String beginStatmentPrefix = null;
    		String packageName = null;
    		String procedureName = null;
    		String count = null;
    		String cpu = null;
    		String elapsed = null;
    		String disk = null;
    		String query = null;
    		String current = null;
    		String rows = null;
    		
	    		while((s=br.readLine())!=null ){
	    			
	    			if(s.contains("BEGIN /* SFDCTRACE"))
	    			{
	    				continue;
	    			}
	    			
	    			if(Pattern.matches("SELECT.+.sql.+", s))
	    			{
	    				
	    				matchSelect = true;
	    				matchBegin = false;
	    				matchAllNonRecursive = false;
	    				matchAllRecursive = false;
	    				packageName = null;
	    				procedureName = null;
	    				String sqlLine[] = s.split("/");
	    				selectStatmentPrefix =sqlLine[1];
	    				statementType = "SELECT";
	    				if (selectStatmentPrefix.contains(":"))
	    				{
	    					String stmntnAr[] = selectStatmentPrefix.split(":");
	    					if (stmntnAr.length  >= 2)
	    					{
		    					packageName = stmntnAr[0].replace(".sql", "").replace("*","");
		    					procedureName = stmntnAr[1];
	    					}
	    				}
	    				else
	    				{
	    					packageName= s;
	    				}
	    				stmtIndex++;
	    			}
	    			
	    			if(s.contains("BEGIN "))
	    			{
	    				matchBegin = true;
	    				matchSelect = false;
	    				matchAllNonRecursive = false;
	    				matchAllRecursive = false;
	    				packageName = null;
	    				procedureName = null;
	    				statementType = "BEGIN";
	    				beginStatmentPrefix=s;
	    				if (beginStatmentPrefix.contains("."))
	    				{
	    					String bgnAr[] = beginStatmentPrefix.split("\\.");
	    					
	    					if (bgnAr.length  >= 2)
	    					{
	 	    					packageName = bgnAr[0].replace("BEGIN ", "");
	 	    					if (packageName.contains("="))
	 	    					{
	 	    						packageName = packageName.split("=")[1];
	 	    					}
		    					procedureName = bgnAr[1];
	    					}
	    				}
	    				else
	    				{
	    					packageName= s;
	    				}
	    				stmtIndex++;
	    			}

	    			if(s.contains(allNonRecursiveStatementString))
	    			{
	    				matchBegin = false;
	    				matchSelect = false;
	    				matchAllNonRecursive = true;
	    				matchAllRecursive = false;
	    				packageName = null;
	    				procedureName = null;
	    				statementType = "Total NON-RECURSIVE";
	    				stmtIndex = -1;
	    			}
	    			
	    			if(s.contains(allRecursiveStatementString))
	    			{
	    				matchAllRecursive = true;
	    				matchAllNonRecursive = false;
	    				matchBegin = false;
	    				matchSelect = false;
	    				packageName = null;
	    				procedureName = null;
	    				statementType = "Total RECURSIVE";

	    				stmtIndex = -1;
	    			}
	    			
	    			
	    			if ((matchBegin || matchSelect || matchAllNonRecursive || matchAllRecursive) && s.contains("total"))
	    			{
	    				
	    				if (procedureName != null){ 
	    				if (procedureName.contains(":"))
		    				{
	    					String prAr[] = procedureName.split(":");	
	    					procedureName = prAr[0].replace("(","");
		    				}
	    				}
	    				
	    				String statmentSummary[] = s.split("\\s+");
	    				count = statmentSummary[1];
	    				cpu = statmentSummary[2];
	    				elapsed = statmentSummary[3];
	    				disk = statmentSummary[4];
	    				query = statmentSummary[5];
	    				current = statmentSummary[6];
	    				rows = statmentSummary[7];
	    				System.out.println(jMeterFileName + "," + jMeterTestName+ "," + tkProfFile.getName() + "," + statementType + "," + stmtIndex + "," + packageName + "," + procedureName + "," + count + "," + cpu + "," + elapsed + "," + disk + "," + query + "," +current+ "," +rows);
	    				 SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
	    				 String jMetertestTimeStamp2 = dateFormat.format(jMetertestTimeStamp);
	    				 String sqlTraceLastModified2= dateFormat.format(sqlTraceLastModified);
	    				 String firstTimeStamp2= dateFormat.format(firstTimeStamp);
	    				 String lastTimeStamp2= dateFormat.format(lastTimeStamp);
	    				summaryFileWriter.append(jMeterFileName + "," + jMeterTestName+ "," +jMetertestElapsed+ "," + statementType + "," + stmtIndex + "," + packageName + "," + procedureName + "," + count + "," + cpu + "," + elapsed + "," + disk + "," + query + "," +current+ "," +rows+"," + jMetertestTimeStamp+"," +sqlTraceLastModified+"," +firstTimeStamp+"," +lastTimeStamp+"," + tkProfFile.getName() + "\n");
	    				//summaryFileWriter.append(jMeterFileName + "," + jMeterTestName+ "," +jMetertestElapsed+ "," + statementType + "," + stmtIndex + "," + packageName + "," + procedureName + "," + count + "," + cpu + "," + elapsed + "," + disk + "," + query + "," +current+ "," +rows+"," + jMetertestTimeStamp2+"," +sqlTraceLastModified2+"," +firstTimeStamp2+"," +lastTimeStamp2+"," + tkProfFile.getName() + "\n");
	    				matchSelect = matchBegin = matchAllNonRecursive = matchAllRecursive = false;
	    			}
	    		}
	    		summaryFileWriter.flush();
    		br.close();
    	 } 
    	catch (FileNotFoundException ex) {
    		Logger.getLogger(GetSqlTraceFiles.class.getName()).log(Level.SEVERE, null, ex);
    	 }
    }
    
    private static void copySqlStraceFilesToDir( File[] sqlTraceFilesMatchingTimeStamp,String jMeterTraceFilesDirName) throws IOException{
    	
        if (sqlTraceFilesMatchingTimeStamp.length > 0)
        {
        	System.out.println("File " + jMeterTraceFilesDirName + " has " + sqlTraceFilesMatchingTimeStamp.length + " matching trace files");
        	File jMeterTraceFilesDir = new File(jMeterTraceFilesDirName);
        	// if the directory does not exist, create it
        	  if (!jMeterTraceFilesDir.exists()) {
        	    boolean result = jMeterTraceFilesDir.mkdir();  
        	     if(result) {    
        	       System.out.println("DIR created");  
        	     }
        	  }
        	  
	        // Copy the files to a given folder
	        for (int j = 0; j < sqlTraceFilesMatchingTimeStamp.length; j++)
	        {
	        	Path original = Paths.get(sqlTraceFilesMatchingTimeStamp[j].getPath()); 
	        	Path destination = Paths.get(jMeterTraceFilesDir.getPath()+"/"+sqlTraceFilesMatchingTimeStamp[j].getName());
	        	Files.copy(original, destination,
	        			   StandardCopyOption.COPY_ATTRIBUTES,StandardCopyOption.REPLACE_EXISTING);
	        }
        }
    }
    
    public static void validateArgs(String[] args) throws Exception
    {
		String jMeterFilesPath = args[0];
		String sqlTraceFilesPath = args[1];
		
		File jMeterFilesDir = new File(jMeterFilesPath);
		File sqlTraceFilesDir = new File(sqlTraceFilesPath);
    	
		if (!jMeterFilesDir.exists()) {
			
			throw new Exception("Invalid arg: jmeter files directory doesn't exist , check " + jMeterFilesPath); 
		}
		
		if (!sqlTraceFilesDir.exists()) {
			
			throw new Exception("Invalid arg: sql trace files directory doesn't exist , check " + sqlTraceFilesDir); 
		}
		
    }
    
	public static void main(String[] args) throws Exception {

			// Validate the input args
			validateArgs(args);	
			String jMeterFilesPath = args[0];
			String sqlTraceFilesPath = args[1];
			
			// Get the list of Jmeter and Sql trace files
		    File[] jMeterFiles = getFilesInFolder(jMeterFilesPath,jMeterFileSuffix);
		    File[] sqlTraceFiles = getFilesInFolder(sqlTraceFilesPath,sqlTraceFileSuffix);
		    
		    // Write the header for the global summary file
		    jmeterSummaryFile = new File(jMeterFilesPath+"/"+jmeterGlobalSummaryFile);
		    jmeterTestsummaryFileWriter  = new BufferedWriter(new FileWriter(jmeterSummaryFile,false));
		    jmeterTestsummaryFileWriter.write(SummaryFileHeader+"\n");
		    jmeterTestsummaryFileWriter.flush();
		    
		    try
		    {
			    // Summarize all the trc files found under testFolder
			    copyAndProcessTraceFiles(sqlTraceFiles,null,jMeterFilesPath+"/"+"allTraceFiles",null,-1,-1,-1,-1,jmeterTestsummaryFileWriter);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		    finally
		    {
				jmeterTestsummaryFileWriter.flush();
				jmeterTestsummaryFileWriter.close();
		    }
		    
		    jmeterPerTestSummaryFile = new File(jMeterFilesPath+"/"+jmeterPerTestSummaryFileName); 
		    jmeterPerTestsummaryFileWriter = new BufferedWriter(new FileWriter(jmeterPerTestSummaryFile,false));
		    jmeterPerTestsummaryFileWriter.write(SummaryFileHeader+"\n");
		    jmeterPerTestsummaryFileWriter.flush();

		    long jMeterLastModifiedTime = 0;
		    for (int i = 0; i < jMeterFiles.length; i++) {
		    	//jmeterTestsummaryFileWriter  	= new BufferedWriter(new FileWriter(jmeterSummaryFile,true));
		    	jmeterPerTestsummaryFileWriter 	= new BufferedWriter(new FileWriter(jmeterPerTestSummaryFile,true));
		    	if (jMeterFiles[i].isFile())
			      {
					  try 
				      {
						  	String jMeterTraceFilesDirName =jMeterFiles[i].getPath().replace(jMeterFileSuffix, "");
						  	String jmeterTestSummaryFileName = "Summary.csv";
						  	File jMeterTraceFilesDi = new File(jMeterTraceFilesDirName);
						  	// Create the directory	
						  	if (!jMeterTraceFilesDi.exists()) {jMeterTraceFilesDi.mkdir();}
						  	
						  	File jmeterTestSummaryFile = new File(jMeterTraceFilesDirName+"/"+jmeterTestSummaryFileName);
						  	BufferedWriter jmeterTestsummaryFileWriter  = new BufferedWriter(new FileWriter(jmeterTestSummaryFile,false));
						  	jmeterTestsummaryFileWriter.write(SummaryFileHeader+"\n");
						    
						  	jMeterLastModifiedTime = jMeterFiles[i].lastModified();

						    // Get the number of lines in the file 
						    LineNumberReader  lnr = new LineNumberReader(new FileReader(jMeterFiles[i]));
						    lnr.skip(Long.MAX_VALUE);
						    
						    // Get the first time stamp in the JMeter file
						    Long firstTimeStamp = getNthTimeStampinJmeterFile(jMeterFiles[i],firstTimeStampId);
						    
						    // 
							List<JMeterResultTuple> parsedJMeterList = parseJMeterFile(jMeterFiles[i]);
						    
						    // Get the matching sql trace files per JMeterTest
						    getSqlTraceFilesPerJmeterTest(parsedJMeterList,sqlTraceFiles,jMeterFiles[i],jmeterPerTestsummaryFileWriter);
						    
						    //Get the last time stamp in the JMeter file
						    Long lastTimeStamp = getNthTimeStampinJmeterFile(jMeterFiles[i],(long) lnr.getLineNumber() - 1);		
						    
						    // Find the list of Sql Trace file that are within secondsOffset from the modified time of the 
						    System.out.println("JMeter file : " + jMeterFiles[i] + " Frist time stamp " + firstTimeStamp + " Last time stamp" + lastTimeStamp );
						    int elapsedTime = (int) (lastTimeStamp - firstTimeStamp);
						    
						    File[] sqlTraceFilesMatchingTimeStamp = getFilesWithinTimeRange(sqlTraceFiles,firstTimeStamp,lastTimeStamp);
						    
						    // Create a directory named as the Jmeter file and copy the sql trace files that belong to this test
						    copyAndProcessTraceFiles(sqlTraceFilesMatchingTimeStamp,"Unknown",jMeterFiles[i].getPath().replace(jMeterFileSuffix, ""),jMeterFiles[i].getName().replace(jMeterFileSuffix,""),elapsedTime,-1,firstTimeStamp,lastTimeStamp,jmeterTestsummaryFileWriter);
						    
						    //copySqlStraceFilesToDir(sqlTraceFilesMatchingTimeStamp,jMeterTraceFilesDirName);
						    
						    System.out.println("File " + jMeterFiles[i].getName() + " " + jMeterLastModifiedTime);
						  
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					  finally
					  {
						  jmeterPerTestsummaryFileWriter.flush();
						  jmeterPerTestsummaryFileWriter.close();
					  }
			      }
				else if (jMeterFiles[i].isDirectory()) {
			        System.out.println("Directory " + jMeterFiles[i].getName());
			      }
			    }
	}
}
