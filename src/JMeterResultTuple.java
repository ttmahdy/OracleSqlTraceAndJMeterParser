
public class JMeterResultTuple {
	public JMeterResultTuple(long timeStamp, String testName,int elapsed) {
		super();
		this.timeStamp = timeStamp;
		this.testName = testName;
		this.elapsed = elapsed;
	}
	public int getElapsed() {
		return elapsed;
	}
	public void setElapsed(int elapsed) {
		this.elapsed = elapsed;
	}
	public long getTimeStamp() {
		return timeStamp;
	}
	public void setTimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}
	public String getTestName() {
		return testName;
	}
	public void setTestName(String testName) {
		this.testName = testName;
	}
	private long timeStamp;
	private int elapsed;
    private String testName;
}
