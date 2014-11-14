package job;

import java.io.Serializable;
import java.text.NumberFormat;

public class JobID implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4745005882476415746L;
	protected static final String JOB = "job";
	protected static final char SEPARATOR = '_';
	protected int id;

	protected static final NumberFormat idFormat = NumberFormat.getInstance();
	static {
		idFormat.setGroupingUsed(false);
		idFormat.setMinimumIntegerDigits(4);
	}

	/**
	 * Constructs a JobID object
	 * 
	 * @param jtIdentifier
	 *            jobTracker identifier
	 * @param id
	 *            job number
	 */
	public JobID(int id) {
		this.id = id;
	}

	public JobID() {
		id = 0;
	}

	@Override
	public boolean equals(Object o) {
		if (!super.equals(o))
			return false;

		JobID that = (JobID) o;
		return this.id == that.id;
	}

	/** Compare JobIds by first jtIdentifiers, then by job numbers */
	public int compareTo(JobID that) {
		return this.id - that.id;
	}

	public StringBuilder appendTo(StringBuilder builder) {
		builder.append(SEPARATOR);
		builder.append(idFormat.format(id));
		return builder;
	}

	@Override
	public int hashCode() {
		return Integer.toString(id).hashCode();
	}

	@Override
	public String toString() {
		return appendTo(new StringBuilder(JOB)).toString();
	}
}
