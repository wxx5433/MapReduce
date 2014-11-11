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
	private final String jtIdentifier;
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
	public JobID(String jtIdentifier, int id) {
		this.jtIdentifier = jtIdentifier;
		this.id = id;
	}

	public JobID() {
		jtIdentifier = "";
		id = 0;
	}

	public String getJtIdentifier() {
		return jtIdentifier.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (!super.equals(o))
			return false;

		JobID that = (JobID) o;
		return this.jtIdentifier.equals(that.jtIdentifier)
				&& this.id == that.id;
	}

	/** Compare JobIds by first jtIdentifiers, then by job numbers */
	public int compareTo(JobID o) {
		JobID that = (JobID) o;
		int jtComp = this.jtIdentifier.compareTo(that.jtIdentifier);
		if (jtComp == 0) {
			return this.id - that.id;
		} else
			return jtComp;
	}

	public StringBuilder appendTo(StringBuilder builder) {
		builder.append(SEPARATOR);
		builder.append(jtIdentifier);
		builder.append(SEPARATOR);
		builder.append(idFormat.format(id));
		return builder;
	}

	@Override
	public int hashCode() {
		return jtIdentifier.hashCode() + id;
	}

	@Override
	public String toString() {
		return appendTo(new StringBuilder(JOB)).toString();
	}
}
