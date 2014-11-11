package Configuration;

public interface MyConfigurable {
	/** Set the configuration to be used by this object. */
	void setConf(MyConfiguration conf);

	/** Return the configuration used by this object. */
	MyConfiguration getConf();
}
