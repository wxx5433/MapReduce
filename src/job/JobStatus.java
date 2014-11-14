package job;

public class JobStatus {

  /**
   * Current state of the job 
   */
  public static enum State {
    RUNNING(1),
    SUCCEEDED(2),
    FAILED(3),
    PREP(4),
    KILLED(5);

    int value;

    State(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }
  }
}
