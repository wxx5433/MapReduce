package job;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.io.IOException;

import job.JobStatus.State;
import task.TaskAttemptID;


/** 
 * <code>RunningJob</code> is the user-interface to query for details on a 
 * running Map-Reduce job.
 * 
 * <p>Clients can get hold of <code>RunningJob</code> via the {@link JobClient}
 * and then query the running-job for details such as name, configuration, 
 * progress etc.</p> 
 * 
 * @see JobClient
 */
public interface RunningJobInterface {
  /**
   * Get the job ID
   * 
   * @return the job ID
   */
  public JobID getJobID();
  
  /**
   * Get the name of the job.
   * 
   * @return the name of the job.
   */
  public String getJobName();

  /**
   * Get the path of the submitted job configuration.
   * 
   * @return the path of the submitted job configuration.
   */
  public String getJobFile();

  /**
   * Get the <i>progress</i> of the job's map-tasks, as a float between 0.0 
   * and 1.0.  When all map tasks have completed, the function returns 1.0.
   * 
   * @return the progress of the job's map-tasks.
   * @throws IOException
   */
  public float mapProgress() throws IOException;

  /**
   * Get the <i>progress</i> of the job's reduce-tasks, as a float between 0.0 
   * and 1.0.  When all reduce tasks have completed, the function returns 1.0.
   * 
   * @return the progress of the job's reduce-tasks.
   * @throws IOException
   */
  public float reduceProgress() throws IOException;

  /**
   * Set the <i>progress</i> of the job's setup-tasks, as a float between 0.0 
   * and 1.0.  When all setup tasks have completed, the function returns 1.0.
   * 
   * @return the progress of the job's setup-tasks.
   * @throws IOException
   */
  public void setMapProgress() throws IOException;
  
  public void setReduceProgress() throws IOException; 

  /**
   * Check if the job is finished or not. 
   * 
   * @return <code>true</code> if the job is complete, else <code>false</code>.
   * @throws IOException
   */
  public boolean isComplete() throws IOException;
  
  /**
   * Blocks until the job is complete.
   * 
   * @throws IOException
   */
  public void waitForCompletion() throws IOException;

  /**
   * Returns the current state of the Job.
   * 
   * @throws IOException
   */
  public int getJobState() throws IOException;
  
  public void setJobState(State s) throws IOException;
  
  /**
   * Returns a snapshot of the current status, {@link JobStatus}, of the Job.
   * Need to call again for latest information.
   * 
   * @throws IOException
   */
  public JobStatus getJobStatus() throws IOException;


}
