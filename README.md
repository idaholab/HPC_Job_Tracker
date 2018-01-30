# HPC_Job_Tracker
HPC job tracker is a wrapper script which will allow some of the resources a HPC application consumes to be tracked for the duration of the job.

The Following can be trackered/monitored
  * Job memory usage, on each node and aggregate for the job.
  * Job load, aggregate and individual nodes.
  * On what physical cores parallel threads are running on over time
  
 Some key features of job tracker
  * Written in Python
  * No recompiling/relinking of software required
  * Ability to start tracking already running job (i.e attach to a running job)
  * Can produce 2D time dependent plots or text reports.
