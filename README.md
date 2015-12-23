#A simple job scheduler

#Tutorial
You can also find examples in the test/ directory.
Modify the test/vars file to change the configurations.

##Start the server
`$ python JobSchedServer.py -a 127.0.0.1 -p 4321 --dbfile /tmp/jobs.sqlite`

Note: You can type python JobSchedServer.py --help to get the help.
  -a specifies the address and -p specifies the port number.
You can start multiple instances with different combinations of addresse or port. 
The maximum number of jobs that can be run simultanously is specified with -j. If the number of running jobs exceeds this value, they will be queued.
  The port number should not be bound by other programs, otherwise an error will occur.
  --dbfile is a database file that contains the job information. A new database file will be created if it does not exist.

##Client
  You can use any HTTP client (curl, wget, etc.) to manage jobs.
You can either use GET or POST method to access the server with the following sytax:

`$ curl --data "param1=value1&param2=value2" 127.0.0.1:4321/command`

A list of available commands can be obtained using: 

`$ curl 127.0.0.1:4321/help`

Available options for each command can be obtained using:

`$ curl 127.0.0.1:4321/help?topic=command`

##Submit jobs
`$ curl --data "name=sleep&command=sleep 10&sync=0" 127.0.0.1:4321/submit`

Note: You can supply an optional job name and group name to identify jobs.
command is a bash command string. 
  The sync parameter should be specified properly. 

If sync=1, then curl will wait until the job finished (useful for short time jobs). If you interrupt curl with Ctrl-C or disconnect from the server, the job will be canceled immediately.
If sync=0, then curl will exit after the job is submitted. The jobid will be shown in the first line of the output. In this case, you can only track the job status with 'jobinfo' command or wait for the job to finish with 'wait' command.

`errlog` and `outlog` specifies the filename which the stdout and stderr will be written to. If you don't specify errlog or outlog when sync=1, they will be written to output of curl.

##Display job information
`$ curl --data "jobid=123,124,135" 127.0.0.1:4321/jobinfo`

`$ curl --data "job=sleep&group=default" 127.0.0.1:4321/jobinfo`

`$ curl --data "all=1" 127.0.0.1:4321/jobinfo`

Note: You can filter the displayed jobs by job id, job name, group name and status.
Only running or queued jobs will be shown by default.
You can supply multiple items by separating them with commas(,).

**Job status**:
* queued: the jobs is added to the queue but not running.
* running: the job is running.
* success: the job finished without interruption. You should check the exit code and the errlog to see if the job finished successfully.
* killed: the job is interrupted. The job may have been cancelled due to disconnection when sync=1, or cancelled with the 'kill' command, or cancelled due to server restart.
* failed: the command cannot be run, or the errlog and outlog file cannot be opened.

##Cancel jobs
`$ curl --data "jobid=123" 127.0.0.1:4321/kill`

##Wait a job to finish
`$ curl --data "jobid=123" 127.0.0.1:4321/wait`

For jobs submitted with sync=0, this command wait for the job to finish.

##Shutdown the server
`$ curl 127.0.0.1:4321/shutdown`

or use Ctrl-C on the server side.
Note: unfinished jobs will be cancelled after server shutdown.


