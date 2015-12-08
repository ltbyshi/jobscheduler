#! /usr/bin/env python

import SocketServer
import argparse
import random
import time
import urlparse
import cgi
import BaseHTTPServer
import sqlite3
import os, sys, subprocess
import select
import threading
import signal
from cStringIO import StringIO
import logging


rng = random.Random()
job_dbfile = None
sock_poll = select.poll()
sock_pid = dict()

class RuntimeInfo:
    def __init__(self):
        self.pid = -1
        self.wfd = None
        self.killed = False
        
# dict of RuntimeInfo object indexed by jobid
runtime_info = dict()
need_shutdown = False
# threading synchronization objects
lock_queue = threading.Lock()
cond_queue = threading.Condition(lock_queue)
# lock when accessing runtime_info or need_shutdown
lock_runtime_info = threading.Lock()
lock_need_shutdown = threading.Lock()
# signal when an thread exits
cond_thread_exit = threading.Condition(lock_runtime_info)

strevent = [(select.POLLIN, 'POLLIN'), 
            (select.POLLPRI, 'POLLPRI'),
            (select.POLLOUT, 'POLLOUT'),
            (select.POLLERR, 'POLLERR'),
            (select.POLLHUP, 'POLLHUP'),
            (select.POLLNVAL, 'POLLNVAL')]

class JobException(Exception):
    def __init__(self, msg):
        Exception.__init__(self)
        self.msg = msg
        
class NeedShutdownException(Exception):
    pass
        
def InitJobDB(dbfile):
    conn = sqlite3.connect(dbfile)
    cursor = conn.cursor()
    cursor.execute('''DROP TABLE IF EXISTS JobInfo''')
    cursor.execute('''CREATE TABLE JobInfo
        (jobid INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT, jobgroup TEXT, command TEXT,
        status TEXT, 
        returncode INTEGER,
        sync INTEGER,
        outlog TEXT,
        errlog TEXT,
        msg TEXT,
        time TEXT)''')
    conn.commit()
    conn.close()
    
def ClearOldJobs():
    dbcon = sqlite3.connect(job_dbfile)
    cursor = dbcon.cursor()
    cursor.execute('''UPDATE JobInfo SET status = 'killed' WHERE status in ('running', 'queued')''')
    dbcon.commit()
    dbcon.close()
    
def KillJob(pid):
    try:
        os.kill(pid, signal.SIGKILL)
        return True
    except OSError:
        return False
    
# kill all running jobs
def ClearJobs(logger, timeout=1):
    with lock_runtime_info:
        global need_shutdown
        need_shutdown = True
        for jobid, rtinfo in runtime_info.iteritems():
            if rtinfo.pid > 0:
                if KillJob(rtinfo.pid):
                    logger.warning('Killed job %d (pid: %d)'%(jobid, rtinfo.pid))
                else:
                    logger.warning('Failed to kill job %d (pid: %d)'%(jobid, rtinfo.pid))
    # wait for all workers to finish
    with cond_thread_exit:
        while len(runtime_info) > 0:
            cond_thread_exit.wait(timeout)
            

class ConnectionMonitor(threading.Thread):
    logger = logging.getLogger('ConnectionMonitor')
    def __init__(self, wait_timeout=200):
        threading.Thread.__init__(self)
        self.last_event = (0, 0, 0)
        self.wait_timeout = wait_timeout
        
    def parse_event(self, event):
        result = []
        for e, s in strevent:
            if (e & event):
                result.append(s)
        return result
                
    def process_event(self, fd, event):
        #print '[ConnectionMonitor] Event on %d'%fd
        fd_found = False
        for jobid, rtinfo in runtime_info.iteritems():
            if rtinfo.wfd == fd:
                fd_found = True
                break
        if not fd_found:
            return
        pid = rtinfo.pid
        if (fd, event) != self.last_event:
             self.last_event = (fd, event)
             eventstr = ','.join(self.parse_event(event))
             self.logger.debug('Poll events, fd: %d, pid: %d, events: %s'%(fd, pid, eventstr))
        if (event & select.POLLIN):
            if not rtinfo.killed:
                if KillJob(rtinfo.pid):
                    self.logger.warning('Killed process %d'%rtinfo.pid)
                    with lock_runtime_info:
                        runtime_info[jobid].killed = True
                else:
                    self.logger.warning('Failed to kill process %d'%rtinfo.pid)
                        
    def run(self):
        self.logger.info('Started (%d)'%self.ident)
        while True:
            with lock_need_shutdown:
                if need_shutdown:
                    break
            res = sock_poll.poll(self.wait_timeout)
            for fd, event in res:
                self.process_event(fd, event)
  
class JobWorker(threading.Thread):
    logger = logging.getLogger('JobWorker')
    def __init__(self, jobid, sync=True, wfile=None):
        threading.Thread.__init__(self)
        self.jobid = jobid
        self.sync = sync
        self.wfile = wfile
        self.child = None
        
    def run(self):
        self.logger.info('Started, JobId: %d'%self.jobid)
        dbcon = sqlite3.connect(job_dbfile)
        dbcon.row_factory = sqlite3.Row
        cursor = dbcon.cursor()
        status = ''
        returncode = 0
        msg = StringIO()
        polled = False
        wfd = runtime_info[self.jobid].wfd
        try:
            cursor.execute('''SELECT * FROM JobInfo WHERE jobid = ?''',
                           (self.jobid,))
            jobinfo = cursor.fetchone()
            if not jobinfo:
                raise JobException('job ID %d not found in database'%self.jobid)
            
            if jobinfo['outlog']:
                stdout = open(jobinfo['outlog'], 'w')
            elif self.sync:
                stdout = self.wfile
            else:
                stdout = open(os.devnull, 'w')
    
            if jobinfo['errlog']:
                stderr = open(jobinfo['errlog'], 'w')
            elif self.sync:
                stderr = self.wfile
            else:
                stderr = open(os.devnull, 'w')
            if need_shutdown:
                raise NeedShutdownException()
            with lock_runtime_info:
                self.child = subprocess.Popen(['bash', '-c', jobinfo['command']],
                             stdin=subprocess.PIPE,
                             stdout=stdout,
                             stderr=stderr,
                             shell=False)
                # update PID
                runtime_info[self.jobid].pid = self.child.pid
            # register connection monitor
            if self.sync:
                sock_poll.register(wfd, select.POLLIN)
                polled = True
                self.logger.debug('Registered poll: %d'%wfd)
            # wait for job finish
            self.child.communicate('')
            with lock_runtime_info:
                runtime_info[self.jobid].pid = -1
            returncode = self.child.returncode
            self.child = None
            if returncode == -9:
                status = 'killed'
            else:
                status = 'success'
        except OSError as e:
            msg.write('Error: failed to run the command: %s\n'%e.strerror)
            returncode = -e.errno
            status = 'failure'
        except IOError as e:
            msg.write('Error: cannot open the log file: %s\n'%e.strerror)
            returncode = -e.errno
            status = 'failure'
        except JobException as e:
            msg.write('Error: %s\n'%e.msg)
            returncode = 100
            status = 'failure'
        except NeedShutdownException:
            returncode = -9
            status = 'killed'
            msg.write('Error: JobScheduler shutdown')
        if self.sync and polled:
            self.logger.debug('Unregister poll: %d'%wfd)
            sock_poll.unregister(wfd)
            
        self.logger.info('Job %d finished with %d'%(self.jobid, returncode))
        msg = msg.getvalue()
        if msg:
            self.logger.debug('Message for job %d: %s'%(self.jobid, msg))
        # update job info
        cursor.execute('''UPDATE JobInfo SET status = ?, returncode = ?, msg = ? WHERE jobid = ?''',
                       (status, returncode, msg, self.jobid))
        dbcon.commit()
        dbcon.close()
        # cleanup runtime info
        with lock_runtime_info:
            del runtime_info[self.jobid]
        with cond_thread_exit:
            cond_thread_exit.notifyAll()
        # wake up JobScheduler
        with cond_queue:
            cond_queue.notifyAll()
       
        
class JobScheduler(threading.Thread):
    def __init__(self, maxjobs=4, wait_timeout=1):
        threading.Thread.__init__(self)
        self.maxjobs = maxjobs
        self.wait_timeout = wait_timeout
        self.workers = []
        self.logger = logging.getLogger('JobScheduler')
        
    def stop_workers(self):
        self.logger.info('Stopping all workers')
        for worker in self.workers:
            if worker.child:
                try:
                    worker.child.kill()
                    self.logger.warning('Killed process %d'%worker.child.pid)
                except OSError:
                    self.logger.warning('Failed to kill process %d'%worker.child.pid)
            worker.join()
        self.workers = []
        
    def cleanup_workers(self):
        new_workers = []
        for i in xrange(len(self.workers)):
            if not self.workers[i].isAlive():
                self.workers[i].join()
            else:
                new_workers.append(self.workers[i])
        self.workers = new_workers
            
    def wait_queue(self, dbcon):
        with cond_queue:
            while True:
                with lock_need_shutdown:
                    if need_shutdown:
                        raise NeedShutdownException()
                cursor = dbcon.cursor()
                cursor.execute('''SELECT COUNT(*) AS cnt FROM JobInfo WHERE status = 'running' AND sync == 0''')
                running_count = cursor.fetchone()['cnt']
                cursor.execute('''SELECT COUNT(*) AS cnt FROM JobInfo WHERE status = 'queued' AND sync == 0''')
                queued_count = cursor.fetchone()['cnt']
                #self.logger.debug('Running jobs: %d, queued jobs: %d'%(running_count, queued_count))
                if (running_count >= self.maxjobs) or (queued_count < 1):
                    cond_queue.wait(self.wait_timeout)
                else:
                    break
    def run(self):
        self.logger.info('Started (%d)'%self.ident)
        dbcon = sqlite3.connect(job_dbfile)
        dbcon.row_factory = sqlite3.Row
        while True:
            # wait for available slots and unempty queue
            try:
                self.wait_queue(dbcon)
            except NeedShutdownException:
                # stop
                break
            # run one job
            cursor = dbcon.cursor()
            cursor.execute('''SELECT * FROM JobInfo WHERE status = 'queued' AND sync = 0''')
            jobinfo = cursor.fetchone()
            if jobinfo:
                cursor.execute('''UPDATE JobInfo SET status = 'running' WHERE jobid = ?''',
                               (jobinfo['jobid'],))
                dbcon.commit()
                worker = JobWorker(jobinfo['jobid'], sync=False)
                worker.start()
                self.workers.append(worker)
            # cleanup finished workers
            self.cleanup_workers()

        # wait for all workers to finish
        for worker in self.workers:
            worker.join()
        self.workers = []
        dbcon.close()
        
            
class JobSchedServer(BaseHTTPServer.BaseHTTPRequestHandler):
    logger = logging.getLogger('JobSchedServer')
    def __init__(self, request, client_address, server):
        BaseHTTPServer.BaseHTTPRequestHandler.__init__(self, request, client_address, server)
        
    def print_info(self):
        path = urlparse.urlparse(self.path)
        self.wfile.write('scheme: %s\n'%path.scheme)
        self.wfile.write('path: %s\n'%path.path)
        self.wfile.write('netloc: %s\n'%path.netloc)
        self.wfile.write('query: %s\n'%path.query)
        self.wfile.write('hostname: %s\n'%path.hostname)
        self.wfile.write('port: %s\n'%path.port)
        
    def submit_handler(self):
        # parse form data
        data = {}
        if self.command == 'POST':
            import cgi
            form = cgi.FieldStorage(fp=self.rfile, headers=self.headers,
                                environ={'REQUEST_METHOD': 'POST', 
                                         'CONTENT_TYPE': self.headers['Content-Type']})
            for field in form:
                data[field] = form[field].value
        elif self.command == 'GET':
            query = urlparse.urlparse(self.path).query
            if query:
                query = urlparse.parse_qs(query)
                for field in query:
                    data[field] = query[field][0]
        else:
            return
        
        # check fields
        required_fields = ['command']
        fields_valid = True
        for field in required_fields:
            if field not in data:
                self.wfile.write('Error: field "%s" is required!\n'%field)
                fields_valid = False
        if not fields_valid:
            return
        if 'name' not in data:
            data['name'] = 'job'
        if 'group' not in data:
            data['group'] = 'group'
        if ('sync' in data) and (data['sync'] not in ('0', '1')):
            self.wfile.write('Error: field "sync" is either 0 or 1')
        else:
            data['sync'] = int(data['sync'])
        outlog = '' if 'outlog' not in data else data['outlog']
        errlog = '' if 'errlog' not in data else data['errlog']
        
        dbcon = sqlite3.connect(job_dbfile)
        dbcon.row_factory = sqlite3.Row
        cursor = dbcon.cursor()
        timestr = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        cursor.execute('''INSERT INTO JobInfo (name, jobgroup, command, status, returncode,
            sync, outlog, errlog, 
            msg, time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (data['name'], data['group'], data['command'], 'queued', 0,
            data['sync'], outlog, errlog,
            '', timestr))
        dbcon.commit()
        jobid = cursor.lastrowid
        dbcon.close()
        
        # run job
        with lock_runtime_info:
            runtime_info[jobid] = RuntimeInfo()
            if data['sync'] == 1:
                runtime_info[jobid].wfd = self.wfile.fileno()
                
        if data['sync'] == 1:
            # wait for worker to finish
            worker = JobWorker(jobid, sync=True, wfile=self.wfile)
            worker.start()
            worker.join()
            #self.run_job_sync(jobid)
        else:
            # notify the JobScheduler
            with cond_queue:
                cond_queue.notifyAll()
        
    def jobinfo_handler(self):
        data = {}
        query = urlparse.urlparse(self.path).query
        if query:
            query = urlparse.parse_qs(query)
            for field in query:
                data[field] = query[field][0]
        
        dbcon = sqlite3.connect(job_dbfile)
        dbcon.row_factory = sqlite3.Row
        cursor = dbcon.cursor()
        
        if 'jobid' in data:
            self.logger.debug('Jobinfo: %s'%data['jobid'])
            jobids = data['jobid'].split(',')
            cursor.execute('SELECT * FROM JobInfo WHERE jobid IN (%s)'%(','.join(['\'%s\''%jobid for jobid in jobids])))
        else:
            cursor.execute('''SELECT * FROM JobInfo WHERE status in ('running', 'queued')''')
        fields_shown = None
        if data.get('fields'):
            fields_all = zip(*cursor.description)[0]
            if col == 'all':
                fields_shown = fields_all
            else:
                fields_shown = data['fields'].split(',')
                for f in fields_shown:
                    if f not in fields_all:
                        self.wfile.write('Error: field %s does not exist\n')
                        return
        else:
            fields_shown = ('jobid', 'name', 'jobgroup', 'status', 'returncode', 
                          'msg', 'time', 'command')
        self.wfile.write('\t'.join(fields_shown) + '\n')
        for row in cursor.fetchall():
            self.wfile.write('\t'.join([str(row[key]) for key in fields_shown]) + '\n')
        dbcon.close()
        
    def shutdown_handler(self):
        self.wfile.write('Shutting down server\n')
        self.server.shutdown()
        
    def help_handler(self):
        pass
    
    def kill_handler(self):
        data = self.parse_query(['jobid'])
        if not data:
            return
        jobid = int(data['jobid'])
        
        dbcon = sqlite3.connect(job_dbfile)
        dbcon.row_factory = sqlite3.Row
        cursor = dbcon.cursor()
        jobinfo = None
        with lock_queue:
            cursor.execute('''SELECT * FROM JobInfo WHERE jobid = ?''',
                data['jobid'])
            jobinfo = cursor.fetchall()
            if not jobinfo:
                self.wfile.write('Error: job %d does not exit\n'%jobid)
                dbcon.close()
                return
            jobinfo = jobinfo[0]
        success = True
        with lock_runtime_info:
            if jobid in runtime_info:
                KillJob(runtime_info[jobid].pid)
            elif jobinfo['status'] == 'queued':
                with cond_queue:
                    cursor.execute('''UPDATE JobInfo SET status = 'killed' WHERE jobid = ?''',
                               jobid)
                    dbcon.commit()
                    cond_queue.notifyAll()
            else:
                self.wfile.write('Error: job %d has finished\n'%jobid)
                success = False
        if success:
            self.wfile.write('Job %d has been killed\n'%data['jobid'])
                
        dbcon.close()
        
        
    def parse_query(self, required_fields=[]):
        data = {}
        if self.command == 'POST':
            form = cgi.FieldStorage(fp=self.rfile, headers=self.headers,
                                environ={'REQUEST_METHOD': 'POST', 
                                         'CONTENT_TYPE': self.headers['Content-Type']})
            for field in form:
                data[field] = form[field].value
        elif self.command == 'GET':
            query = urlparse.urlparse(self.path).query
            if query:
                query = urlparse.parse_qs(query)
                for field in query:
                    if len(query[field]) == 1:
                        data[field] = query[field][0]
                    else:
                        data[field] = query[field]
        else:
            return None
         
        # check fields
        fields_valid = True
        for field in required_fields:
            if field not in data:
                self.wfile.write('Error: field "%s" is required!\n'%field)
                fields_valid = False
        if not fields_valid:
            return None
        
        return data
        
    def do_GETPOST(self):
        path = urlparse.urlparse(self.path)
        
        handlers = {}
        handlers['/submit'] = self.submit_handler
        handlers['/jobinfo'] = self.jobinfo_handler
        handlers['/shutdown'] = self.shutdown_handler
        handlers['/print_info'] = self.print_info
        handlers['/help'] = self.help_handler
        handlers['/kill'] = self.kill_handler
        
        if path.path in handlers:
            self.send_response(200)
            self.end_headers()
            handlers[path.path]()
        else:
            self.send_error(404)
         
    def do_GET(self):
        #self.logger.debug('GET %s %s'%(self.headers['Host'], self.path))
        self.do_GETPOST()
            
    def do_POST(self):
        #self.logger.debug('POST %s %s'%(self.headers['Host'], self.path))
        self.do_GETPOST()
            
class ComputeNode:
    def __init__(self, name='Node', address='127.0.0.1', port=4321,
                 maxjobs=4, dbfile='jobs.sqlite'):
        self.name = name
        self.address = address
        self.port = port
        self.maxjobs = maxjobs
        self.dbfile = dbfile
        self.logger = logging.getLogger(name)
        
        # threading synchronization objects
        self.lock_queue = threading.Lock()
        self.cond_queue = threading.Condition(lock_queue)
        # lock when accessing runtime_info
        self.lock_runtime_info = threading.Lock()
        self.cond_thread_exit = threading.Condition(lock_runtime_info)
        # dict of RuntimeInfo object indexed by jobid
        self.runtime_info = {}
        
        self.lock_need_shutdown = threading.Lock()
        self.need_shutdown = False
        # signal when an thread exits
        self.sock_poll = select.poll()
        
        
    def connect_db(self):
        dbcon = sqlite3.connect(self.dbfile)
        dbcon.row_factory = sqlite3.Row
        return dbcon
    
    def init_db(self):
        dbcon = sqlite3.connect(self.dbfile)
        cursor = dbcon.cursor()
        cursor.execute('''DROP TABLE IF EXISTS JobInfo''')
        cursor.execute('''CREATE TABLE JobInfo
            (jobid INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT, jobgroup TEXT, command TEXT,
            status TEXT, 
            returncode INTEGER,
            sync INTEGER,
            outlog TEXT,
            errlog TEXT,
            msg TEXT,
            time TEXT)''')
        dbcon.commit()
        dbcon.close()
    
    def clear_old_jobs(self):
        cursor = self.dbcon.cursor()
        cursor.execute('''UPDATE JobInfo SET status = 'killed' WHERE status in ('running', 'queued')''')
        self.dbcon.commit()
    
    def start(self):
        if not os.path.exists(self.dbfile):
            logger.info('Initializing job database ...')
            self.init_db()
        self.dbcon = self.connect_db()
        logger.info('Clearing unfinished jobs since last run')
        self.clear_old_jobs()
        logger.info('Starting ConnectionMonitor')
        self.conmon = ConnectMonitor()
        self.conmon.start()
        logger.info('Starting JobScheduler')
        self.jobsched = JobScheduler(maxjobs=self.maxjobs)
        self.jobsched.start()
        # start server
        SocketServer.TCPServer.allow_reuse_address = True
        self.server = SocketServer.ThreadingTCPServer((self.address, self.port), JobSchedServer)
        
        self.logger.info('JobSchedServer serving at %s:%d'%(self.address, self.port))
        self.server.serve_forever()
        
    def stop(self):
        with self.lock_need_shutdown:
            self.need_shutdown = True
        # kill all running jobs
        self.logger.info('Clearing all running jobs')
        self.clear_jobs()
    
        self.logger.info('Shutting down the server')
        self.server.shutdown()
        self.logger.info('Stopping JobScheduler')
        with cond_queue:
            cond_queue.notifyAll()
        self.jobsched.join()
        self.logger.info('Stopping ConnectionMonitor')
        self.conmon.join()
        
    def clear_jobs(self, timeout=1):
        with self.lock_runtime_info:
            for jobid, rtinfo in self.runtime_info.iteritems():
                if rtinfo.pid > 0:
                    if KillJob(rtinfo.pid):
                        self.logger.warning('Killed job %d (pid: %d)'%(jobid, rtinfo.pid))
                    else:
                        self.logger.warning('Failed to kill job %d (pid: %d)'%(jobid, rtinfo.pid))
        # wait for all workers to finish
        with self.cond_thread_exit:
            while len(self.runtime_info) > 0:
                self.cond_thread_exit.wait(timeout)
        
        
        
def main():
    parser = argparse.ArgumentParser('Simple HTTP proxy server')
    parser.add_argument('-a', '--addr', type=str, required=False,
                        default='127.0.0.1', help='Address to listen on')
    parser.add_argument('-p', '--port', type=int, required=True, default=None,
                        help='Port to listen to')
    parser.add_argument('-d', '--dbfile', type=str, required=False, default='jobs.sqlite',
                        help='SQLite database containing job information')
    parser.add_argument('-j', '--jobs', type=int, required=False, default=4,
                        help='Maximum number of jobs to run in parallel')
    parser.add_argument('-c', '--clear', action='store_true', required=False,
                        default=False, help='Clear the job database')
    
    args = parser.parse_args()
    
    if not args.port:
        args.port = rng.randint(2000, 30000)
    
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger('main')
    
    global job_dbfile
    job_dbfile = args.dbfile
    #if os.path.exists(args.dbfile):
    #    os.unlink(args.dbfile)
    if args.clear or (not os.path.exists(args.dbfile)):
        logger.info('Initializing job database ...')
        InitJobDB(args.dbfile)
    logger.info('Clearing unfinished jobs since last run')
    ClearOldJobs()
    logger.info('Starting ConnectionMonitor')
    conmon = ConnectionMonitor()
    conmon.start()
    logger.info('Starting JobScheduler')
    jobsched = JobScheduler(maxjobs=args.jobs)
    jobsched.start()
    # start server
    SocketServer.TCPServer.allow_reuse_address = True
    httpd = SocketServer.ThreadingTCPServer((args.addr, args.port), JobSchedServer)
    try:
        logger.info('JobSchedServer serving at %s:%d'%(args.addr, args.port))
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    except NeedShutdownException:
        pass
    # shutdown
    global need_shutdown
    need_shutdown = True
    # kill all running jobs
    logger.info('Clearing all running jobs')
    ClearJobs(logger)
    
    logger.info('Shutting down the server')
    httpd.shutdown()
    logger.info('Stopping JobScheduler')
    with cond_queue:
        cond_queue.notifyAll()
    jobsched.join()
    logger.info('Stopping ConnectionMonitor')
    conmon.join()
    
    logging.shutdown()
    
if __name__ == '__main__':
    main()
