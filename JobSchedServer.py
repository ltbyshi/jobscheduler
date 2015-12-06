#! /usr/bin/env python

import SocketServer
import argparse
import random
import time
import urlparse
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
        self.tid = -1
        self.killed = False
        
# dict of RuntimeInfo object indexed by jobid
runtime_info = dict()
# threading synchronization objects
cond_queue = threading.Condition()
lock_runtime_info = threading.Lock()

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
        
def InitJobDB(dbfile):
    conn = sqlite3.connect(dbfile)
    cursor = conn.cursor()
    cursor.execute('''DROP TABLE IF EXISTS JobInfo''')
    cursor.execute('''CREATE TABLE JobInfo
        (jobid INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT, command TEXT,
        status TEXT, 
        returncode INTEGER,
        sync INTEGER,
        outlog TEXT,
        errlog TEXT,
        msg TEXT,
        time TEXT)''')
    conn.commit()
    conn.close()
    
def KillJob(pid):
    try:
        os.kill(pid, signal.SIGKILL)
        return True
    except OSError:
        return False

class ConnectionMonitor(threading.Thread):
    def __init__(self, wait_timeout=200):
        threading.Thread.__init__(self)
        self.last_event = (0, 0, 0)
        self.stoppable = False
        self.wait_timeout = wait_timeout
        
    def parse_event(self, event):
        result = []
        for e, s in strevent:
            if (e & event):
                result.append(s)
        return result
                
    def process_event(self, fd, event):
        #print '[ConnectionMonitor] Event on %d'%fd
        for jobid, rtinfo in runtime_info.iteritems():
            if rtinfo.wfd == fd:
                break
        pid = rtinfo.pid
        if (fd, event) != self.last_event:
             self.last_event = (fd, event)
             eventstr = ','.join(self.parse_event(event))
             print >>sys.stderr, '[ConMon] fd: %d, pid: %d, events: %s'%(fd, pid, eventstr)
        if (event & select.POLLIN):
            if not rtinfo.killed:
                if KillJob(rtinfo.pid):
                    print '[ConMon]: killed process %d'%rtinfo.pid
                    with lock_runtime_info:
                        runtime_info[jobid].killed = True
                else:
                    print '[ConMon]: failed to kill process %d'%rtinfo.pid
                        
    def run(self):
        print '[ConnectionMonitor] started (%d)'%threading.currentThread().ident
        dbcon = sqlite3.connect(job_dbfile)
        cursor = dbcon.cursor()
        while not self.stoppable:
            res = sock_poll.poll(self.wait_timeout)
            for fd, event in res:
                self.process_event(fd, event)
        dbcon.close()
  
class JobWorker(threading.Thread):
    def __init__(self, jobid, sync=True, wfile=None):
        threading.Thread.__init__(self)
        self.jobid = jobid
        self.sync = sync
        self.wfile = wfile
        
    def run(self):
        print '[JobWorker] Started, JobId:', self.jobid
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
            p = subprocess.Popen(['bash', '-c', jobinfo['command']],
                             stdin=subprocess.PIPE,
                             stdout=stdout,
                             stderr=stderr,
                             shell=False)
            # update PID
            with lock_runtime_info:
                runtime_info[self.jobid].pid = p.pid
            # register connection monitor
            if self.sync:
                print '[JobWorker] register poll: %d'%wfd
                sock_poll.register(wfd, select.POLLIN)
                polled = True
            p.communicate('')
            returncode = p.returncode
            if returncode == -9:
                status = 'killed'
            else:
                status = 'success'
        except OSError as e:
            msg.write('Error: failed to run the command: %s\n'%e.strerror)
            returncode = -e.errno
            status = 'failure'
        except IOError as e:
            msg.write('Error: cannot open the file: %s\n'%e.strerror)
            returncode = -e.errno
            status = 'failure'
        except JobException as e:
            msg.write('Error: %s\n'%e.msg)
            returncode = 100
            status = 'failure'
        if self.sync and polled:
            print '[JobWorker] unregister poll: %d'%wfd
            sock_poll.unregister(wfd)
            
        msg = msg.getvalue()
        if msg:
            print '[JobWorker] message:'
            print msg
        # update job info
        cursor.execute('''UPDATE JobInfo SET status = ?, returncode = ?, msg = ? WHERE jobid = ?''',
                       (status, returncode, msg, self.jobid))
        dbcon.commit()
        dbcon.close()
        # cleanup runtime info
        with lock_runtime_info:
            del runtime_info[self.jobid]
        
        # wake up JobScheduler
        with cond_queue:
            cond_queue.notifyAll()
       
        
class JobScheduler(threading.Thread):
    def __init__(self, maxjobs=4, wait_timeout=0.1):
        threading.Thread.__init__(self)
        self.stoppable = False
        self.maxjobs = maxjobs
        self.wait_timeout = wait_timeout
        
    def stop_workers(self):
        print '[JobScheduler] Stop all workers'
        with lock_runtime_info:
            for rtinfo in runtime_info.itervalues():
                if KillJob(rtinfo.pid):
                    print '[JobScheduler]: Killed process %d'%rtinfo.pid
                else:
                    print '[JobScheduler]: Failed to kill process %d'%rtinfo.pid
         
    def run(self):
        print '[JobScheduler] Started (%d)'%threading.currentThread().ident
        dbcon = sqlite3.connect(job_dbfile)
        dbcon.row_factory = sqlite3.Row
        workers = []
        while not self.stoppable:
            cond_queue.acquire()
            # wait for available slots
            jobinfo = None
            while (not self.stoppable):
                cursor = dbcon.cursor()
                cursor.execute('''SELECT COUNT(*) AS cnt FROM JobInfo WHERE status = 'running' AND sync == 0''')
                running_count = cursor.fetchone()['cnt']
                cursor.execute('''SELECT COUNT(*) AS cnt FROM JobInfo WHERE status = 'queued' AND sync == 0''')
                queued_count = cursor.fetchone()['cnt']
                if (running_count >= self.maxjobs) or (queued_count < 1):
                    cond_queue.wait(self.wait_timeout)
                else:
                    break
            cond_queue.release()
            # stop
            if self.stoppable:
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
                workers.append(worker)
            # cleanup workers
            dead_workers = []
            for i in xrange(len(workers)):
                if not workers[i].isAlive():
                    workers[i].join()
                    dead_workers.append(i)
            for i in dead_workers:
                del workers[i]
                   
        self.stop_workers()
        dbcon.close()
        
            
class JobMonitor(BaseHTTPServer.BaseHTTPRequestHandler):
    def run_job_sync(self, jobid):
        conn = sqlite3.connect(job_dbfile)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM JobInfo WHERE jobid = ?', (jobid,))
        row = cursor.fetchone()
        returncode = 0
        if row:
            status = None
            wfd = None
            script_file = row[2]
            cursor.execute('UPDATE JobInfo SET status = ? WHERE jobid = ?',
                       ('running', jobid))
            conn.commit()
            try:
                print 'Run command:',script_file
                p = subprocess.Popen(['bash', '-c', script_file], 
                                 stdin=subprocess.PIPE,
                                 stdout=self.wfile, stderr=self.wfile, shell=False)
                wfd = self.rfile.fileno()
                sock_poll.register(wfd)
                sock_pid[wfd] = p.pid
                p.communicate()
                returncode = p.returncode
                if returncode == 0:
                    status = 'success'
                elif returncode == -9:
                    status = 'killed'
                else:
                    status = 'failure'
            except subprocess.CalledProcessError as e:
                sys.stderr.write('CalledProcessError: exit code: %d\n'%e.returncode)
                sys.stderr.write('Command line: %s\n'%e.cmd)
                status = 'failed'
                returncode = -1
            if wfd:
                sock_poll.unregister(wfd)
            #print self.rfile.read(1024)
            #self.wfile.write('Program exited\n')
            cursor.execute('UPDATE JobInfo SET status = ? WHERE jobid = ?',
                       (status, jobid))
            cursor.execute('UPDATE JobInfo SET returncode = ? WHERE jobid = ?',
                       (returncode, jobid))
            conn.commit()
            
        conn.close()
        return returncode

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
        required_fields = ['name', 'command']
        fields_valid = True
        for field in required_fields:
            if field not in data:
                self.wfile.write('Error: field "%s" is required!\n'%field)
                fields_valid = False
        if not fields_valid:
            return
        if ('sync' in data) and (data['sync'] not in ('0', '1')):
            self.wfile.write('Error: field "sync" is either 0 or 1')
        else:
            data['sync'] = int(data['sync'])
        outlog = '' if 'outlog' not in data else data['outlog']
        errlog = '' if 'errlog' not in data else data['errlog']
        
        dbcon = sqlite3.connect(job_dbfile)
        cursor = dbcon.cursor()
        timestr = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        cursor.execute('''INSERT INTO JobInfo (name, command, status, returncode,
            sync, outlog, errlog, 
            msg, time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (data['name'], data['command'], 'queued', 0,
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
        
        #self.wfile.write('Job %d has been submitted\n'%jobid)
        #self.wfile.write('Output: %s\n'%result[0])
        #self.wfile.write(result[0])
        
        # for debugging
        """
        cursor.execute('SELECT * FROM JobInfo WHERE jobid == ?', (jobid,))
        row = cursor.fetchone()
        if row:
            self.wfile.write('JobInfo: ' + ', '.join([str(v) for v in row]) + '\n')
        """
        
    def jobinfo_handler(self):
        data = {}
        query = urlparse.urlparse(self.path).query
        if query:
            query = urlparse.parse_qs(query)
            for field in query:
                data[field] = query[field][0]
        
        self.wfile.write('Job information\n')
        conn = sqlite3.connect(job_dbfile)
        cursor = conn.cursor()
        
        if 'jobid' in data:
            cursor.execute('SELECT * FROM JobInfo WHERE jobid = ?', (data['jobid'],))
        else:
            cursor.execute('SELECT * FROM JobInfo')
        self.wfile.write('\t'.join(zip(*cursor.description)[0]) + '\n')
        for row in cursor.fetchall():
            self.wfile.write('\t'.join([str(v) for v in row]) + '\n')
        conn.close()
        
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
        if jobid in runtime_info:
            KillJob(runtime_info[jobid].pid)
        else:
            self.wfile.write('Error: jobid %d does not exist\n'%jobid)
        
        
    def parse_query(self, required_fields=[]):
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
        print '[GET]', self.headers['Host'], self.path
        
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
        print >>sys.stderr, '[GET]', self.headers['Host'], self.path
        self.do_GETPOST()
            
    def do_POST(self):
        print >>sys.stderr, '[POST]', self.headers['Host'], self.path
        self.do_GETPOST()
            
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
    
    args = parser.parse_args()
    
    if not args.port:
        args.port = rng.randint(2000, 30000)
        
    global job_dbfile
    job_dbfile = args.dbfile
    if not os.path.exists(args.dbfile):
        print 'Initializing job database ...'
        InitJobDB(args.dbfile)
    
    print 'Starting ConnectionMonitor'
    conmon = ConnectionMonitor()
    conmon.start()
    print 'Starting JobScheduler'
    jobsched = JobScheduler();
    jobsched.start()
    
    SocketServer.TCPServer.allow_reuse_address = True
    httpd = SocketServer.ThreadingTCPServer((args.addr, args.port), JobMonitor)
    try:
        print "JobMonitor serving at %s:%d"%(args.addr, args.port)
        httpd.serve_forever()
    except KeyboardInterrupt:
        print 'Shutting down the server'
        httpd.shutdown()
    print 'Stopping JobScheduler'
    jobsched.stoppable = True
    jobsched.join()
    print 'Stopping ConnectionMonitor'
    conmon.stoppable = True
    conmon.join()
    
    #conmon.join()
    #os.kill(conmon.getident(), 9)
    
if __name__ == '__main__':
    main()