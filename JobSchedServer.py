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


class RuntimeInfo:
    def __init__(self):
        self.pid = -1
        self.wfd = None
        self.killed = False
        
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

class ParamError(Exception):
    def __init__(self, msg):
        Exception.__init__(self)
        self.msg = msg
        
    
def KillJob(pid):
    try:
        os.kill(pid, signal.SIGKILL)
        return True
    except OSError:
        return False

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
        for jobid, rtinfo in self.node.runtime_info.iteritems():
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
                    with self.node.lock_runtime_info:
                        self.node.runtime_info[jobid].killed = True
                else:
                    self.logger.warning('Failed to kill process %d'%rtinfo.pid)
                        
    def run(self):
        self.logger.info('Started')
        while True:
            with self.node.lock_need_shutdown:
                if self.node.need_shutdown:
                    break
            res = self.node.sock_poll.poll(self.wait_timeout)
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
        dbcon = self.node.connect_db()
        dbcon.row_factory = sqlite3.Row
        cursor = dbcon.cursor()
        status = ''
        returncode = 0
        msg = StringIO()
        polled = False
        wfd = self.node.runtime_info[self.jobid].wfd
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
            if self.node.need_shutdown:
                raise NeedShutdownException()
            with self.node.lock_runtime_info:
                self.child = subprocess.Popen(['bash', '-c', jobinfo['command']],
                             stdin=subprocess.PIPE,
                             stdout=stdout,
                             stderr=stderr,
                             shell=False)
                # update PID
                self.node.runtime_info[self.jobid].pid = self.child.pid
            # register connection monitor
            if self.sync:
                self.node.sock_poll.register(wfd, select.POLLIN)
                polled = True
                self.logger.debug('Registered poll: %d'%wfd)
            # wait for job finish
            self.child.communicate('')
            with self.node.lock_runtime_info:
                self.node.runtime_info[self.jobid].pid = -1
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
            self.node.sock_poll.unregister(wfd)
            
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
        with self.node.lock_runtime_info:
            del self.node.runtime_info[self.jobid]
        with self.node.cond_thread_exit:
            self.node.cond_thread_exit.notifyAll()
        # wake up JobScheduler
        with self.node.cond_queue:
            self.node.cond_queue.notifyAll()
       
        
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
        with self.node.cond_queue:
            while True:
                with self.node.lock_need_shutdown:
                    if self.node.need_shutdown:
                        raise NeedShutdownException()
                cursor = dbcon.cursor()
                cursor.execute('''SELECT COUNT(*) AS cnt FROM JobInfo WHERE status = 'running' AND sync == 0''')
                running_count = cursor.fetchone()['cnt']
                cursor.execute('''SELECT COUNT(*) AS cnt FROM JobInfo WHERE status = 'queued' AND sync == 0''')
                queued_count = cursor.fetchone()['cnt']
                #self.logger.debug('Running jobs: %d, queued jobs: %d'%(running_count, queued_count))
                if (running_count >= self.maxjobs) or (queued_count < 1):
                   self.node.cond_queue.wait(self.wait_timeout)
                else:
                    break
    def run(self):
        self.logger.info('Started')
        dbcon = self.node.connect_db()
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
                worker.node = self.node
                worker.start()
                self.workers.append(worker)
            # cleanup finished workers
            self.cleanup_workers()

        # wait for all workers to finish
        for worker in self.workers:
            worker.join()
        self.workers = []
        dbcon.close()
        
class Parameter(object):
    def __init__(self, **kwargs):
        self.__dict__.update(**kwargs)
        
class JobSchedServer(BaseHTTPServer.BaseHTTPRequestHandler):
    logger = logging.getLogger('JobSchedServer')
    def __init__(self, request, client_address, server):
        BaseHTTPServer.BaseHTTPRequestHandler.__init__(self, request, client_address, server)
        self.setup_handlers()
        self.node = self.server.node
        
    def setup(self):
        self.setup_handlers()
        self.node = self.server.node
        BaseHTTPServer.BaseHTTPRequestHandler.setup(self)
        
    def setup_handlers(self):
        self.optionmap = {
        'submit': {
            '_handler': self.submit_handler,
            '_help': 'submit a job. The job id will display in the first line after submission.',
            'name': {'type': str, 'required': False, 'default': 'job', 'help': 'job name'},
            'command': {'type': str, 'required': True, 'help': 'shell command string'},
            'sync': {'type': int, 'required': False, 'default': 1, 'help': 'if sync=1, the client waits until the job is finished'},
            'group': {'type': str, 'required': False, 'default': 'default', 'help': 'job group name'},
            'errlog': {'type': str, 'required': False, 'help': 'filename where stderr is logged'},
            'outlog': {'type': str, 'required': False, 'help': 'filename where stdout is logged'} 
         },
        'jobinfo': {
            '_handler': self.jobinfo_handler,
            '_help': 'display job information',
            'all': {'type': int, 'required': False, 'help': 'all=1 will show all jobs'},
            'jobid': {'type': str, 'required': False, 'help': 'a comma-separated job ids to filter'},
            'name': {'type': str, 'required': False, 'help': 'a comma-separated names to filter'},
            'group': {'type': str, 'required': False, 'help': 'a comma-separated group names to filter'},
            'status': {'type': str, 'required': False, 'help': 'a comma-separated status to filter'},
            'fields': {'type': str, 'required': False, 
                       'default': 'jobid,name,jobgroup,status,returncode,msg,time,command',
                       'help': 'a comma-seperated list of field names'}
         },
        'kill': {
            '_handler': self.kill_handler,
            '_help': 'cancel jobs',
            'jobid': {'type': str, 'required': True, 'help': 'a comma-separated list of job ids'} 
         },
        'wait': {
            '_handler': self.wait_handler,
            '_help': 'wait for a job to finish',
            'jobid': {'type': int, 'required': True, 'help': 'a single job id'}
         },
        'shutdown': {
            '_handler': self.shutdown_handler,
            '_help': 'cancel all jobs and shutdown server',
         },
        'help': {
            '_handler': self.help_handler,
            '_help': 'display help',
            'topic': {'type': str, 'required': False}
         }
        }
        self.typestr = {int: 'integer', str: 'string', float: 'floating-point number'}
        
    def create_job_filter(self, data, fields=None):
        '''filter jobs by jobid, name, group and status
        '''
        sql = StringIO()
        fields = fields_all
        sql.write('SELECT ')
        if fields:
            sql.write(','.join(fields))
        else:
            sql.write('*')
        sql.write(' FROM JobInfo')
        if not data.get('all'):
            nfilters = 0
            sql.write(' WHERE')
            if data.get('jobid'):
                sql.write(' jobid IN (')
                sql.write(','.join([repr(jobid) for jobid in data['jobid'].split(',')]))
                sql.write(')')
                nfilters += 1
            if data.get('name'):
                if nfilters > 0:
                    sql.write(' AND')
                sql.write(' name IN (')
                sql.write(','.join([repr(name) for name in data['name'].split(',')]))
                sql.write(')')
                nfilters += 1
            if data.get('group'):
                if nfilters > 0:
                    sql.write(' AND')
                sql.write(' jobgroup IN (')
                sql.write(','.join([repr(group) for group in data['group'].split(',')]))
                sql.write(')')
                nfilters += 1
            if data.get('status'):
                if nfilters > 0:
                    sql.write(' AND')
                sql.write(' status IN (')
                sql.write(','.join([repr(status) for status in data['status'].split(',')]))
                sql.write(')')
                nfilters += 1
            if nfilters == 0:
                # default filter
                sql.write(''' status IN ('running','queued')''')
        return sql.getvalue()
    
    def print_info(self, data):
        path = urlparse.urlparse(self.path)
        self.wfile.write('scheme: %s\n'%path.scheme)
        self.wfile.write('path: %s\n'%path.path)
        self.wfile.write('netloc: %s\n'%path.netloc)
        self.wfile.write('query: %s\n'%path.query)
        self.wfile.write('hostname: %s\n'%path.hostname)
        self.wfile.write('port: %s\n'%path.port)
        
    def submit_handler(self, data):        
        dbcon = self.node.connect_db()
        dbcon.row_factory = sqlite3.Row
        cursor = dbcon.cursor()
        timestr = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        cursor.execute('''INSERT INTO JobInfo (name, jobgroup, command, status, returncode,
            sync, outlog, errlog, 
            msg, time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (data['name'], data['group'], data['command'], 'queued', 0,
            data['sync'], data['outlog'], data['errlog'],
            '', timestr))
        dbcon.commit()
        jobid = cursor.lastrowid
        dbcon.close()
        
        # run job
        with self.node.lock_runtime_info:
            self.node.runtime_info[jobid] = RuntimeInfo()
            if data['sync'] == 1:
                self.node.runtime_info[jobid].wfd = self.wfile.fileno()
        self.wfile.write('%s\n'%jobid)
        if data['sync'] == 1:
            # wait for worker to finish
            worker = JobWorker(jobid, sync=True, wfile=self.wfile)
            worker.node = self.node
            worker.start()
            worker.join()
            #self.run_job_sync(jobid)
        else:
            # notify the JobScheduler
            with self.node.cond_queue:
                self.node.cond_queue.notifyAll()
        
    def jobinfo_handler(self, data):
        dbcon = self.node.connect_db()
        dbcon.row_factory = sqlite3.Row
        cursor = dbcon.cursor()
        cursor.execute('''SELECT * FROM JobInfo''')
        fields_all = zip(*cursor.description)[0]
        
        sql = StringIO()
        fields = fields_all
        if data.get('fields'):
            fields = data['fields'].split(',')
            for f in fields:
                if f not in fields_all:
                    self.wfile.write('Error: field %s does not exist\n')
                    return
            sql.write('SELECT ')
            sql.write(','.join(fields))
            sql.write(' FROM JobInfo')
        if not data.get('all'):
            nfilters = 0
            sql.write(' WHERE')
            if data.get('jobid'):
                sql.write(' jobid IN (')
                sql.write(','.join([repr(jobid) for jobid in data['jobid'].split(',')]))
                sql.write(')')
                nfilters += 1
            if data.get('name'):
                if nfilters > 0:
                    sql.write(' AND')
                sql.write(' name IN (')
                sql.write(','.join([repr(name) for name in data['name'].split(',')]))
                sql.write(')')
                nfilters += 1
            if data.get('group'):
                if nfilters > 0:
                    sql.write(' AND')
                sql.write(' jobgroup IN (')
                sql.write(','.join([repr(group) for group in data['group'].split(',')]))
                sql.write(')')
                nfilters += 1
            if data.get('status'):
                if nfilters > 0:
                    sql.write(' AND')
                sql.write(' status IN (')
                sql.write(','.join([repr(status) for status in data['status'].split(',')]))
                sql.write(')')
                nfilters += 1
            if nfilters == 0:
                # default filter
                sql.write(''' status IN ('running','queued')''')
        
        self.logger.debug(sql.getvalue())
        cursor.execute(sql.getvalue())
        self.wfile.write('\t'.join(fields) + '\n')
        for row in cursor.fetchall():
            self.wfile.write('\t'.join([str(row[key]) for key in fields]) + '\n')
        dbcon.close()
        
    def shutdown_handler(self, data):
        self.wfile.write('Shutting down server\n')
        self.server.shutdown()
        
    def help_handler(self, data):
        if data.get('topic'):
            topic = data['topic']
            if topic not in self.optionmap:
                self.wfile.write('command %s is not found\n'%data[topic])
                return
            self.wfile.write('%s - %s\n'%(topic, self.optionmap[topic]['_help']))
            self.wfile.write('Parameters:\n')
            for param, desc in self.optionmap[topic].iteritems():
                if not param.startswith('_'):
                    self.wfile.write('  %-9s '%param)
                    if 'help' in desc:
                        self.wfile.write(desc['help'] + '. ' )
                    if desc.get('required'):
                        self.wfile.write('Required. ')
                    if 'default' in desc:
                        self.wfile.write('(default is %s)'%repr(desc['default']))
                    self.wfile.write('\n')
                    
        else:
            self.wfile.write('Available commands: %s\n'%(' '.join(self.optionmap.keys())))
        
    def wait_handler(self, data):
        jobid = int(data['jobid'])
        dbcon = self.node.connect_db()
        dbcon.row_factory = sqlite3.Row
        cursor = dbcon.cursor()
        jobinfo = None
        with self.node.cond_queue:
            while True:
                cursor.execute('''SELECT * FROM JobInfo WHERE jobid = ?''',
                    (data['jobid'],))
                jobinfo = cursor.fetchall()
                if not jobinfo:
                    self.wfile.write('Error: job %d does not exit\n'%jobid)
                    break
                jobinfo = jobinfo[0]
                if jobinfo['status'] in ('running', 'queued'):
                    self.node.cond_queue.wait()
                else:
                    break
        dbcon.close()
    
    def kill_handler(self, data):
        for jobid in data['jobid'].split(','):
            try:
                jobid = int(jobid)
                self.node.kill_job(jobid)
                self.wfile.write('Job %s has been killed\n'%jobid)
            except JobException as e:
                self.wfile.write('Error: ' + e.msg + '\n')
            except ValueError:
                self.wfile.write('Error: cannot convert %s to integer'%jobid)
        
        
    def parse_query(self, command):
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
         
        for param, desc in self.optionmap[command].iteritems():
            if not param.startswith('_'):
                if desc.get('required') and (param not in data):
                    raise ParamError('param "%s" is required\n'%param)
                try:
                    if param in data:
                        data[param] = desc['type'](data[param])
                    elif 'default' in desc:
                        data[param] = desc['default']
                    else:
                        data[param] = None
                except ValueError:
                    raise ParamError('cannot convert param "%s" to %s'%(param, self.typestr[desc['type']]))
                
        return data
        
    def do_GETPOST(self):
        path = urlparse.urlparse(self.path)
        command = path.path.strip('/')
        if command in self.optionmap:
            self.send_response(200)
            self.end_headers()
            try:
                data = self.parse_query(command)
                self.optionmap[command]['_handler'](data)
            except ParamError as e:
                self.wfile.write('Error: %s\n'%e.msg)
        else:
            self.send_error(404)
         
    def do_GET(self):
        #self.logger.debug('GET %s %s'%(self.headers['Host'], self.path))
        self.do_GETPOST()
            
    def do_POST(self):
        #self.logger.debug('POST %s %s'%(self.headers['Host'], self.path))
        self.do_GETPOST()
            
class ComputeNode:
    def __init__(self, name, address, port, maxjobs, dbfile):
        self.name = name
        self.address = address
        self.port = port
        self.maxjobs = maxjobs
        self.dbfile = dbfile
        self.logger = logging.getLogger(name)
        
        # threading synchronization objects
        self.lock_queue = threading.Lock()
        self.cond_queue = threading.Condition(self.lock_queue)
        # lock when accessing runtime_info
        self.lock_runtime_info = threading.Lock()
        self.cond_thread_exit = threading.Condition(self.lock_runtime_info)
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
        dbcon = self.connect_db()
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
            self.logger.info('Initializing job database ...')
            self.init_db()
        self.dbcon = self.connect_db()
        self.logger.info('Clearing unfinished jobs since last run')
        self.clear_old_jobs()
        self.logger.info('Starting ConnectionMonitor')
        self.conmon = ConnectionMonitor()
        self.conmon.node = self
        self.conmon.start()
        self.logger.info('Starting JobScheduler')
        self.jobsched = JobScheduler(maxjobs=self.maxjobs)
        self.jobsched.node = self
        self.jobsched.start()
        # start server
        SocketServer.TCPServer.allow_reuse_address = True
        self.server = SocketServer.ThreadingTCPServer((self.address, self.port), JobSchedServer)
        self.server.node = self
        try:
            self.logger.info('JobSchedServer serving at %s:%d'%(self.address, self.port))
            self.server.serve_forever()
        except KeyboardInterrupt:
            pass
        except NeedShutdownException:
            pass
        
    def stop(self):
        self.logger.info('Stopping Node %s'%self.name)
        with self.lock_need_shutdown:
            self.need_shutdown = True
        # kill all running jobs
        self.logger.info('Clearing all running jobs')
        self.clear_jobs()
    
        self.logger.info('Shutting down the server')
        self.server.shutdown()
        self.logger.info('Stopping JobScheduler')
        with self.cond_queue:
            self.cond_queue.notifyAll()
        self.jobsched.join()
        self.logger.info('Stopping ConnectionMonitor')
        self.conmon.join()
        
    def kill_job(self, jobid):
        dbcon = self.connect_db()
        cursor = dbcon.cursor()
        cursor.execute('''SELECT * FROM JobInfo WHERE jobid = %s'''%jobid)
        jobinfo = cursor.fetchall() 
        if not jobinfo:
            raise JobException('Job %s does not exist'%jobid)
        else:
            jobinfo = jobinfo[0]
        with self.lock_runtime_info:
            with self.cond_queue:
                if jobinfo['status'] in ('running', 'queued'):
                    cursor.execute('''UPDATE JobInfo SET status = 'killed' WHERE jobid = %s'''%jobid)
                    dbcon.commit()
                    self.cond_queue.notifyAll()
                if jobid in self.runtime_info:
                    try:
                        os.kill(self.runtime_info[jobid].pid, signal.SIGKILL)
                    except OSError:
                        self.logger.warning('Failed to kill job %s'%jobid)
                else:
                    raise JobException('Job %s has finished'%jobid)
        dbcon.close()
        self.logger.info('Job %s has been killed'%jobid)
        
    def clear_jobs(self, timeout=1):
        dbcon = self.connect_db()
        cursor = dbcon.cursor()
        cursor.execute('''SELECT jobid FROM JobInfo WHERE status IN ('running', 'status')''')
        jobids = cursor.fetchall()
        for jobid in jobids:
            self.kill_job(jobid['jobid'])
        '''
        with self.lock_runtime_info:
            for jobid, rtinfo in self.runtime_info.iteritems():
                if rtinfo.pid > 0:
                    if KillJob(rtinfo.pid):
                        self.logger.warning('Killed job %d (pid: %d)'%(jobid, rtinfo.pid))
                    else:
                        self.logger.warning('Failed to kill job %d (pid: %d)'%(jobid, rtinfo.pid))
        '''
        # wait for all workers to finish
        with self.cond_thread_exit:
            while len(self.runtime_info) > 0:
                self.cond_thread_exit.wait(timeout)
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser('Simple job scheduler')
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
    parser.add_argument('-l', '--logfile', type=str, required=False,
                        help='Log file name')
    
    args = parser.parse_args()
    #main(args)
    logging.basicConfig(level=logging.DEBUG)
    if args.logfile:
        logging.basicConfig(filename=args.logfile)
    if args.clear:
        if os.path.exists(args.dbfile):
            os.unlink(args.dbfile)
    node = ComputeNode(name='MainNode', address=args.addr, port=args.port, 
                       maxjobs=args.jobs, dbfile=args.dbfile)
    node.start()
    node.stop()
    logging.shutdown()
    
