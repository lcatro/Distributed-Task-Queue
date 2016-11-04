
import json
import local_database
import task_pool
import thread
import time
import tornado.web
import tornado.ioloop


LOCAL_BIND_PORT=80
SLAVE_LOGIN_PASSWORD='t4sk_s3rv3r_l0g1n_p4ssw0rd'


class task_slave_machine :
    
    class task_slave_machine_state :
        
        wait_for_dispatch=0
        running_task=1
        running_except=2
    
    
    def __init__(self,slave_machine_id,slave_machine_ip,slave_machine_name) :
        self.slave_machine_id=slave_machine_id
        self.slave_machine_ip=slave_machine_ip
        self.slave_machine_name=slave_machine_name
        self.slave_machine_state=task_slave_machine.task_slave_machine_state.wait_for_dispatch
        self.slave_machine_time_tick=time.time()
        self.slave_machine_current_execute_task=None
        self.slave_machine_task_queue=task_pool.task_queue()
        
    def get_slave_machine_ip(self) :
        return self.slave_machine_ip
        
    def get_slave_machine_name(self) :
        return self.slave_machine_name
        
    def add_task(self,task,is_single_task) :
        self.slave_machine_task_queue.add_task(task,is_single_task)
        
    def get_task_queue_length(self) :
        return self.slave_machine_task_queue.get_current_queue_length()
        
    def get_current_execute_task_id(self) :
        if task_slave_machine.task_slave_machine_state.running_task==self.slave_machine_state :
            return self.slave_machine_current_execute_task['task_object'].get_task_id()
            
        return None
        
    def dispatch_task(self) :
        if task_slave_machine.task_slave_machine_state.wait_for_dispatch==self.slave_machine_state :
            self.slave_machine_current_execute_task=self.slave_machine_task_queue.get_task()
            self.slave_machine_state=task_slave_machine.task_slave_machine_state.running_task
            self.slave_machine_time_tick=time.time()
            
            return self.slave_machine_current_execute_task
        
        return None
        
    def finish_task(self) :
        if task_slave_machine.task_slave_machine_state.running_task==self.slave_machine_state :
            self.slave_machine_state=task_slave_machine.task_slave_machine_state.wait_for_dispatch
            self.slave_machine_time_tick=time.time()
            self.slave_machine_current_execute_task=None
        
    def get_time_tick(self) :
        return self.slave_machine_time_tick
        
    def delete_task(self,task_id) :
        self.slave_machine_task_queue.delete_task(task_id)
        
    def get_slave_machine_state(self) :
        return self.slave_machine_state
    
    def is_empty_task_queue(self) :
        if self.slave_machine_task_queue.get_current_queue_length() :
            return False
        
        return True
        
    def clear_all_task(self) :
        return_task_list=[]
        
        while not self.is_empty_task_queue() :
            return_task_list.append(self.slave_machine_task_queue.get_task())
            
        return return_task_list
    
    
class task_slave_machine_manager :
    
    __slave_machine_list={}
    __slave_thread_lock=thread.allocate_lock()
    
    @staticmethod
    def __make_slave_machine_id(slave_machine_ip,slave_machine_name) :
        return slave_machine_ip+slave_machine_name+str(time.time())
    
    @staticmethod
    def login_slave_machine(slave_machine_login_password,slave_machine_ip,slave_machine_name) :
        global SLAVE_LOGIN_PASSWORD
        
        return_slave_machine_id=None
        
        task_slave_machine_manager.__slave_thread_lock.acquire()
        
        if SLAVE_LOGIN_PASSWORD==slave_machine_login_password :
            task_slave_machine_manager.__slave_thread_lock.release()
            
            slave_machine_id=task_slave_machine_manager.get_slave_machine_id(slave_machine_ip,slave_machine_name)
            
            task_slave_machine_manager.__slave_thread_lock.acquire()
            
            if None==slave_machine_id :
                slave_machine_id=task_slave_machine_manager.__make_slave_machine_id(slave_machine_ip,slave_machine_name)
                new_task_slave_machine=task_slave_machine(slave_machine_id,slave_machine_ip,slave_machine_name)
                task_slave_machine_manager.__slave_machine_list[slave_machine_id]=new_task_slave_machine
                return_slave_machine_id=slave_machine_id
    
        task_slave_machine_manager.__slave_thread_lock.release()
        
        return return_slave_machine_id
    
    @staticmethod
    def logout_slave_machine(slave_machine_id) :
        return_slave_machine_unexecute_task_list=False
        
        task_slave_machine_manager.__slave_thread_lock.acquire()
        
        #if is_valid_slave_machine_id(slave_machine_id) :
        #  WARNING ! it will making a thread dead-lock .. 
            
        if task_slave_machine_manager.__slave_machine_list.has_key(slave_machine_id) :
            return_slave_machine_unexecute_task_list=task_slave_machine_manager.__slave_machine_list[slave_machine_id].clear_all_task()
            
            task_slave_machine_manager.__slave_machine_list.pop(slave_machine_id)
        
        task_slave_machine_manager.__slave_thread_lock.release()
        
        return return_slave_machine_unexecute_task_list
    
    @staticmethod
    def get_slave_machine_id(slave_machine_ip,slave_machine_name) :
        return_slave_machine_id=None
        
        task_slave_machine_manager.__slave_thread_lock.acquire()
        
        for slave_machine_index in task_slave_machine_manager.__slave_machine_list.keys() :
            if slave_machine_ip==task_slave_machine_manager.__slave_machine_list[slave_machine_index].get_slave_machine_ip() and \
                slave_machine_name==task_slave_machine_manager.__slave_machine_list[slave_machine_index].get_slave_machine_name() :
                return_slave_machine_id=slave_machine_index
                
                break
            
        task_slave_machine_manager.__slave_thread_lock.release()
        
        return return_slave_machine_id
    
    @staticmethod
    def get_slave_machine(slave_machine_id) :
        return_slave_machine=None
        
        task_slave_machine_manager.__slave_thread_lock.acquire()
        
        try :
            return_slave_machine=task_slave_machine_manager.__slave_machine_list[slave_machine_id]
        except :
            pass
        
        task_slave_machine_manager.__slave_thread_lock.release()
        
        return return_slave_machine
    
    @staticmethod
    def get_slave_machine_list() :
        return_slave_machine_list=[]
        
        task_slave_machine_manager.__slave_thread_lock.acquire()

        for slave_machin_index in task_slave_machine_manager.__slave_machine_list.keys() :
            return_slave_machine_list.append(slave_machin_index)

        task_slave_machine_manager.__slave_thread_lock.release()
        
        return return_slave_machine_list
    
    @staticmethod
    def is_empty_slave_machine_list() :
        return_result=True
        
        task_slave_machine_manager.__slave_thread_lock.acquire()
        
        if len(task_slave_machine_manager.__slave_machine_list) :
            return_result=False
        
        task_slave_machine_manager.__slave_thread_lock.release()
        
        return return_result
    
    @staticmethod
    def is_valid_slave_machine_id(slave_machine_id) :
        return_result=False
        
        task_slave_machine_manager.__slave_thread_lock.acquire()
        
        return_result=task_slave_machine_manager.__slave_machine_list.has_key(slave_machine_id)
        
        task_slave_machine_manager.__slave_thread_lock.release()
        
        return return_result
    
 
class task_dispatch :
    
    __dispatch_task_queue=task_pool.task_queue()
    __history_dispatch_task_queue=task_pool.task_queue()
    __dispatch_thread_lock=thread.allocate_lock()
    
    '''
    @staticmethod
    def __create_new_task_dispatch_pool_database() :
        task_dispatch_pool_database=database.create_new_database(__TASK_DISPATCH_POOL__)
        
        task_dispatch_pool_database.get_key_set().set_key(__TASK_DISPATCH_POOL__,task_dispatch.task_dispatch_pool)
        task_dispatch_pool_database.save_database()
    
    @staticmethod
    def reload_task_dispatch_pool() :
        task_dispatch_pool_database=None
        
        try :
            task_dispatch_pool_database=database(__TASK_DISPATCH_POOL__)
        except :
            task_dispatch.__create_new_task_dispatch_pool_database()
            
            return False
        
        task_dispatch.task_dispatch_pool=task_dispatch_pool_database.get_key_set().get_key('task_pool')
        
        return True
        
    @staticmethod
    def backup_task_dispatch_pool() :
        task_dispatch_pool_database=None
        
        try :
            task_dispatch_pool_database=database(__TASK_DISPATCH_POOL__)
        except :
            task_dispatch.__create_new_task_dispatch_pool_database()
            
            return False
            
        task_dispatch_pool_database.get_key_set().set_key(__TASK_DISPATCH_POOL__,task_dispatch.task_dispatch_pool)
        task_dispatch_pool_database.save_database()
        
        return True
    '''
    
    @staticmethod
    def dispatch() :
        task_dispatch.__dispatch_thread_lock.acquire()
        
        if not task_slave_machine_manager.is_empty_slave_machine_list() :
            if task_dispatch.__dispatch_task_queue.get_current_queue_length() :  #  dispatch task from __dispatch_task_queue
                first_task=task_dispatch.__dispatch_task_queue.get_task()
                first_free_slave_machine=None
                
                for slave_machine_index in task_slave_machine_manager.get_slave_machine_list() :
                    slave_machine=task_slave_machine_manager.get_slave_machine(slave_machine_index)
                    
                    if None==first_free_slave_machine :
                        first_free_slave_machine=slave_machine
                    elif first_free_slave_machine.get_task_queue_length()>slave_machine.get_task_queue_length() :
                        first_free_slave_machine=slave_machine
                        
                if 'single_task'==first_task['task_type'] :
                    first_free_slave_machine.add_task(first_task['task_object'],True)
                else :
                    first_free_slave_machine.add_task(first_task['task_object'],False)
            else :  #  dynamic adjust slave machine's pressure balance
                first_free_slave_machine_id=None
                first_busy_slave_machine_id=None
                
                for slave_machine_id_index in task_slave_machine_manager.get_slave_machine_list() :
                    if None==first_free_slave_machine_id :
                        first_free_slave_machine_id=slave_machine_id_index
                    if None==first_busy_slave_machine_id :
                        first_busy_slave_machine_id=slave_machine_id_index
                    
                    if task_slave_machine_manager.get_slave_machine(first_free_slave_machine_id).get_task_queue_length()> \
                        task_slave_machine_manager.get_slave_machine(slave_machine_id_index).get_task_queue_length() :
                        first_free_slave_machine_id=slave_machine_id_index
                    if task_slave_machine_manager.get_slave_machine(first_busy_slave_machine_id).get_task_queue_length()< \
                        task_slave_machine_manager.get_slave_machine(slave_machine_id_index).get_task_queue_length() :
                        first_busy_slave_machine_id=slave_machine_id_index
                
                if not first_free_slave_machine_id==first_busy_slave_machine_id :
                    balance_task=task_slave_machine_manager.get_slave_machine(first_busy_slave_machine_id).get_task()
                    
                    if 'single_task'==balance_task['task_type'] :
                        task_slave_machine_manager.get_slave_machine(first_free_slave_machine_id).add_task(balance_task['task_object'],True)
                    else :
                        task_slave_machine_manager.get_slave_machine(first_free_slave_machine_id).add_task(balance_task['task_object'],False)
        
        task_dispatch.__dispatch_thread_lock.release()
    
    @staticmethod
    def submit_result(task_id,task_result) :
        task_dispatch.__dispatch_thread_lock.acquire()
        
        task=task_dispatch.find_task(task_id)
        
        if not None==task :
            task['task_state']=task_pool.task_state.end
            task['task_result']=task_result
        
        task_dispatch.__dispatch_thread_lock.release()
    
    @staticmethod
    def add_task(task,is_single_task) :
        task_dispatch.__dispatch_thread_lock.acquire()
        task_dispatch.__dispatch_task_queue.add_task(task,is_single_task)
        task_dispatch.__history_dispatch_task_queue.add_task(task,is_single_task)
        task_dispatch.__dispatch_thread_lock.release()
        task_dispatch.dispatch()

            
class task_slave_login_handle(tornado.web.RequestHandler) :
    
    def get(self) :
        slave_machine_login_password=self.get_argument('slave_machine_login_password')
        slave_machine_ip=self.get_argument('slave_machine_ip')
        slave_machine_name=self.get_argument('slave_machine_name')
        slave_machine_id=task_slave_machine_manager.login_slave_machine(slave_machine_login_password,slave_machine_ip,slave_machine_name)
        return_json={}
    
        if not None==slave_machine_id :
            task_dispatch.dispatch()
            
            return_json['slave_machine_id']=slave_machine_id
        else :
            return_json['error']='None'
            
        self.write(json.dumps(return_json))
        

class task_slave_logout_handle(tornado.web.RequestHandler) :
    
    def get(self) :
        slave_machine_id=self.get_argument('slave_machine_id')
        return_json={}
        
        if task_slave_machine_manager.is_valid_slave_machine_id(slave_machine_id) :
            slave_machine_unexecute_task_list=task_slave_machine_manager.logout_slave_machine(slave_machine_id)

            for slave_machine_unexecute_task_index in slave_machine_unexecute_task_list :
                task_dispatch.add_task(slave_machine_unexecute_task_index['task_object'],slave_machine_unexecute_task_index['task_type'])
        
            return_json['success']='OK'
        else :
            return_json['error']='None'
        
        self.write(json.dumps(return_json))
        
       
class task_dispatch_handle(tornado.web.RequestHandler) :

    def get(self) :
        slave_machine_id=self.get_argument('slave_machine_id')
        return_json={}

        if task_slave_machine_manager.is_valid_slave_machine_id(slave_machine_id) :
            slave_machine=task_slave_machine_manager.get_slave_machine(slave_machine_id)
            
            if not None==slave_machine :
                new_task=slave_machine.dispatch_task()
                
                if not None==new_task :
                    return_json['dispatch_task']=new_task['task_object'].serialize()

        self.write(json.dumps(return_json))

        
class task_report_handle(tornado.web.RequestHandler) :
    
    def get(self) :
        slave_machine_id=self.get_argument('slave_machine_id')
        slave_machine_execute_task_id=self.get_argument('slave_machine_execute_task_id')
        slave_machine_report=self.get_argument('slave_machine_report')
        return_json={}
    
        if not None==slave_machine_id and not None==slave_machine_execute_task_id and not None==slave_machine_report :
            slave_machine=task_slave_machine_manager.get_slave_machine(slave_machine_id)
            
            if not None==slave_machine :
                if task_slave_machine.task_slave_machine_state.running_task==slave_machine.get_slave_machine_state() and \
                    slave_machine.get_current_execute_task_id()==slave_machine_execute_task_id :
                    slave_machine.finish_task()
                    task_dispatch.submit_result(slave_machine_execute_task_id,slave_machine_report)
                    task_dispatch.dispatch()
                    
                    return_json['success']=slave_machine_execute_task_id
    
        self.write(json.dumps(return_json))
    
        
def test_case() :
    test_task=task_pool.single_task('print 123')

    task_dispatch.add_task(test_task,True)

    handler = [
        ('/login',task_slave_login_handle),
        ('/logout',task_slave_logout_handle),
        ('/dispatch',task_dispatch_handle),
    ]
    http_server=tornado.web.Application(handlers=handler)

    http_server.listen(LOCAL_BIND_PORT)
    tornado.ioloop.IOLoop.current().start()
    
        
if __name__=='__main__' :
    
#    test_case()
    
    handler = [
        ('/login',task_slave_login_handle),
        ('/logout',task_slave_logout_handle),
        ('/dispatch',task_dispatch_handle),
    ]
    http_server=tornado.web.Application(handlers=handler)
    
    http_server.listen(LOCAL_BIND_PORT)
    tornado.ioloop.IOLoop.current().start()
