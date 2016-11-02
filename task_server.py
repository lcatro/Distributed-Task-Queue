
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
    
    slave_machine_list={}
    __slave_thread_lock=thread.allocate_lock()
    
    @staticmethod
    def register_new_slave_machine(slave_machine_login_password,slave_machine_ip,slave_machine_name) :
        global SLAVE_LOGIN_PASSWORD
        
        return_slave_machine_id=None
        
        task_slave_machine.__slave_thread_lock.acquire()
        
        if SLAVE_LOGIN_PASSWORD==slave_machine_login_password :
            slave_machine_id=get_slave_machine_id(slave_machine_ip,slave_machine_name)
            
            if None==slave_machine_id :
                slave_machine_id=slave_machine_ip+slave_machine_name+str(time.time()*1000)
                task_slave_machine.slave_machine_list[new_slave_machine_id]['slave_machine_ip']=slave_machine_ip
                task_slave_machine.slave_machine_list[new_slave_machine_id]['slave_machine_name']=slave_machine_name
                return_slave_machine_id=slave_machine_id
    
        task_slave_machine.__slave_thread_lock.release()
        
        return return_slave_machine_id
    
    @staticmethod
    def get_slave_machine_id(slave_machine_ip,slave_machine_name) :
        return_slave_machine_id=None
        
        task_slave_machine.__slave_thread_lock.acquire()
        
        for slave_machine_index in task_slave_machine.slave_machine_list.keys() :
            if slave_machine_ip==task_slave_machine.slave_machine_list[slave_machine_index]['slave_machine_ip'] and 
                slave_machine_name==task_slave_machine.slave_machine_list[slave_machine_index]['slave_machine_name'] :
                return_slave_machine_id=slave_machine_index
            
        task_slave_machine.__slave_thread_lock.release()
        
        return None
    
    @staticmethod
    def get_slave_information(slave_machine_id) :
        try :
            return task_slave_machine.slave_machine_list[slave_machine_id]
        except :
            return None
    
    @staticmethod
    def is_valid_slave_machine_id(slave_machine_id) :
        
        return task_slave_machine.slave_machine_list.has_key(slave_machine_id)

    def queue
    
    asdasdacacas #  slave_machine queue
    
    
    
class task_slave_login_handle(tornado.web.RequestHandler) :
    
    def get(self) :
        slave_machine_login_password=self.get_argument('slave_machine_login_password')
        slave_machine_ip=self.get_argument('slave_machine_ip')
        slave_machine_name=self.get_argument('slave_machine_name')
        slave_machine_id=register_new_slave_machine(slave_machine_login_password,slave_machine_ip,slave_machine_name)
        return_json={}
    
        if None==slave_machine_id :
            return_json['slave_machine_id']=slave_machine_id
            
        self.write(json.dumps(return_json))
        

class task_dispatch :
    
    __TASK_QUEUE_UNEXECUTE_TASK__  ='unexecute_task'
    __TASK_QUEUE_RUNNING_TASK__    ='running_task'
    __TASK_QUEUE_EXCEPT_TASK__     ='except_task'
    __TASK_QUEUE_FINISH_TASK__     ='finish_task'
    __TASK_QUEUE_FREE_MACHINE__    ='free_machine'
    __TASK_QUEUE_RUNNING_MACHINE__ ='running_machine'
    
    __TASK_DISPATCH_POOL__         ='task_dispatch_pool'
    
    task_dispatch_pool=task_pool()
    
    @staticmethod
    def __create_new_task_dispatch_pool_database() :
        task_dispatch_pool_database=database.create_new_database(__TASK_DISPATCH_POOL__)

        if 0==task_dispatch_pool.task_dispatch_pool.get_current_queue_count() :
            task_dispatch.task_dispatch_pool=task_pool()

            task_dispatch.task_dispatch_pool.create_queue(__TASK_QUEUE_UNEXECUTE_TASK__)
            task_dispatch.task_dispatch_pool.create_queue(__TASK_QUEUE_RUNNING_TASK__)
            task_dispatch.task_dispatch_pool.create_queue(__TASK_QUEUE_EXCEPT_TASK__)
            task_dispatch.task_dispatch_pool.create_queue(__TASK_QUEUE_FINISH_TASK__)
            task_dispatch.task_dispatch_pool.create_queue(__TASK_QUEUE_FREE_MACHINE__)
            task_dispatch.task_dispatch_pool.create_queue(__TASK_QUEUE_RUNNING_MACHINE__)

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
        
    @staticmethod
    def add_task(self,task,is_single_task) :
        task_dispatch.task_dispatch_pool.get_queue(__TASK_QUEUE_UNEXECUTE_TASK__).add_task(task,is_single_task)
        
    @staticmethod
    def __add_free_machine(self,slave_machine_id) :
        
        if not None==task_dispatch.task_dispatch_pool.get_queue(__TASK_QUEUE_RUNNING_TASK__).find_task() :
        
        free_machine=single_task()
        
        free_machine.set_task_information('slave_machine_id',slave_machine_id)
        task_dispatch.task_dispatch_pool.get_queue(__TASK_QUEUE_FREE_MACHINE__).add_task(task,free_machine)
        
    @staticmethod
    def enter_dispatch_queue(self,slave_machine_id) :
        if task_slave_machine.is_valid_slave_machine_id(slave_machine_id) :
            #  TIPS : it will be block when a slave machine watting for task dispatch ..
            #  WARNING ! if unexecute task dispatch is blocking ,it will making a web timeout except ..
            
            pass
            
        return None
        
    @staticmethod
    def submit_task_result(self,slave_machine_id,task_id) :
        task_dispatch.task_dispatch_pool.get_queue(
        
    @staticmethod
    def check_slave_task_execute_state(self,
        
        
        
class task_dispatch_handle :
    
    def get(self) :
        slave_machine_id=self.get_argument('slave_machine_id')

        if task_slave_machine.is_valid_slave_machine_id(slave_machine_id) :
            asdasdasd
            
        self.write(json.dumps(return_json))
            

if __name__=='__main__' :
    handler = [
        ('/login',task_slave_login_handle),
        ('/dispatch',task_dispatch_handle),
    ]
    http_server=tornado.web.Application(handlers=handler)
    
    http_server.listen(LOCAL_BIND_PORT)
    tornado.ioloop.IOLoop.current().start()

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    