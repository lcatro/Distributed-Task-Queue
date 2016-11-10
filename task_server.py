
import base64
import json
import local_database
import os
import pickle
import sys
import task_pool
import thread
import time
import tornado.web
import tornado.ioloop


LOCAL_BIND_PORT=80
SLAVE_LOGIN_PASSWORD='t4sk_s3rv3r_l0g1n_p4ssw0rd'
TASK_DISPATCH_MANAGER_PASSWORD='t4sk_s3rv3r_d1sp4tch_m4n4g3r_p4ssw0rd'


class task_slave_machine :
    
    class task_slave_machine_state :
        
        wait_for_dispatch=0
        running_task=1
        running_except=2
        server_maintenance=3
        
    
    def __init__(self,slave_machine_id,slave_machine_ip,slave_machine_name) :
        self.slave_machine_id=slave_machine_id
        self.slave_machine_ip=slave_machine_ip
        self.slave_machine_name=slave_machine_name
        self.slave_machine_state=task_slave_machine.task_slave_machine_state.wait_for_dispatch
        self.slave_machine_time_tick=time.time()
        self.slave_machine_is_server_maintenance=False
        self.slave_machine_current_execute_task=None
        self.slave_machine_task_queue=task_pool.task_queue()
        self.slave_machine_information=local_database.key_value()
        
    def get_slave_machine_ip(self) :
        return self.slave_machine_ip
        
    def get_slave_machine_name(self) :
        return self.slave_machine_name
        
    def add_task(self,task,is_single_task,is_necessary_task=False) :
        self.slave_machine_task_queue.add_task(task,is_single_task)
        
    def get_task_queue_length(self) :
        return self.slave_machine_task_queue.get_current_queue_length()
        
    def get_current_execute_task_id(self) :
        if task_slave_machine.task_slave_machine_state.running_task==self.slave_machine_state :
            return self.slave_machine_current_execute_task['task_object'].get_task_id()
            
        return None
        
    def dispatch_task(self) :
        if task_slave_machine.task_slave_machine_state.wait_for_dispatch==self.slave_machine_state and \
            not self.slave_machine_is_server_maintenance :
            self.slave_machine_current_execute_task=self.slave_machine_task_queue.get_task()
            
            if not None==self.slave_machine_current_execute_task :
                self.slave_machine_state=task_slave_machine.task_slave_machine_state.running_task
                self.slave_machine_time_tick=time.time()

                return self.slave_machine_current_execute_task
        
        return None
        
    def finish_task(self) :
        if task_slave_machine.task_slave_machine_state.running_task==self.slave_machine_state :
            if self.slave_machine_is_server_maintenance :
                self.slave_machine_state=task_slave_machine.task_slave_machine_state.server_maintenance
            else :
                self.slave_machine_state=task_slave_machine.task_slave_machine_state.wait_for_dispatch
                
            self.slave_machine_time_tick=time.time()
            self.slave_machine_current_execute_task=None
        
    def get_balance_task(self) :
        return self.slave_machine_task_queue.get_unnecessary_task()
        
    def get_time_tick(self) :
        return self.slave_machine_time_tick
        
    def delete_task(self,task_id) :
        self.slave_machine_task_queue.delete_task(task_id)
        
    def enter_maintenance(self) :
        self.slave_machine_is_server_maintenance=True
        
    def exit_maintenance(self) :
        self.slave_machine_is_server_maintenance=False
        self.slave_machine_state=task_slave_machine.task_slave_machine_state.wait_for_dispatch
        
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
    
    def set_information(self,information_key,information_value) :
        self.slave_machine_information.set_key(information_key,information_value)
        
    def get_information(self,information_key) :
        return self.slave_machine_information.get_key(information_key)
        
    def get_information_key_list(self) :
        return self.slave_machine_information.list_key()
    
    
class task_slave_machine_manager :
    
    __backup_slave_machine_list={}
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
    def get_slave_machine_information(slave_machine_id,information_key) :
        return_value=None
        
        task_slave_machine_manager.__slave_thread_lock.acquire()
        
        try :
            return_value=task_slave_machine_manager.__slave_machine_list[slave_machine_id].get_information(information_key)
        except :
            pass
        
        task_slave_machine_manager.__slave_thread_lock.release()
        
        return return_value
    
    @staticmethod
    def get_slave_machine_information_key_list(slave_machine_id) :
        return_value=None
        
        task_slave_machine_manager.__slave_thread_lock.acquire()
        
        try :
            return_value=task_slave_machine_manager.__slave_machine_list[slave_machine_id].get_information_key_list()
        except :
            pass
        
        task_slave_machine_manager.__slave_thread_lock.release()
        
        return return_value
    
    @staticmethod
    def set_slave_machine_information(slave_machine_id,information_key,information_value) :
        task_slave_machine_manager.__slave_thread_lock.acquire()
        
        try :
            task_slave_machine_manager.__slave_machine_list[slave_machine_id].set_information(information_key,information_value)
        except :
            pass
        
        task_slave_machine_manager.__slave_thread_lock.release()
    
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
    
    @staticmethod
    def serialize() :
        task_slave_machine_manager.__slave_thread_lock.acquire()
        
        return_serialize_string=pickle.dumps(task_slave_machine_manager.__slave_machine_list)
        
        task_slave_machine_manager.__slave_thread_lock.release()
        
        return return_serialize_string
    
    @staticmethod
    def deserialize(input_deserialize_string) :
        task_slave_machine_manager.__slave_thread_lock.acquire()
        
        task_slave_machine_manager.__backup_slave_machine_list=task_slave_machine_manager.__slave_machine_list  
        return_result=True
        
        try :
            task_slave_machine_manager.__slave_machine_list=pickle.loads(input_deserialize_string)
        except :
            task_slave_machine_manager.__slave_machine_list=task_slave_machine_manager.__backup_slave_machine_list
            return_result=False
            
        task_slave_machine_manager.__slave_thread_lock.release()
    
        return return_result
    
 
class task_dispatch :
    
    __TASK_DISPATCH__='task_dispatch'
    __TASK_DISPATCH_DISPATCH_INIT_TASK_QUEUE__='task_init_dispatch_queue'
    __TASK_DISPATCH_DISPATCH_TASK_QUEUE__='task_dispatch_queue'
    __TASK_DISPATCH_HISTORY_DISPATCH_TASK_QUEUE__='task_history_dispatch_queue'
    __TASK_DISPATCH_SLAVE_MACHINE_LIST__='task_slave_machine_list'
    __TASK_DISPATCH_TIME_WAIT_FOR_SLAVE_MACHINE_ENTER_SERVER_MAINTENANCE__=5
    
    __dispatch_init_task_queue=task_pool.task_queue()
    __dispatch_task_queue=task_pool.task_queue()
    __history_dispatch_task_queue=task_pool.task_queue()
    __dispatch_thread_lock=thread.allocate_lock()
    
    @staticmethod
    def __create_new_task_dispatch_pool_database() :
        task_dispatch.__dispatch_thread_lock.acquire()
        
        task_dispatch_database=local_database.database.create_new_database(task_dispatch.__TASK_DISPATCH__)
        
        task_dispatch_database.get_key_set().set_key(task_dispatch.__TASK_DISPATCH_DISPATCH_INIT_TASK_QUEUE__,task_pool.task_queue().serialize())
        task_dispatch_database.get_key_set().set_key(task_dispatch.__TASK_DISPATCH_DISPATCH_TASK_QUEUE__,task_pool.task_queue().serialize())
        task_dispatch_database.get_key_set().set_key(task_dispatch.__TASK_DISPATCH_HISTORY_DISPATCH_TASK_QUEUE__,task_pool.task_queue().serialize())
        task_dispatch_database.get_key_set().set_key(task_dispatch.__TASK_DISPATCH_SLAVE_MACHINE_LIST__,pickle.dumps({}))  #  serialize a empty dict ..
        task_dispatch_database.save_database()
        
        task_dispatch.__dispatch_thread_lock.release()
        
        return task_dispatch_database
    
    @staticmethod
    def recovery_task_dispatch() :
        task_dispatch_database=None
        return_result=True
    
        try :
            task_dispatch_database=local_database.database(task_dispatch.__TASK_DISPATCH__)
            
            task_dispatch.__dispatch_thread_lock.acquire()
            task_dispatch.__dispatch_init_task_queue.deserialize(task_dispatch_database.get_key_set().get_key(task_dispatch.__TASK_DISPATCH_DISPATCH_INIT_TASK_QUEUE__))
            task_dispatch.__dispatch_task_queue.deserialize(task_dispatch_database.get_key_set().get_key(task_dispatch.__TASK_DISPATCH_DISPATCH_TASK_QUEUE__))
            task_dispatch.__history_dispatch_task_queue.deserialize(task_dispatch_database.get_key_set().get_key(task_dispatch.__TASK_DISPATCH_HISTORY_DISPATCH_TASK_QUEUE__))
            task_slave_machine_manager.deserialize(task_dispatch_database.get_key_set().get_key(task_dispatch.__TASK_DISPATCH_SLAVE_MACHINE_LIST__))
            task_dispatch.__dispatch_thread_lock.release()
            
            for slave_machine_index in task_slave_machine_manager.get_slave_machine_list() :
                if task_slave_machine.task_slave_machine_state.server_maintenance==slave_machine_index.get_slave_machine_state() :
                    slave_machine_index.exit_maintenance()
        except :
            task_dispatch.__create_new_task_dispatch_pool_database()
            
            return_result=False
        
        return return_result
        
    @staticmethod
    def hot_backup_task_dispatch() :
        task_dispatch_database=None
        
        try :
            task_dispatch_database=local_database.database(task_dispatch.__TASK_DISPATCH__)
        except :
            task_dispatch_database=task_dispatch.__create_new_task_dispatch_pool_database()
            
        task_dispatch.__dispatch_thread_lock.acquire()
        task_dispatch_database.get_key_set().set_key(task_dispatch.__TASK_DISPATCH_DISPATCH_INIT_TASK_QUEUE__,task_dispatch.__dispatch_init_task_queue.serialize())
        task_dispatch_database.get_key_set().set_key(task_dispatch.__TASK_DISPATCH_DISPATCH_TASK_QUEUE__,task_dispatch.__dispatch_task_queue.serialize())
        task_dispatch_database.get_key_set().set_key(task_dispatch.__TASK_DISPATCH_HISTORY_DISPATCH_TASK_QUEUE__,task_dispatch.__history_dispatch_task_queue.serialize())
        task_dispatch_database.get_key_set().set_key(task_dispatch.__TASK_DISPATCH_SLAVE_MACHINE_LIST__,task_slave_machine_manager.serialize())
        task_dispatch_database.save_database()
        task_dispatch.__dispatch_thread_lock.release()
    
    @staticmethod
    def cold_backup_task_dispatch() :
        task_dispatch_database=None
        
        try :
            task_dispatch_database=local_database.database(task_dispatch.__TASK_DISPATCH__)
        except :
            task_dispatch_database=task_dispatch.__create_new_task_dispatch_pool_database()
            
        slave_machine_list=[]
        
        for slave_machine_index in task_slave_machine_manager.get_slave_machine_list() :
            slave_machine=task_slave_machine_manager.get_slave_machine(slave_machine_index)
            
            slave_machine.enter_maintenance()
            slave_machine_list.append(slave_machine)
        
        is_all_slave_machine_enter_maintenance=False
        
        while not is_all_slave_machine_enter_maintenance :
            for slave_machine_index in slave_machine_list :
                if not task_slave_machine.task_slave_machine_state.server_maintenance==slave_machine_index.get_slave_machine_state() :
                    time.sleep(task_dispatch.__TASK_DISPATCH_TIME_WAIT_FOR_SLAVE_MACHINE_ENTER_SERVER_MAINTENANCE__)
                    
                    continue
                
            is_all_slave_machine_enter_maintenance=True
            
        task_dispatch.hot_backup_task_dispatch()
    
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
                    balance_task=task_slave_machine_manager.get_slave_machine(first_busy_slave_machine_id).get_balance_task()
                    
                    if 'single_task'==balance_task['task_type'] :
                        task_slave_machine_manager.get_slave_machine(first_free_slave_machine_id).add_task(balance_task['task_object'],True)
                    else :
                        task_slave_machine_manager.get_slave_machine(first_free_slave_machine_id).add_task(balance_task['task_object'],False)
        
        task_dispatch.__dispatch_thread_lock.release()
    
    @staticmethod
    def dispatch_init_task(slave_machine_id) :
        slave_machine=task_slave_machine_manager.get_slave_machine(slave_machine_id)
        task_dispatch_init_task_list=task_dispatch.__dispatch_init_task_queue.clone()
        
        if task_dispatch_init_task_list.get_current_queue_length() :
            while task_dispatch_init_task_list.get_current_queue_length() :
                task_dispatch_init_task_index=task_dispatch_init_task_list.get_task()
                task_dispatch_init_task_index_task_object=task_dispatch_init_task_index['task_object']
                
                if 'single_task'==task_dispatch_init_task_index['task_type'] :
                    task_dispatch.add_task(task_dispatch_init_task_index_task_object,True,slave_machine_id)
                else :
                    task_dispatch.add_task(task_dispatch_init_task_index_task_object,False,slave_machine_id)
                
            return True
        
        return False
    
    @staticmethod
    def submit_result(task_id,task_result) :
        task_dispatch.__dispatch_thread_lock.acquire()
        
        task=task_dispatch.__history_dispatch_task_queue.find_task(task_id)
        
        if not None==task :
            task['task_state']=task_pool.task_state.end
            task['task_result']=task_result
        
        task_dispatch.__dispatch_thread_lock.release()
    
    @staticmethod
    def get_dispatch_task_queue_length() :
        return task_dispatch.__dispatch_task_queue.get_current_queue_length()
    
    @staticmethod
    def add_task(task,is_single_task,dispatch_to_target_slave_machine_id=None) :
        task_dispatch.__dispatch_thread_lock.acquire()
        
        if None==dispatch_to_target_slave_machine_id :
            task_dispatch.__dispatch_task_queue.add_task(task,is_single_task)
            task_dispatch.__history_dispatch_task_queue.add_task(task,is_single_task)
        else :
            if task_slave_machine_manager.is_valid_slave_machine_id(dispatch_to_target_slave_machine_id) :
                target_slave_machine=task_slave_machine_manager.get_slave_machine(dispatch_to_target_slave_machine_id)
                
                target_slave_machine.add_task(task,is_single_task,True)
                task_dispatch.__history_dispatch_task_queue.add_task(task,is_single_task)
            
        task_dispatch.__dispatch_thread_lock.release()
        task_dispatch.dispatch()

    @staticmethod
    def add_init_task(task,is_single_task) :
        task_dispatch.__dispatch_init_task_queue.add_task(task,is_single_task,True)
        
    @staticmethod
    def get_init_task_list() :
        return task_dispatch.__dispatch_init_task_queue.get_task_id_list()
    
    @staticmethod
    def get_init_task(task_id) :
        return task_dispatch.__dispatch_init_task_queue.find_task(task_id)
        
    @staticmethod
    def delete_init_task(task_id) :
        return task_dispatch.__dispatch_init_task_queue.delete_task(task_id)
        
            
class task_slave_login_handle(tornado.web.RequestHandler) :
    
    def get(self) :
        slave_machine_login_password=self.get_argument('slave_machine_login_password')
        slave_machine_ip=self.get_argument('slave_machine_ip')
        slave_machine_name=self.get_argument('slave_machine_name')
        slave_machine_id=task_slave_machine_manager.login_slave_machine(slave_machine_login_password,slave_machine_ip,slave_machine_name)
        return_json={}
    
        if not None==slave_machine_id :
            if not task_dispatch.dispatch_init_task(slave_machine_id) :
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
                if not slave_machine_unexecute_task_index['task_is_necessary'] :
                    task_dispatch.add_task(slave_machine_unexecute_task_index['task_object'],slave_machine_unexecute_task_index['task_type'])
        
            return_json['success']='OK'
        else :
            return_json['error']='None'
        
        self.write(json.dumps(return_json))
        
        
class task_add_task_handle(tornado.web.RequestHandler) :
    
    def get(self) :
        self.write('Try to using post ..')
    
    def post(self) :
        global TASK_DISPATCH_MANAGER_PASSWORD
        
        manager_password=self.get_body_argument('task_dispatch_manager_password')
        remote_ip=self.request.remote_ip
        result_json={}
        
        if TASK_DISPATCH_MANAGER_PASSWORD==manager_password and '127.0.0.1'==remote_ip :
            task_type=self.get_argument('task_type')
            task_dispatch_to_target_object_id=None
            
            try :
                task_dispatch_to_target_object_id=self.get_argument('dispatch_to_target_object_id')
            except :
                task_dispatch_to_target_object_id=None
            
            if 'single_task'==task_type :
                task_eval_code=base64.b64decode(self.get_argument('task_eval_code'))
                
                task_dispatch.add_task(task_pool.single_task(task_eval_code),True,task_dispatch_to_target_object_id)
            elif 'multiple_task'==task_type :
                try :
                    task_code_list_json=self.get_argument('task_code_list')
                    task_code_list=json.loads(task_code_list_json)
                    task_list=task_pool.multiple_task()
                    
                    if len(task_code_list) :
                        for task_index in task_code_list :
                            task=task_pool.single_task(base64.b64decode(task_index['task_eval_code']))

                            task_code_list.add_task(task_index)

                        task_dispatch.add_task(task_list,False,task_dispatch_to_target_object_id)
                    else :
                        result_json['error']='None'
                except :
                    pass
            elif 'init_single_task'==task_type :
                task_eval_code=base64.b64decode(self.get_argument('task_eval_code'))
                
                task_dispatch.add_init_task(task_pool.single_task(task_eval_code),True)
            elif 'init_multiple_task'==task_type :
                try :
                    task_code_list_json=self.get_argument('task_code_list')
                    task_code_list=json.loads(task_code_list_json)
                    task_list=task_pool.multiple_task()
                    
                    if len(task_code_list) :
                        for task_index in task_code_list :
                            task=task_pool.single_task(base64.b64decode(task_index['task_eval_code']))

                            task_code_list.add_task(task_index)

                        task_dispatch.add_init_task(task_list,False)
                    else :
                        result_json['error']='None'
                except :
                    pass
                
            result_json['success']='OK'
        else :
            result_json['error']='None'
        
        self.write(json.dumps(result_json))
    
    
class task_manager_handle(tornado.web.RequestHandler) :
    
    def get(self) :
        global TASK_DISPATCH_MANAGER_PASSWORD
        
        manager_password=self.get_argument('task_dispatch_manager_password')
        manager_operate_type=self.get_argument('manager_operate_type')
        manager_operate_argument=None
        
        try :
            manager_operate_argument=self.get_argument('manager_operate_argument')
        except :
            manager_operate_argument=None
            
        remote_ip=self.request.remote_ip
        result_json={}
        
        if TASK_DISPATCH_MANAGER_PASSWORD==manager_password and '127.0.0.1'==remote_ip :
            if 'recovery'==manager_operate_type :
                if task_dispatch.recovery_task_dispatch() :
                    result_json['success']='OK'
                else :
                    result_json['error']='recovery error ..'
            elif 'hot_backup'==manager_operate_type :
                task_dispatch.hot_backup_task_dispatch()
                
                result_json['success']='OK'
            elif 'cold_backup'==manager_operate_type :
                task_dispatch.cold_backup_task_dispatch()
                
                result_json['success']='OK'
            elif 'queue'==manager_operate_type :
                result_json['queue_length']=task_dispatch.get_dispatch_task_queue_length()
            elif 'slave_machine_list'==manager_operate_type :
                slave_machine_list=[]
                
                for slave_machine_index in task_slave_machine_manager.get_slave_machine_list() :
                    slave_machine_object=task_slave_machine_manager.get_slave_machine(slave_machine_index)
                    slave_machine_information={}
                    slave_machine_information['slave_machine_id']=slave_machine_index
                    slave_machine_information['slave_machine_ip']=slave_machine_object.get_slave_machine_ip()
                    slave_machine_information['slave_machine_name']=slave_machine_object.get_slave_machine_name()
                    slave_machine_information['slave_machine_state']=slave_machine_object.get_slave_machine_state()
                    slave_machine_information['slave_machine_task_queue_length']=slave_machine_object.get_task_queue_length()
                    
                    for slave_machine_information_key_index in task_slave_machine_manager.get_slave_machine_information_key_list(slave_machine_index) :
                        slave_machine_information[slave_machine_information_key_index]=task_slave_machine_manager.get_slave_machine_information(slave_machine_index,slave_machine_information_key_index)
                    
                    slave_machine_list.append(slave_machine_information)
                    
                result_json['slave_machine_list']=slave_machine_list
                
        self.write(json.dumps(result_json))
       
    
class task_dispatch_handle(tornado.web.RequestHandler) :

    def get(self) :
        slave_machine_id=self.get_argument('slave_machine_id')
        return_json={}

        if task_slave_machine_manager.is_valid_slave_machine_id(slave_machine_id) :
            slave_machine=task_slave_machine_manager.get_slave_machine(slave_machine_id)
            
            if not None==slave_machine :
                new_task=slave_machine.dispatch_task()
                
                if not None==new_task :
#                    return_json['dispatch_task']=new_task['task_object'].python_serialize()  #  Python serialize
                    return_json['dispatch_task']=new_task['task_object'].json_serialize()  #  JSON serialize
                    # WARNING ! it making serialize for task_object ,so we need to deserialize again ..

        self.write(json.dumps(return_json))

        
class task_report_handle(tornado.web.RequestHandler) :
    
    def get(self) :
        slave_machine_id=self.get_argument('slave_machine_id')
        slave_machine_report=self.get_argument('slave_machine_report')
        slave_machine_execute_task_id=None
        return_json={}
    
        try :
            slave_machine_execute_task_id=self.get_argument('slave_machine_execute_task_id')
        except :
            slave_machine_execute_task_id=None
    
        if not None==slave_machine_id and not None==slave_machine_report :
            slave_machine_report=json.loads(slave_machine_report)
            
            if not None==slave_machine_execute_task_id :
                slave_machine=task_slave_machine_manager.get_slave_machine(slave_machine_id)

                if not None==slave_machine :
                    if task_slave_machine.task_slave_machine_state.running_task==slave_machine.get_slave_machine_state() and \
                        slave_machine.get_current_execute_task_id()==slave_machine_execute_task_id :
                        slave_machine.finish_task()
                        task_dispatch.submit_result(slave_machine_execute_task_id,slave_machine_report)
                        task_dispatch.dispatch()

                        return_json['success']=slave_machine_execute_task_id
            else :
                try :
                    for key_index in slave_machine_report.keys() :
                        task_slave_machine_manager.set_slave_machine_information(slave_machine_id,key_index,slave_machine_report[key_index])
                except :
                    pass
    
        self.write(json.dumps(return_json))
    
    
class task_update_handle(tornado.web.RequestHandler) :
    
    @staticmethod
    def get_module_path() :
        current_file_name=sys.argv[0]
        current_path=current_file_name[:current_file_name.rfind('\\')+1]+'module\\'

        return current_path
    
    @staticmethod
    def get_relative_path(file_path) :
        try :
            current_file_path=task_update_handle.get_module_path()

            return file_path.replace(current_file_path,'')
        except :
            return None

    @staticmethod
    def list_dir_file(file_path) :
        output_file_list=[]
        
        for dir_path,dir_name,file_name in os.walk(file_path) :
            for file_name_index in file_name :
                output_file_list.append(dir_path+'\\'+file_name_index)

        return output_file_list
    
    @staticmethod
    def list_dir_dir(file_path) :
        output_file_list=[]
        
        for dir_path,dir_name,file_name in os.walk(file_path) :
            sub_dir_name=dir_path.replace(task_update_handle.get_module_path(),'')

            if -1==sub_dir_name.find('\\') and len(sub_dir_name.replace(' ','')) :
                output_file_list.append(sub_dir_name)

        return output_file_list
    
    @staticmethod
    def read_file(file_path) :
        file_handle=open(file_path)
        
        if file_handle :
            file_data=file_handle.read()
            
            file_handle.close()
            
            return file_data
        
        return None
    
    def get(self) :
        global TASK_DISPATCH_MANAGER_PASSWORD
        
        update_module_data=''
        task_dispatch_manager_password=None
        slave_machine_id=None
        update_module_file=None
        module_path=task_update_handle.get_module_path()
        return_json={}
        
        try :
            task_dispatch_manager_password=self.get_argument('task_dispatch_manager_password')
        except :
            task_dispatch_manager_password=None
        
        try :
            slave_machine_id=self.get_argument('slave_machine_id')
        except :
            slave_machine_id=None
        
        try :
            update_module_file=self.get_argument('module_name').replace('.','')
        except :
            update_module_file=None
            
        if task_slave_machine_manager.is_valid_slave_machine_id(slave_machine_id) or \
            TASK_DISPATCH_MANAGER_PASSWORD==task_dispatch_manager_password :
            if not None==update_module_file :
                update_module_path=module_path+update_module_file
                
                if os.path.exists(update_module_path) :
                    current_file_list=task_update_handle.list_dir_file(update_module_path)

                    for current_file_index in current_file_list :
                        return_json[task_update_handle.get_relative_path(current_file_index)]=task_update_handle.read_file(current_file_index)
            else :
                return_json=[]
                
                for module_index in task_update_handle.list_dir_dir(module_path) :
                    return_json.append(module_index)
        
        self.write(json.dumps(return_json))
        
    
class task_web_manager_handle(tornado.web.RequestHandler) :
    
    def get(self) :
        global TASK_DISPATCH_MANAGER_PASSWORD
        
        task_manager_html_data=''
        task_manager_html_file=open('task_manager.html')
        
        if task_manager_html_file :
            task_manager_html_data=task_manager_html_file.read().replace('%task_dispatch_manager_password%',TASK_DISPATCH_MANAGER_PASSWORD)
            task_manager_html_file.close()
        
        self.write(task_manager_html_data)
        
        
def test_case_dynamic_add_task() :
    import requests
    
    while True :
        input_code=raw_input('>')
        
        if 'len'==input_code :
            result=requests.get('http://127.0.0.1/manager?task_dispatch_manager_password=t4sk_s3rv3r_d1sp4tch_m4n4g3r_p4ssw0rd&manager_operate_type=queue')
            
            print result.text
        elif 'target'==input_code :
            machine_id=raw_input('slave_machine_id >')
            code=raw_input('code >')
            
            requests.get('http://127.0.0.1/add_task?task_dispatch_manager_password=t4sk_s3rv3r_d1sp4tch_m4n4g3r_p4ssw0rd&task_type=single_task&dispatch_to_target_object_id='+machine_id+'&task_eval_code='+code)
        elif 'recovery'==input_code :
            result=requests.get('http://127.0.0.1/manager?task_dispatch_manager_password=t4sk_s3rv3r_d1sp4tch_m4n4g3r_p4ssw0rd&manager_operate_type=recovery')
            
            print result.text
        elif 'hot_backup'==input_code :
            result=requests.get('http://127.0.0.1/manager?task_dispatch_manager_password=t4sk_s3rv3r_d1sp4tch_m4n4g3r_p4ssw0rd&manager_operate_type=hot_backup')
            
            print result.text
        elif 'cold_backup'==input_code :
            result=requests.get('http://127.0.0.1/manager?task_dispatch_manager_password=t4sk_s3rv3r_d1sp4tch_m4n4g3r_p4ssw0rd&manager_operate_type=cold_backup')
            
            print result.text
        elif 'slave_machine_list'==input_code :
            result=requests.get('http://127.0.0.1/manager?task_dispatch_manager_password=t4sk_s3rv3r_d1sp4tch_m4n4g3r_p4ssw0rd&manager_operate_type=slave_machine_list')
            
            print result.text
        else :
            post_argument={
                'task_dispatch_manager_password':'t4sk_s3rv3r_d1sp4tch_m4n4g3r_p4ssw0rd',
                'task_type':'single_task',
                'task_eval_code':base64.b64encode(input_code)
            }
            
            requests.post('http://127.0.0.1/add_task',data=post_argument)
        
def test_case() :
    task_dispatch.recovery_task_dispatch()
    
    test_task_1=task_pool.single_task('print 123')
    test_task_2=task_pool.single_task('print 321')
    test_task_3=task_pool.single_task('print 1234567')
    test_multiple_task=task_pool.multiple_task()
    
    test_multiple_task.add_task(test_task_1)
    test_multiple_task.add_task(test_task_2)
    test_multiple_task.add_task(test_task_3)
    task_dispatch.add_task(test_task_1,True)
    task_dispatch.add_task(test_multiple_task,False)
    task_dispatch.add_task(test_task_3,True)

    handler = [
        ('/login',task_slave_login_handle),
        ('/logout',task_slave_logout_handle),
        ('/add_task',task_add_task_handle),
        ('/manager',task_manager_handle),
        ('/dispatch',task_dispatch_handle),
        ('/report',task_report_handle),
        ('/',task_web_manager_handle),
    ]
    http_server=tornado.web.Application(handlers=handler)

    thread.start_new_thread(test_case_dynamic_add_task,())
    http_server.listen(LOCAL_BIND_PORT)
    tornado.ioloop.IOLoop.current().start()
    
        
if __name__=='__main__' :
    
#    test_case()
    
    task_dispatch.recovery_task_dispatch()
    
    handler = [
        ('/login',task_slave_login_handle),
        #  http://127.0.0.1/login?slave_machine_login_password=t4sk_s3rv3r_l0g1n_p4ssw0rd&slave_machine_ip=127.0.0.1&slave_machine_name=slave1
        ('/logout',task_slave_logout_handle),
        #  http://127.0.0.1/logout?slave_machine_id=
        ('/add_task',task_add_task_handle),
        #  http://127.0.0.1/add_task?task_dispatch_manager_password=t4sk_s3rv3r_d1sp4tch_m4n4g3r_p4ssw0rd&task_type=single_task&task_eval_code=
        ('/manager',task_manager_handle),
        #  http://127.0.0.1/manager?task_dispatch_manager_password=t4sk_s3rv3r_d1sp4tch_m4n4g3r_p4ssw0rd&manager_operate_type=&manager_operate_argument=
        ('/dispatch',task_dispatch_handle),
        #  http://127.0.0.1/dispatch?slave_machine_id=
        ('/report',task_report_handle),
        #  http://127.0.0.1/report?slave_machine_id=&slave_machine_execute_task_id=&slave_machine_report=
        ('/update',task_update_handle),
        ('/',task_web_manager_handle),
        #  http://127.0.0.1/
    ]
    http_server=tornado.web.Application(handlers=handler)
    
    http_server.listen(LOCAL_BIND_PORT)
    tornado.ioloop.IOLoop.current().start()
    