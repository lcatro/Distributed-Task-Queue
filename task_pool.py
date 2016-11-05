
import json
import local_database
import pickle
import random
import thread
import time


def make_task_id() :
    return str(time.time())+str(random.random())

class single_task :
    
    def __init__(self,task_code) :
        self.task_code=task_code
        self.task_other_information=local_database.key_value()
        self.task_id=make_task_id()
        
    def set_task_information(self,information_key_name,information_key_value) :
        self.task_other_information.set_key(information_key_name,information_key_value)
        
    def get_task_information(self,information_key_name) :
        return self.task_other_information.get_key(information_key_name)
        
    def get_task_id(self) :
        return self.task_id
    
    def python_serialize(self) :
        return pickle.dumps(self)
    
    def json_serialize(self) :
        return_json={}
        return_json['task_id']=self.task_id
        return_json['task_code']=self.task_code
        return_json['task_other_information']={}
        
        for task_other_information_key_index in self.task_other_information.list_key() :
            return_json['task_other_information'][task_other_information_key_index]=self.task_other_information.get_key(task_other_information_key_index)
            
        return json.dumps(return_json)
    
class multiple_task :
    
    def __init__(self) :
        self.single_task_list=[]
        self.task_other_information=local_database.key_value()
        self.task_id=make_task_id()
            
    def add_task(self,single_task_object) :
        self.single_task_list.append(single_task_object)
        
        return len(self.single_task_list)-1
    
    def find_task(self,task_id) :
        for single_task_index in self.single_task_list :
            if task_id==single_task_index.get_task_id() :
                
                return single_task_index
            
        return None
    
    def delete_task(self,task_id) :
        for single_task_index in self.single_task_list :
            if task_id==single_task_index.get_task_id() :
                self.single_task_list.remove(single_task_index)
                
                return True
            
        return False

    def set_task_information(self,information_key_name,information_key_value) :
        self.task_other_information.set_key(information_key_name,information_key_value)
        
    def get_task_information(self,information_key_name) :
        return self.task_other_information.get_key(information_key_name)
        
    def set_task_index_information(self,task_id,information_key_name,information_key_value) :
        for task_index in self.single_task_list :
            if task_id==task_index.get_task_id() :
                task_index.set_key(information_key_name,information_key_value)
        
    def get_task_index_information(self,task_id,information_key_name) :
        for task_index in self.single_task_list :
            if task_id==task_index.get_task_id() :
                return task_index.get_key(information_key_name)
                
        return None
        
    def get_task_list_length(self) :
        return len(self.single_task_list)
    
    def get_task_id(self) :
        return self.task_id
    
    def python_serialize(self) :
        return pickle.dumps(self)
    
    def json_serialize(self) :
        return_json={}
        return_json['task_id']=self.task_id
        return_json['task_list']=[]
        
        for task_index in self.single_task_list :
            return_json['task_list'].append(task_index.json_serialize())
        
        return_json['task_other_information']={}
        
        for task_other_information_key_index in self.task_other_information.list_key() :
            return_json['task_other_information'][task_other_information_key_index]=self.task_other_information.get_key(task_other_information_key_index)
        
        return json.dumps(return_json)

class task_state :
    
    unexecute=0
    running=1
    end=2
    excepted=3
    
    
class task_queue :
    
    def __init__(self) :
        self.task_list=[]
        self.lock=thread.allocate_lock()
        
    def add_task(self,task,is_single_task) :
        self.lock.acquire()
        
        if is_single_task :
            self.task_list.append(
                {
                    'task_type':'single_task',
                    'task_object':task,
                    'task_state':task_state.unexecute,
                    'task_result':None
                }
            )
        else :
            self.task_list.append(
                {
                    'task_type':'multiple_task',
                    'task_object':task,
                    'task_state':task_state.unexecute,
                    'task_result':None
                }
            )
        
        self.lock.release()
        
    def get_task(self) :
        return_task=None
        
        self.lock.acquire()
        
        if len(self.task_list) :
            return_task=self.task_list[0]
            
            self.task_list.remove(return_task)
            
        self.lock.release()
        
        return return_task
    
    def find_task(self,task_id) :
        return_task=None
        
        self.lock.acquire()
        
        if len(self.task_list) :
            for task_index in self.task_list :
                if 'single_task'==task_index['task_type'] :
                    if task_index['task_object'].get_task_id()==task_id :
                        return_task=task_index
                        
                        break
                elif 'multiple_task'==task_index['task_type'] :
                    if task_index['task_object'].get_task_id()==task_id :
                        return_task=task_index
                        
                        break
                    else :
                        pass
                    #  WARNING !! read a single_task in multiple_task ,it can not return a object like self.task_list
                    #  so we can not set task status ..
                        '''
                        task_object=task_index['task_object'].find_task(task_id)
                        
                        if not None==task_object :
                            return_task=task_object
            
                            break
                        '''
            
        self.lock.release()
        
        return return_task
    
    def delete_task(self,task_id) :
        self.lock.acquire()
        
        for task_index in self.task_list :
            if 'single_task'==task_index['task_type'] :
                if task_id==task_index['task_object'].get_task_id() :
                    self.task_list.remove(task_index['task_object'])
                    
                    break
            elif 'multiple_task'==task_index['task_type'] :
                if task_id==task_index['task_object'].get_task_id() :
                    self.task_list.remove(task_index['task_object'])
                    
                    break
        
        self.lock.release()
        
    def get_current_queue_length(self) :
        self.lock.acquire()
        
        return_length=len(self.task_list)
        
        self.lock.release()
        
        return return_length
    
    def serialize(self) :
        return pickle.dumps(self.task_list)
    
    def deserialize(self,input_serialize_string) :
        self.backup_task_list=self.task_list  
        #  TIPS : thread lock is not a lasting object 
        #  so we have not serialize it ..
        
        try :
            self.task_list=pickle.loads(input_serialize_string)
            
            return True
        except :
            self.task_list=self.backup_task_list
            
            return False
        
        
class task_pool :
    
    def __init__(self) :
        self.task_queue_list={}
        self.lock=thread.allocate_lock()
    
    def create_queue(self,task_queue_name) :
        self.lock.acquire()
        
        if not self.task_queue_list.has_key(task_queue_name) :
            self.task_queue_list[task_queue_name]=task_queue()
        
        self.lock.release()
        
    def get_queue(self,task_queue_name) :
        self.lock.acquire()
        
        return_queue=None
        
        try :
            return_queue=self.task_queue_list[task_queue_name]
        except :
            return_queue=None
        
        self.lock.release()
        
        return return_queue

    def get_current_queue_count(self) :
        self.lock.acquire()
        
        return_current_queue_count=len(self.task_queue_list)

        self.lock.release()
        
        return return_current_queue_count
    
    def serialize(self) :
        return pickle.dumps(self.task_queue_list)
    
    def deserialize(self,input_serialize_string) :
        self.backup_task_queue_list=self.task_queue_list
        
        try :
            self.task_queue_list=pickle.loads(input_serialize_string)
            
            return True
        except :
            self.task_queue_list=self.backup_task_queue_list
            
            return False
        
    
if __name__=='__main__' :  #  test case
    test_task_queue=task_queue()
    test_singal_task=single_task('print "TEST"')
    test_multiple_task=multiple_task()
    
    print 'test_singal_task.serialize() ->',test_singal_task.python_serialize()
    
    for create_task_index in range(5) :
        test_multiple_task.add_task(single_task('print '+str(create_task_index)))
        print 'test_multiple_task.add_task(single_task("print "+'+str(create_task_index)+'))'
        
    print 'test_multiple_task.get_task_list_length() ->',test_multiple_task.get_task_list_length()
    print 'test_multiple_task.get_task_id() ->',test_multiple_task.get_task_id()
        
    print 'test_task_queue.add_task()'
    test_task_queue.add_task(test_singal_task,True)
    test_task_queue.add_task(test_multiple_task,False)

    print 'test_task_queue.get_current_queue_length() ->',test_task_queue.get_current_queue_length()

    print 'test_task_queue.get_task()'
    test_task_queue.get_task()
    
    print 'test_task_queue.get_current_queue_length() ->',test_task_queue.get_current_queue_length()

    print 'test_task_queue.find_task(test_singal_task.get_task_id()) ->',test_task_queue.find_task(test_singal_task.get_task_id())
    print 'test_task_queue.find_task(test_multiple_task.get_task_id()) ->',test_task_queue.find_task(test_multiple_task.get_task_id())

