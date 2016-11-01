
import local_database
import pickle
import thread
import time


class single_task :
    
    def __init__(self,task_code) :
        self.task_code=task_code
        self.task_id=str(time.time())
        
    def get_task_id(self) :
        return self.task_id
    
    def serialize(self) :
        return pickle.dumps(self)
    
    
class multiple_task :
    
    def __init__(self) :
        self.single_task_list=[]
        self.task_id=str(time.time())
            
    def add_task(self,single_task_object) :
        self.single_task_list.append(single_task_object)
        
        return len(self.single_task_list)-1
    
    def delete_task(self,task_id) :
        for single_task_index in self.single_task_list :
            if task_id==single_task_index.get_task_id() :
                self.single_task_list.remove(single_task_index)
                
                return True
            
        return False

    def get_task_list_length(self) :
        return len(self.single_task_list)
    
    def get_task_id(self) :
        return self.task_id
    
    def serialize(self) :
        return pickle.dumps(self)
    

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
    
    def delete_task(self,task_id) :
        self.lock.acquire()
        
        for task_index in self.task_list :
            if 'single_task'==task_index.task_type :
                if task_id==task_index.task_object.get_task_id() :
                    self.task_list.remove(task_index.task_object)
                    
                    break
            elif 'multiple_task'==task_index.task_type :
                if task_id==task_index.task_object.get_task_id() :
                    self.task_list.remove(task_index.task_object)
                    
                    break
                else :
                    if task_index.task_object.delete_task(task_id) :
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
        
        self.task_queue_list[task_queue_name]=task_queue
        
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
        
    
if __name__=='__main__' :
    test_task_queue=task_queue()
    test_singal_task=single_task('print "TEST"')
    test_multiple_task=multiple_task()
    
    print 'test_singal_task.serialize() ->',test_singal_task.serialize()
    
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


