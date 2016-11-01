
import pickle
import sys


class key_value :

    def serialize(self) :
        return pickle.dumps(self.key_set)

    def deserialize(self,input_deserialize_string) :
        try :
            self.key_set=pickle.loads(input_deserialize_string)
            
            return True
        except :
            self.key_set={}
            
            return False

    def __init__(self) :
        self.key_set={}
        
    def set_key(self,key_name,key_value) :
        self.key_set[key_name]=key_value
        
    def get_key(self,key_name) :
        try :
            return self.key_set[key_name]
        except :
            return None
        
    def list_key(self) :
        return self.key_set.keys()
    
    def copy_key_set(self,key_value) :
        self.key_set.clear()
        
        for key_index in key_value.list_key() :
            self.key_set[key_index]=key_value.get_key(key_index)
        
class database :

    @staticmethod
    def __read_file(file_path) :
        file_handle=open(file_path)

        if file_handle :
            read_data=file_handle.read()

            file_handle.close()

            return read_data

        return None

    @staticmethod
    def __rewrite_file(file_path,new_data) :
        file_handle=open(file_path,'w')

        if file_handle :
            file_handle.write(new_data)
            file_handle.close()
        
    @staticmethod
    def __get_current_path() :
        last_dir_index=sys.argv[0].rfind('\\')

        return sys.argv[0][:last_dir_index+1]
    
    @staticmethod
    def create_new_database(database_name) :
        init_file_data=key_value()
        init_file_path=database.__get_current_path()+'\\database\\'+database_name+'.db'
        
        database.__rewrite_file(init_file_path,init_file_data.serialize())
        
        return database(database_name)
    

    def __init__(self,database_name) :
        self.database_path=database.__get_current_path()+'\\database\\'+database_name+'.db'
        
        self.reload_database()
        
    def get_key_set(self) :
        return self.key_value
        
    def reload_database(self) :
        deserialize_string=database.__read_file(self.database_path)
        
        self.key_value=key_value()
        
        if not self.key_value.deserialize(deserialize_string) :
            raise ClassError,'Can\'not Open Database File ..'
        
    def save_database(self) :
        database.__rewrite_file(self.database_path,self.key_value.serialize())
        
        
if __name__=='__main__' :  #  test case
    test_key_value=key_value()
    
    print 'test_key_value.set_key("test",1)'
    test_key_value.set_key('test',1)
    print 'test_key_value.get_key("test") ->',test_key_value.get_key('test')
    print 'test_key_value.get_key("1234") ->',test_key_value.get_key('1234')
    
    print 'test_key_value.set_key("test2",{"test2":1,"a":"123123"})'
    test_key_value.set_key('test2',{'test2':1,'a':'123123'})
    print 'test_key_value.get_key("test2") ->',test_key_value.get_key('test2')
    
    print 'test_key_value.serialize() ->',test_key_value.serialize()
    print 'deserialize(serialize()) ->',\
        test_key_value.deserialize(test_key_value.serialize())
    
    
    try :
        test_database=database('test')
    
        print 'test_database.get_key_set().get_key("test2") ->',test_database.get_key_set().get_key('test2')
    except :
        test_database=database.create_new_database('test')

        print 'test_database.get_key_set().copy_key(test_key_value)'
        test_database.get_key_set().copy_key_set(test_key_value)
        print 'test_database.save_database()'
        test_database.save_database()
        print 'test_database.reload_database()'
        test_database.reload_database()
        print 'test_database.get_key_set().get_key("test2") ->',test_database.get_key_set().get_key('test2')
    