
import json
import requests
import time


DISPATCH_TASK_DELAY_TIME=2


class task_slave :
    
    __slave_machine_id=None
    
    @staticmethod
    def login(login_password,local_ip,local_machine_name) :
        login_url='http://127.0.0.1/login?slave_machine_login_password='+login_password+'&slave_machine_ip='+local_ip+'&slave_machine_name='+local_machine_name
        login=requests.get(login_url)
        login_result=json.loads(login.text)
        
        if login_result.has_key('slave_machine_id') :
            task_slave.__slave_machine_id=login_result['slave_machine_id']
            
            return True
        else :
            return False

    @staticmethod
    def dispatch() :
        return_task_information={}
        
        if not None==task_slave.__slave_machine_id :
            dispatch_url='http://127.0.0.1/dispatch?slave_machine_id='+task_slave.__slave_machine_id
            dispatch=requests.get(dispatch_url)
            dispatch_result=json.loads(dispatch.text)
            
            if {}==dispatch_result :
                return return_task_information
            
            dispatch_task=json.loads(dispatch_result['dispatch_task'])
            dispatch_task_id=dispatch_task['task_id']
            return_task_list=[]
            
            if dispatch_task.has_key('task_list') :
                for task_index_ in dispatch_task['task_list'] :
                    task_index=json.loads(task_index_)
                    
                    return_task_list.append(task_index['task_code'])
            else :
                return_task_list.append(dispatch_task['task_code'])

            return_task_information['task_id']=dispatch_task_id
            return_task_information['task_list']=return_task_list
            
            return return_task_information
        
        return return_task_information

    @staticmethod
    def execute_task(task_id,task_list) :
        report_json={}
        report_json['task_id']=task_id
        
        try :
            task_result_list=[]

            for task_index in task_list :
                report_result=None

                exec(task_index)
                task_result_list.append(report_result)

            report_json['report_result']=task_result_list
        except Exception,e :
            report_json['report_except_name']=Exception
            report_json['report_exception_descript']=e.message
            report_json['report_state']='except'
    
        return report_json
    
    @staticmethod
    def report(report_object) :
        report_json_string=json.dumps(report_object)
        task_id=report_object['task_id']
        dispatch_url='http://127.0.0.1/report?slave_machine_id='+task_slave.__slave_machine_id+'&slave_machine_execute_task_id='+task_id+'&slave_machine_report='+report_json_string
        report=requests.get(dispatch_url)
        report_result=json.loads(report.text)
    
        return report_result
    
    @staticmethod
    def logout() :
        if not None==task_slave.__slave_machine_id :
            logout_url='http://127.0.0.1/logout?slave_machine_id='+task_slave.__slave_machine_id
            logout=requests.get(logout_url)
            logout_result=json.loads(logout.text)
    
    
if __name__=='__main__' :
    
    if task_slave.login('t4sk_s3rv3r_l0g1n_p4ssw0rd','127.0.0.1','slave1') :
        
        try :
            while True :
                dispatch_task=task_slave.dispatch()

                if len(dispatch_task) :
                    task_slave.report(task_slave.execute_task(dispatch_task['task_id'],dispatch_task['task_list']))
                else :
                    time.sleep(DISPATCH_TASK_DELAY_TIME)
        except :  #  except for KeyInterrupt ..
            pass
        
        task_slave.logout()
    else :
        print 'login Error ! ..'
    