
import json
import psutil
import random
import requests
import thread
import time


DISPATCH_TASK_DELAY_TIME=2


class task_slave :
    
    __slave_machine_id=None
    __slave_machine_running=False
    
    @staticmethod
    def login(login_password,local_ip,local_machine_name) :
        try :
            login_url='http://127.0.0.1/login?slave_machine_login_password='+login_password+'&slave_machine_ip='+local_ip+'&slave_machine_name='+local_machine_name
            login=requests.get(login_url)
            login_result=json.loads(login.text)

            if login_result.has_key('slave_machine_id') :
                task_slave.__slave_machine_id=login_result['slave_machine_id']
                task_slave.__slave_machine_running=True

                thread.start_new_thread(task_slave.__performance_report_thread,())

                print 'Login Slave ID :'+task_slave.__slave_machine_id

                return True
            else :
                return False
        except :
            return False

    @staticmethod
    def dispatch() :
        return_task_information={}
        
        try :
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
        except :
            return_task_information={}
        
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
        report_url='http://127.0.0.1/report?slave_machine_id='+task_slave.__slave_machine_id+'&slave_machine_execute_task_id='+task_id+'&slave_machine_report='+report_json_string
        report=requests.get(report_url)
        report_result=json.loads(report.text)
    
        return report_result
    
    @staticmethod
    def logout() :
        try :
            if not None==task_slave.__slave_machine_id :
                task_slave.__slave_machine_running=False
                logout_url='http://127.0.0.1/logout?slave_machine_id='+task_slave.__slave_machine_id
                logout=requests.get(logout_url)
                logout_result=json.loads(logout.text)
        except :
            pass
    
    @staticmethod
    def __performance_report_thread() :
        while task_slave.__slave_machine_running :
            performance_report_object={}
            performance_report_object['slave_machine_cpu_rate']=task_slave.__get_cpu_rate()
            performance_report_object['slave_machine_memory_rate']=task_slave.__get_memory_rate()
            report_url='http://127.0.0.1/report?slave_machine_id='+task_slave.__slave_machine_id+'&slave_machine_report='+json.dumps(performance_report_object)
            report=requests.get(report_url)
            
            time.sleep(1)

    @staticmethod
    def __get_cpu_rate() :
        cpu_data_list=psutil.cpu_percent(interval=5,percpu=True)
        cpu_rate=0.0
        for cpu_data_index in cpu_data_list :
            cpu_rate+=cpu_data_index
        cpu_rate/=psutil.cpu_count()
        return cpu_rate

    @staticmethod
    def __get_memory_rate() :
        memory_data=psutil.virtual_memory()
        return memory_data.percent

    
if __name__=='__main__' :
    
    if task_slave.login('t4sk_s3rv3r_l0g1n_p4ssw0rd','127.0.0.1','slave'+str(time.time())+str(random.random())) :
        
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
    