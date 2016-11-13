
##Distributed Task Queue 分布式任务队列框架

  `Distributed Task Queue` 是一个简单的分布式框架,核心原理是**由任务服务器分配代码到各个任务执行的主机**,换句话说也就是**远程代码执行**,只需要在跑任务的机器上布置任务执行模块,无需做更多的操作即可实现分布式任务执行.设计思路来源于工单系统,公司里每员工就像执行任务的主机,一旦公司内部或者外部出现了问题或者需求,那么就需要根据这个点建立一张待处理的工单,然后派发到指定的员工执行,同样地,这个工单对于执行任务的主机来就是由任务调度服务器派发下来的任务,这些任务的主要内容就是即将要执行的代码..
  
##How to using Distributed Task Queue 

  `task_server` 是任务派遣服务器,包含有任务调度算法,为了方便使用,直接访问`http://127.0.0.1/` 是任务服务器的管理后台(包括接口支持的所有功能),如果需要自己拓展,服务器提供的接口如下:<br/>
  
---

  slave machine 登陆接口<br/>
  参数:**slave_machine_login_password** 登陆密码,**slave_machine_ip** 主机IP ,**slave_machine_name** 主机名<br/>
  可选参数:**slave_machine_group** 登陆进入指定的机组<br/>
  返回值:**slave_machine_id** 服务器分配主机唯一ID <br/>
  URL  http://127.0.0.1/login?slave_machine_login_password=&slave_machine_ip=&slave_machine_name=<br/>
  
---
  
  slave machine 退出接口<br/>
  参数:**slave_machine_id** 服务器分配主机唯一ID <br/>
  返回值:**success** 或者**error** <br/>
  `TIPS : 退出之后任务系统会重新调度任务列表`<br/>
  URL  http://127.0.0.1/logout?slave_machine_id=<br/>
  
---

  slave machine 任务领取接口<br/>
  参数:**slave_machine_id** 服务器分配主机唯一ID <br/>
  返回值:**任务详细信息**<br/>
  URL  http://127.0.0.1/dispatch?slave_machine_id=<br/>
  
---
  
  slave machine 任务报告接口<br/>
  参数:**slave_machine_id** 服务器分配主机唯一ID ,**slave_machine_execute_task_id** 任务ID ,**slave_machine_report** 详细报告信息<br/>
  返回值:**success** 或者**error** <br/>
  URL  http://127.0.0.1/report?slave_machine_id=&slave_machine_execute_task_id=&slave_machine_report=<br/>
    
---
  
  slave machine 性能报告接口<br/>
  参数:**slave_machine_id** 服务器分配主机唯一ID ,**slave_machine_report** 主机性能参数列表<br/>
  返回值:**success** 或者**error** <br/>
  URL  http://127.0.0.1/report?slave_machine_id=&slave_machine_report=<br/>
    
---
  
  slave machine 模块更新接口<br/>
  参数:**slave_machine_id** 服务器分配主机唯一ID <br/>
  返回值:**可更新模块列表** <br/>
  URL  http://127.0.0.1/update?slave_machine_id=<br/>
  
  <br/>
  
  参数:**slave_machine_id** 服务器分配主机唯一ID ,**module_name** 模块名<br/>
  返回值:**模块文件列表和代码** <br/>
  URL  http://127.0.0.1/update?slave_machine_id=&module_name=<br/>
    
  `TIPS : 任务服务器支持更新模块在module 文件夹下,每个模块的代码使用独立的文件夹来管理,参考module 目录中的test_update_module 模块`
    
---

  task dispatch server 管理接口<br/>
  参数:**task_dispatch_manager_password** 管理密码,**manager_operate_type** 管理命令,**manager_operate_argument** 管理命令参数列表<br/>
  返回值:**详细信息**<br/>
  `TIPS : 限制在本地主机IP 管理`<br/>
  URL  http://127.0.0.1/manager?task_dispatch_manager_password=&manager_operate_type=recovery&manager_operate_argument=<br/>
    
    manager_operate_type 支持命令:
    recovery                恢复task dispatch 服务器环境
    hot_backup              热备份task dispatch 数据(直接保存当前任务队列信息和主机列表)
    cold_backup             冷备份task dispatch 数据(等待所有主机执行完成任务再备份任务队列信息和主机列表)
    queue                   查看当前任务队列信息
    slave_machine_list      查看slave machine 列表
    
---

  task dispatch server 添加任务接口<br/>
  参数:**task_dispatch_manager_password** 管理密码,**task_type** 任务类型,**task_eval_code** 任务代码<br/>
  可选参数:**dispatch_to_target_slave_machine_group** 向指定机组添加任务(默认向全局机组添加),**dispatch_to_target_object_id** 向指定机器添加任务<br/>
  返回值:**success** 或者**error**<br/>
  `TIPS : 限制在本地主机IP 管理`<br/>
  
    task_type 任务类型支持初始化任务和普通任务的处理,传递参数的值如下:
    init_single_task    添加初始化单任务
    init_multiple_task  添加初始化多任务
    init_workflow_task  添加初始化工作流任务
    single_task         添加普通单任务
    multiple_task       添加普通多任务
    workflow_task       添加工作流任务
  
    单任务:执行单个代码任务
    多任务:多个单任务的集合
    初始化任务:当新的slave machine 登陆到服务器时先要执行的任务
    普通任务:由服务器自动分配到slave machine 执行的任务
    工作流任务:把当前执行完成的任务结果递交到接下来要处理这些数据slave machine 组执行
  
  URL  http://127.0.0.1/add_task?task_dispatch_manager_password=&task_type=&task_eval_code=<br/>
  URL  http://127.0.0.1/add_task?task_dispatch_manager_password=&task_type=&task_eval_code=&dispatch_to_target_object_id=<br/>
  URL  http://127.0.0.1/add_task?task_dispatch_manager_password=&task_type=&task_eval_code=&dispatch_to_target_slave_machine_group=<br/>
  
  关于服务器相关的接口使用例子保存在`task_server.py test_case()` ,客户端相关的接口使用例子保存在`task_client.py task_slave` 类
  
  <br/><br/>
  
  `task_client` 是执行任务的模块,只需要运行这个Python 即可,`task_client.py` 依赖`requests` ,需要在布置的时候安装它<br/><br/>


  Example -- `task_server.py` 默认测试用例:<br/>

  ![using_example](https://raw.githubusercontent.com/lcatro/Distributed-Task-Queue/master/readme_pic/using_example.png)<br/><br/>


  Example -- 分布式DDOS 例子:<br/>

  Python 代码:
  
    for index in range(50) :
        print 'Request Index :'+str(index),requests.get('http://www.qq.com/')

  ![ddos](https://raw.githubusercontent.com/lcatro/Distributed-Task-Queue/master/readme_pic/ddos.png)<br/><br/>


  Example -- 更新模块例子:<br/>

  Python 代码(TIPS : 建议作为初始化任务执行,让新的slave machine 先更新模块再执行任务):

    print task_slave.get_update_module_list()
    
    task_slave.update('test_update_module')


##Distributed Task Queue 的其他细节

####远程代码执行与任务执行

  执行任务的主机尽量简单布置,最好能让一个模块就能够做很多的事情,所以把代码当作是一个任务来执行是不错的解决方案,首先可以解决掉复杂的任务语句处理(就像Windows 的控制台解析批处理文件一样,会使得执行任务的模块变得臃肿而且拓展性不高),除去Python 可以执行分配下来的任务,几乎支持所有可以使用`eval()` 执行的语言(无论是Python 还是JavaScript 等),不受执行的平台所限制(即便是作为本地进程来运行的Python ,还是浏览器的前端JavaScript 都可以领取任务执行)<br/>

  派遣到主机分为两种任务:`single_task` (单任务)和`multiple_task` (多任务),`multiple_task` 相当于提供一组`single_task` 单任务列表,让执行任务的主机按照顺序来执行每个任务

---

####机器分组

  分布式系统在很多情况下需要执行多种不同样的任务,而且这些任务只能派发到指定分组的机器执行,比如说A 组机器用于扫描目的站点的漏洞(分布式扫描,加快扫描进度),B 组机器用于针对扫描出来的站点漏洞进行利用(分布式利用,不容易发现真实的攻击机器)

---

####工作流任务

  在进行分布式计算时,假设A 机组用于生产数据,B 机组用于处理数据,C 机组用于保存数据,那么根据数据处理流程来说是`A -> B -> C` ,工作流任务的意义为把派发到A 机组处理完成的任务结果转发到B 机组继续处理,然后把B 机组处理完成的结果继续递交到C 机组处理.`workflow_task` 实际上是`multiple_task` ,只是在`multiple_task` 里面设置了`is_workflow_task` 值,在`task_server.py task_report_handle` 中会根据Slave Machine 处理完成的任务ID 来判断这个是否为`workflow_task` 任务,最后通过`next_task_id` 和`next_dispatch_slave_machine_group` 值来转发到下一个要处理数据的机组

---

####关于任务的负载均衡

  关于任务的负载均衡使用简单的遍历算法,让任务数较多的主机把自己的任务分配到任务比较少的主机,尽可能让每个执行任务的主机都能够平衡执行任务的压力,关于分配的算法写在`task_dispatch.dispatch()` 中

---

####任务服务器的维护备份

  任务服务器的备份方式分别为:**热备份**和**冷备份**,相关逻辑在`task_server.py task_dispatch.hot_backup_task_dispatch() 和task_dispatch.cold_backup_task_dispatch()` ,热备份直接把任务队列和当前登陆到任务服务器的主机列表保存到`database/task_dispatch.db` 中;冷备份需要等待所有执行任务的主机执行完所有任务之后进入停机状态,然后保存数据,使用冷备份的方式需要用`task_dispatch.recovery_task_dispatch()` 使dispatch 服务器重新启动.简单地说,热备份适用于定时备份当前的数据,冷备份适用于服务器进入停机维护状态
