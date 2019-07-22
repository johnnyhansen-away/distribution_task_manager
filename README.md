# distribution_task_manager
分布式进程任务管理器+进程负载均衡器 在分布式部署的环境下, 对于一批未知任务数并且每个任务的任务量不同, 程序代码无状态(不在每个程序内绑定固定任务,一并程序的灵活性),实现每个启动的进程能够负载相对均衡的领到任务, 并且能够保证任务数及任务量的动态调整(增加/减少任务数, 加大/减少任务量)后,实现负载的再平衡, 在进程或主机down掉的情况下, 能将任务平滑过渡到剩下活着的进程中!

举例:
针对不同上游供应商做请求代理, 每个供应商授权的qps不同, 此模块需要分布式部署在多台机器上, 并且随着供应商数的增加及减少, 进程部署情况可以随之动态调整
每个供应商的qps相差较大, 写死的配置,不能动态适配每台机器的负载


